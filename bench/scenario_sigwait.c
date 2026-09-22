/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * scenario_sigwait.c - signal waits under load (IOR_OP_SIGWAIT).
 *
 * A stress test more than a benchmark: `depth` signal waits stay pending on
 * one queued (real-time) signal while a sender thread queues instances of
 * it, each carrying a sequence number, at the pace the waits consume them:
 * a credit per signal collected, starting with half as many as there are
 * waits, so signals are always scarcer than the waits racing for them.
 * Each completed wait is re-armed at once. With --race-pct, that fraction of
 * re-arms also cancels a pending wait picked at random, so cancellation
 * races the signal's arrival; with --timer linked every wait is guarded by
 * a generous link timeout.
 *
 * What this exercises differs per backend. On io_uring every pending wait
 * is a signalfd poll and one arrival wakes all of them: one collects the
 * signal, the rest find it gone and complete -EAGAIN, to be re-armed with a
 * fresh signalfd, so a deep run is a storm of polls and descriptors. On the
 * thread backend every pending wait pins a worker in sigtimedwait() slices,
 * so the depth competes with everything else the pool does and a cancel
 * lands between slices.
 *
 * Correctness: every sequence number sent is collected exactly once (or is
 * still pending at the end, having been sent to a wait that was cancelled
 * meanwhile); a wait completes only with the signal, -EAGAIN or -ECANCELED;
 * a cancel that reports 0 pairs with a -ECANCELED wait, -EALREADY and
 * -ENOENT with either outcome; no CQE is missing or duplicated; a link
 * timeout never fires. Latency is send to collection. Throughput counts
 * signals collected; -EAGAIN and cancelled completions are reported
 * separately at the end.
 */
#include "config.h"
#include "bench_platform.h"
#include "bench_scenario.h"
#include "bench_trace.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Op kinds, packed with the slot index into the SQE user_data. */
enum {
	K_WAIT = 0,
	K_LINK_TIMEOUT = 1,
	K_CANCEL = 2,
	K_BITS = 2,
	K_MASK = (1u << K_BITS) - 1,
};

static inline void *make_tag(uint32_t slot, unsigned kind)
{
	return (void *) (((uintptr_t) slot << K_BITS) | (kind & K_MASK));
}
static inline uint32_t tag_slot(void *data)
{
	return (uint32_t) ((uintptr_t) data >> K_BITS);
}
static inline unsigned tag_kind(void *data)
{
	return (unsigned) ((uintptr_t) data & K_MASK);
}

typedef struct sw_slot {
	ior_siginfo_t info;
	unsigned awaiting; /* CQEs still expected this round */
	int armed; /* the wait is submitted and not yet completed */
	int cancel_out; /* a cancel for it is in flight */
	int32_t wait_res;
	int32_t cancel_res;
	int seen_wait, seen_lt, seen_cancel;
} sw_slot;

typedef struct sender {
	bench_gate gate; /* credits: signals the sender may queue */
	int signo;
	_Atomic uint32_t sent; /* next sequence number */
	_Atomic uint64_t *send_ns; /* [seq % window]: when it was queued */
	uint32_t window;
} sender;

typedef struct sig_ctx {
	ior_ctx *ior;
	const bench_options *opts;
	bench_metrics *m;
	int signo;
	ior_sigset_t snd_set; /* just signo */
	sw_slot *slots;
	uint32_t depth;
	uint32_t *free_slots; /* stack of slots with no wait pending */
	uint32_t free_count;
	ior_timespec timeout;
	int linked;
	uint32_t race_pct;
	uint64_t rng;
	uint64_t inflight;
	int draining;

	sender snd;
	bench_thread snd_thread;
	int snd_started;
	uint32_t *seen; /* [seq % window]: seq + 1 of the last sequence collected there */
	uint32_t window;

	/* Outcome split. */
	uint64_t received; /* waits that collected a signal */
	uint64_t eagain; /* waits that found the signal gone */
	uint64_t cancelled; /* waits that ended -ECANCELED */
	uint64_t n_c0, n_calready, n_cnoent; /* cancel results */
} sig_ctx;

/* The sender thread: one queued signal per credit, sequence-numbered. */
static void sender_main(void *arg)
{
	sender *s = arg;
	uint32_t n;
	while ((n = bench_gate_take(&s->gate)) > 0) {
		for (uint32_t i = 0; i < n; i++) {
			uint32_t seq = atomic_load_explicit(&s->sent, memory_order_relaxed);
			atomic_store_explicit(
					&s->send_ns[seq % s->window], bench_now_ns(), memory_order_relaxed);
			atomic_store_explicit(&s->sent, seq + 1, memory_order_release);
			int ret;
			while ((ret = bench_sig_queue(s->signo, (int) seq)) == -EAGAIN) {
				/* The process-wide queue is full: give a wait time to drain it. */
				uint64_t until = bench_now_ns() + 1000000;
				while (bench_now_ns() < until) { }
			}
			if (ret < 0) {
				return;
			}
		}
	}
}

static int roll(sig_ctx *s)
{
	s->rng = s->rng * 6364136223846793005ULL + 1442695040888963407ULL;
	return (uint32_t) ((s->rng >> 33) % 100) < s->race_pct;
}

/* A pending wait, other than `self`, that no cancel targets yet; UINT32_MAX
 * if there is none. */
static uint32_t pick_target(sig_ctx *s, uint32_t self)
{
	s->rng = s->rng * 6364136223846793005ULL + 1442695040888963407ULL;
	uint32_t start = (uint32_t) ((s->rng >> 33) % s->depth);
	for (uint32_t i = 0; i < s->depth; i++) {
		uint32_t si = (start + i) % s->depth;
		sw_slot *t = &s->slots[si];
		if (si != self && t->armed && !t->cancel_out) {
			return si;
		}
	}
	return UINT32_MAX;
}

/* Submit a cancel for slot si; 0 if the SQ was full. */
static int issue_cancel(sig_ctx *s, uint32_t si)
{
	ior_sqe *sqe = ior_get_sqe(s->ior);
	if (!sqe) {
		return 0;
	}
	sw_slot *t = &s->slots[si];
	ior_prep_cancel(s->ior, sqe, make_tag(si, K_WAIT));
	ior_sqe_set_data(s->ior, sqe, make_tag(si, K_CANCEL));
	t->cancel_out = 1;
	t->awaiting++;
	s->inflight++;
	return 1;
}

/* Arm a wait on a free slot (guarded when linked), and with --race-pct
 * cancel a random pending one in the same batch; 0 if the SQ was full. */
static int issue_one(sig_ctx *s)
{
	if (s->free_count == 0) {
		return 0;
	}
	ior_sqe *sqe = ior_get_sqe(s->ior);
	if (!sqe) {
		return 0;
	}
	uint32_t si = s->free_slots[--s->free_count];
	sw_slot *t = &s->slots[si];

	memset(&t->info, 0, sizeof(t->info));
	if (ior_prep_sigwait(s->ior, sqe, &s->snd_set, &t->info) < 0) {
		ior_prep_nop(s->ior, sqe); /* keep the entry harmless */
		bench_metrics_error(s->m);
		s->free_slots[s->free_count++] = si;
		return 1;
	}
	ior_sqe_set_data(s->ior, sqe, make_tag(si, K_WAIT));
	t->armed = 1;
	t->cancel_out = 0;
	t->seen_wait = t->seen_lt = t->seen_cancel = 0;
	t->awaiting = 1;
	s->inflight++;

	if (s->linked) {
		ior_sqe *lsqe = ior_get_sqe(s->ior);
		if (lsqe) {
			ior_sqe_set_flags(s->ior, sqe, IOR_SQE_IO_LINK);
			ior_prep_link_timeout(s->ior, lsqe, &s->timeout, 0);
			ior_sqe_set_data(s->ior, lsqe, make_tag(si, K_LINK_TIMEOUT));
			t->awaiting++;
			s->inflight++;
		}
		/* else: run this wait unguarded rather than leave a dangling link. */
	}

	if (s->race_pct && roll(s)) {
		uint32_t ti = pick_target(s, si);
		if (ti != UINT32_MAX) {
			(void) issue_cancel(s, ti); /* an SQ full here just skips the race */
		}
	}
	BENCH_TRACE2("arm slot=%llu inflight=%llu", si, s->inflight);
	return 1;
}

/* A collected signal: sequence in range and not seen before, latency. */
static void account_signal(sig_ctx *s, sw_slot *t)
{
	uint32_t seq = (uint32_t) bench_sig_value(&t->info);
	uint32_t sent = atomic_load_explicit(&s->snd.sent, memory_order_acquire);
	if (seq >= sent || sent - seq > s->window) {
		bench_metrics_error(s->m); /* not sent, or older than the window */
		return;
	}
	uint32_t at = seq % s->window;
	if (s->seen[at] == seq + 1) {
		bench_metrics_error(s->m); /* collected twice */
		return;
	}
	s->seen[at] = seq + 1;
	uint64_t sent_ns = atomic_load_explicit(&s->snd.send_ns[at], memory_order_relaxed);
	uint64_t now = bench_now_ns();
	bench_metrics_record(s->m, now > sent_ns ? now - sent_ns : 0, 0);
	s->received++;
}

/* Every CQE of a slot's round is in: check the pairing, free the slot. */
static void finish_round(sig_ctx *s, uint32_t si)
{
	sw_slot *t = &s->slots[si];
	int ok = 1;
	int consumed = 0;

	if (t->wait_res == s->signo) {
		account_signal(s, t);
		consumed = 1;
	} else if (t->wait_res == -EAGAIN) {
		s->eagain++;
	} else if (t->wait_res == -ECANCELED) {
		s->cancelled++;
		if (!t->cancel_out) {
			ok = 0; /* nothing cancelled it */
		}
	} else {
		ok = 0;
	}

	if (t->cancel_out) {
		if (t->cancel_res == 0) {
			s->n_c0++;
			if (t->wait_res != -ECANCELED) {
				ok = 0; /* found and cancelled, yet it completed otherwise */
			}
		} else if (t->cancel_res == -EALREADY) {
			s->n_calready++;
		} else if (t->cancel_res == -ENOENT) {
			s->n_cnoent++;
			if (t->wait_res == -ECANCELED) {
				ok = 0; /* nothing found it, yet it was cancelled */
			}
		} else {
			ok = 0;
		}
	}
	if (!ok) {
		bench_metrics_error(s->m);
	}

	t->armed = 0;
	t->cancel_out = 0;
	s->free_slots[s->free_count++] = si;
	/* The signal it took is replaced; a wait that took none owes nothing new. */
	if (consumed && !s->draining) {
		bench_gate_post(&s->snd.gate, 1);
	}
}

static void harvest_one(sig_ctx *s, ior_cqe *cqe)
{
	void *data = ior_cqe_get_data(s->ior, cqe);
	int32_t res = ior_cqe_get_res(s->ior, cqe);
	uint32_t si = tag_slot(data);
	unsigned kind = tag_kind(data);
	sw_slot *t = &s->slots[si];

	s->inflight--;
	BENCH_TRACE4(
			"cqe slot=%llu kind=%llu res=%lld inflight=%llu", si, kind, (int64_t) res, s->inflight);

	int dup = 0;
	switch (kind) {
		case K_WAIT:
			dup = t->seen_wait++;
			t->wait_res = res;
			break;
		case K_LINK_TIMEOUT:
			dup = t->seen_lt++;
			if (res == -ETIME) {
				bench_metrics_error(s->m); /* the guard fired */
			}
			break;
		case K_CANCEL:
			dup = t->seen_cancel++;
			t->cancel_res = res;
			break;
		default:
			bench_metrics_error(s->m);
			return;
	}
	if (dup || t->awaiting == 0) {
		bench_metrics_error(s->m); /* a CQE this round did not expect */
		return;
	}
	if (--t->awaiting == 0) {
		finish_round(s, si);
	}
}

/* Stop the sender, then take every pending wait back. */
static void start_drain(sig_ctx *s)
{
	s->draining = 1;
	bench_metrics_stop(s->m);
	bench_gate_close(&s->snd.gate);
	bench_thread_join(&s->snd_thread);
	s->snd_started = 0;

	for (uint32_t si = 0; si < s->depth; si++) {
		sw_slot *t = &s->slots[si];
		if (t->armed && !t->cancel_out) {
			while (!issue_cancel(s, si)) {
				ior_submit(s->ior);
			}
		}
	}
#if !defined(IOR_HAVE_URING) && !defined(IOR_HAVE_SIGTIMEDWAIT)
	/* A worker in sigwait() returns for a signal alone: send one each. */
	for (uint32_t si = 0; si < s->depth; si++) {
		if (s->slots[si].armed) {
			uint32_t seq = atomic_fetch_add(&s->snd.sent, 1);
			atomic_store(&s->snd.send_ns[seq % s->window], bench_now_ns());
			(void) bench_sig_queue(s->signo, (int) seq);
		}
	}
#endif
}

static int run_loop(sig_ctx *s)
{
	const bench_options *o = s->opts;
	uint64_t deadline_ns = o->ops ? 0 : bench_now_ns() + (uint64_t) (o->duration_s * 1e9);
	ior_cqe *batch[256];

	while (1) {
		unsigned n = ior_peek_batch_cqe(s->ior, batch, 256);
		for (unsigned i = 0; i < n; i++) {
			harvest_one(s, batch[i]);
		}
		if (n > 0) {
			ior_cq_advance(s->ior, n);
		}

		if (!s->draining) {
			int done = o->ops ? (s->received >= o->ops) : (bench_now_ns() >= deadline_ns);
			if (done) {
				start_drain(s);
			}
		}

		if (!s->draining) {
			while (s->free_count > 0 && issue_one(s)) { }
		}

		ior_submit(s->ior);

		if (s->draining && s->inflight == 0) {
			break;
		}

		if (n == 0 && s->inflight > 0) {
			ior_cqe *cqe = NULL;
			int ret = bench_wait_completion(s->ior, s->opts, &cqe, s->received);
			if (ret < 0 && ret != -EAGAIN && ret != -EINTR && ret != -ETIME) {
				return ret;
			}
		}
	}
	return 0;
}

int bench_run_sigwait(const bench_options *opts, bench_metrics *m, const char **backend_name_out)
{
	int ret = 0;
	sig_ctx s;
	memset(&s, 0, sizeof(s));
	s.opts = opts;
	s.m = m;
	s.depth = opts->depth ? opts->depth : 8;
	s.race_pct = opts->race_pct > 100 ? 100 : opts->race_pct;
	s.linked = opts->timer_mode == BENCH_TIMER_LINKED;
	s.timeout.tv_sec = opts->timeout_ms / 1000;
	s.timeout.tv_nsec = (long long) (opts->timeout_ms % 1000) * 1000000LL;
	s.rng = 0x9e3779b97f4a7c15ULL;
	s.window = s.depth * 32 + 1024;

	s.signo = bench_sig_number(0);
	if (s.signo < 0) {
		return -ENOTSUP;
	}
	if (ior_sigemptyset(&s.snd_set) < 0 || ior_sigaddset(&s.snd_set, s.signo) < 0) {
		return -EINVAL;
	}
	/* Blocked here before any thread is started, so every thread has it so. */
	ret = bench_sig_block(s.signo);
	if (ret < 0) {
		return ret;
	}

	uint32_t sq = opts->sq_entries;
	if (sq < s.depth * 4) {
		sq = s.depth * 4;
	}
	if (sq < 256) {
		sq = 256;
	}
	ret = ior_queue_init(sq, &s.ior);
	if (ret < 0) {
		return ret;
	}
	if (backend_name_out) {
		*backend_name_out = ior_get_backend_name(s.ior);
	}

	s.slots = calloc(s.depth, sizeof(*s.slots));
	s.free_slots = calloc(s.depth, sizeof(*s.free_slots));
	s.seen = calloc(s.window, sizeof(*s.seen));
	s.snd.send_ns = calloc(s.window, sizeof(*s.snd.send_ns));
	if (!s.slots || !s.free_slots || !s.seen || !s.snd.send_ns) {
		ret = -ENOMEM;
		goto out;
	}
	for (uint32_t i = 0; i < s.depth; i++) {
		s.free_slots[s.free_count++] = s.depth - 1 - i;
	}
	s.snd.signo = s.signo;
	s.snd.window = s.window;
	atomic_init(&s.snd.sent, 0);
	ret = bench_gate_init(&s.snd.gate);
	if (ret < 0) {
		goto out;
	}

	bench_metrics_start(m);
	ret = bench_thread_start(&s.snd_thread, sender_main, &s.snd);
	if (ret < 0) {
		goto out;
	}
	s.snd_started = 1;
	/* Half as many signals as waits to begin with, so waits outnumber them
	 * throughout; each collection earns the next one. */
	bench_gate_post(&s.snd.gate, (s.depth + 1) / 2);

	ret = run_loop(&s);
	if (m->wall_end_ns == 0) {
		bench_metrics_stop(m);
	}

	/* Sent to a wait that was cancelled meanwhile: still pending, still ours. */
	int leftover = bench_sig_drain(s.signo);
	uint32_t sent = atomic_load(&s.snd.sent);
	if ((uint64_t) sent != s.received + (uint64_t) leftover) {
		bench_metrics_error(m);
	}

	fprintf(stderr,
			"sigwait outcomes: sent=%u received=%llu eagain=%llu cancelled=%llu leftover=%d | "
			"cancel res: 0=%llu -EALREADY=%llu -ENOENT=%llu\n",
			sent, (unsigned long long) s.received, (unsigned long long) s.eagain,
			(unsigned long long) s.cancelled, leftover, (unsigned long long) s.n_c0,
			(unsigned long long) s.n_calready, (unsigned long long) s.n_cnoent);

out:
	if (s.snd_started) {
		bench_gate_close(&s.snd.gate);
		bench_thread_join(&s.snd_thread);
	}
	ior_queue_exit(s.ior);
	bench_gate_destroy(&s.snd.gate);
	free(s.snd.send_ns);
	free(s.seen);
	free(s.free_slots);
	free(s.slots);
	(void) bench_sig_drain(s.signo);
	return ret;
}
