/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * scenario_accept.c - a multishot accept under bursts of connections.
 *
 * Models a loop that serves accepts from one multishot accept on a listener
 * (ior_prep_accept_multishot), the way the PHP io_hooks ring does under
 * DirectAccept: a client thread connects a burst of --conns connections as
 * fast as it can, the loop reaps once per pass (a pass being --work-us of
 * sleep, or a wait for the first completion with 0), and every connection
 * reaped goes into a buffer the loop takes --takes connections from per
 * pass (0: all of them). Past a cap of --depth buffered connections the
 * multishot is cancelled, and re-armed once a take brings the buffer below
 * half, or at once when it ended on its own (a connection that found the
 * completion queue full); 0 means no cap. A burst is done when all of its
 * connections have been reaped and taken; both ends are then reset.
 *
 * Connections are accepted in the order they arrived, so the k-th completion
 * of a burst is the k-th connection the client made: the latency recorded is
 * from the client's connect returning to the completion being reaped, which
 * is what ior is answerable for. The time to the take, the loop's own doing,
 * is reported alongside, with how the completions came out: how many per
 * reap, how many passes a burst took, how often the cap cancelled and
 * re-armed the multishot, how often it ended on its own, and how many
 * connections carried IOR_CQE_F_SOCK_NONEMPTY. A connection reported out of
 * order, an accept failing, or a burst not fully reported is an error.
 */
#include "bench_platform.h"
#include "bench_scenario.h"
#include "bench_trace.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Op kinds, packed with the multishot's generation into the SQE user_data. */
enum {
	K_MACCEPT = 0, /* the multishot accept, generation in the index bits */
	K_CANCEL = 1, /* its cancel */
	K_BITS = 3,
	K_MASK = (1u << K_BITS) - 1,
};

static inline void *make_tag(uint32_t gen, unsigned kind)
{
	return (void *) (((uintptr_t) gen << K_BITS) | (kind & K_MASK));
}
static inline uint32_t tag_gen(void *data)
{
	return (uint32_t) ((uintptr_t) data >> K_BITS);
}
static inline unsigned tag_kind(void *data)
{
	return (unsigned) ((uintptr_t) data & K_MASK);
}

typedef struct accept_ctx {
	ior_ctx *ior;
	const bench_options *opts;
	bench_metrics *m; /* arrival to reap */
	bench_metrics take; /* arrival to take */
	ior_fd_t listener;
	struct sockaddr_storage addr;
	socklen_t addrlen;
	uint32_t burst;
	uint32_t cap;
	uint32_t takes;
	uint32_t pass_us;

	/* The client thread: connects a burst when the gate lets it, stamping
	 * each connection as its connect returns and publishing the count. */
	bench_thread thread;
	bench_gate go;
	ior_fd_t *clients;
	uint64_t *arrive_ns;
	_Atomic uint32_t arrived;
	_Atomic int client_failed;

	/* The burst under way, reaped in arrival order. */
	ior_fd_t *accepted;
	uint64_t *reaped_ns;
	uint32_t reaped;
	uint32_t taken;
	uint32_t buffered;
	int armed; /* the multishot is in flight */
	int cancelling; /* its cancel is in flight */
	uint32_t gen;
	uint64_t inflight; /* ops in flight, the multishot counted once */

	/* What the completions looked like. */
	uint64_t bursts;
	uint64_t passes;
	uint64_t reaps; /* peeks that returned something */
	uint64_t cqes;
	uint32_t cqes_max; /* most in one peek */
	uint64_t cancels;
	uint64_t rearms;
	uint64_t ended_on_own;
	uint64_t nonempty;
	uint32_t passes_max; /* most for one burst */
} accept_ctx;

static void client_main(void *arg)
{
	accept_ctx *s = arg;
	while (bench_gate_take(&s->go) > 0) {
		for (uint32_t k = 0; k < s->burst; k++) {
			ior_fd_t fd = IOR_INVALID_FD;
			if (bench_make_tcp_socket(&fd) < 0
					|| bench_connect(fd, (const struct sockaddr *) &s->addr, s->addrlen) < 0) {
				if (bench_fd_is_valid(fd)) {
					bench_close_fd(fd);
				}
				atomic_store(&s->client_failed, 1);
				return;
			}
			s->clients[k] = fd;
			s->arrive_ns[k] = bench_now_ns();
			atomic_store_explicit(&s->arrived, k + 1, memory_order_release);
		}
	}
}

/* Stage the multishot accept (a new generation); 0 if the SQ was full. */
static int arm(accept_ctx *s)
{
	ior_sqe *sqe = ior_get_sqe(s->ior);
	if (!sqe) {
		return 0;
	}
	s->gen++;
	ior_prep_accept_multishot(s->ior, sqe, s->listener, 0);
	ior_sqe_set_data(s->ior, sqe, make_tag(s->gen, K_MACCEPT));
	s->armed = 1;
	s->inflight++;
	return 1;
}

static int cancel(accept_ctx *s)
{
	ior_sqe *sqe = ior_get_sqe(s->ior);
	if (!sqe) {
		return 0;
	}
	ior_prep_cancel(s->ior, sqe, make_tag(s->gen, K_MACCEPT));
	ior_sqe_set_data(s->ior, sqe, make_tag(s->gen, K_CANCEL));
	s->cancelling = 1;
	s->inflight++;
	return 1;
}

static void harvest_one(accept_ctx *s, ior_cqe *cqe, int draining)
{
	void *data = ior_cqe_get_data(s->ior, cqe);
	int32_t res = ior_cqe_get_res(s->ior, cqe);
	uint32_t flags = ior_cqe_get_flags(s->ior, cqe);
	unsigned kind = tag_kind(data);
	uint32_t gen = tag_gen(data);

	if (kind == K_CANCEL) {
		s->inflight--;
		if (res != 0 && res != -ENOENT && res != -EALREADY) {
			bench_metrics_error(s->m);
		}
		return;
	}
	if (kind != K_MACCEPT) {
		bench_metrics_error(s->m);
		return;
	}

	int more = (flags & IOR_CQE_F_MORE) != 0;
	int was_cancelling = s->cancelling || gen != s->gen || draining;
	if (!more) {
		s->inflight--;
		if (gen == s->gen) {
			s->armed = 0;
			s->cancelling = 0;
		}
	}
	if (res < 0) {
		/* A cancelled multishot ends -ECANCELED; anything else is a failure
		 * (of the listener, or of an accept: -EMFILE). */
		if (res != -ECANCELED || more || !was_cancelling) {
			bench_metrics_error(s->m);
		}
		return;
	}
	if (!more) {
		s->ended_on_own++; /* a connection that found the queue full */
	}
	if (flags & IOR_CQE_F_SOCK_NONEMPTY) {
		s->nonempty++;
	}

	/* A connection: the next one of the burst, in arrival order. */
	ior_fd_t fd = bench_fd_from_res(res);
	if (draining || s->reaped >= s->burst) {
		bench_close_fd_abort(fd);
		if (!draining) {
			bench_metrics_error(s->m);
		}
		return;
	}
	uint32_t k = s->reaped++;
	s->accepted[k] = fd;
	/* The client stamps the connection right after its connect returns,
	 * which the kernel may complete a moment before it does. */
	while (atomic_load_explicit(&s->arrived, memory_order_acquire) <= k) {
		if (atomic_load(&s->client_failed)) {
			break;
		}
	}
	uint64_t now = bench_now_ns();
	s->reaped_ns[k] = now;
	bench_metrics_record(s->m, now - s->arrive_ns[k], 0);
	s->buffered++;
}

static void take(accept_ctx *s)
{
	uint32_t n = s->takes ? s->takes : s->buffered;
	if (n > s->buffered) {
		n = s->buffered;
	}
	uint64_t now = bench_now_ns();
	for (uint32_t i = 0; i < n; i++) {
		uint32_t k = s->taken++;
		bench_metrics_record(&s->take, now - s->arrive_ns[k], 0);
	}
	s->buffered -= n;
}

/* Reap everything that is there; how many completions came out. */
static uint32_t reap_all(accept_ctx *s, int draining)
{
	ior_cqe *batch[256];
	uint32_t total = 0;
	for (;;) {
		unsigned n = ior_peek_batch_cqe(s->ior, batch, 256);
		if (n == 0) {
			break;
		}
		for (unsigned i = 0; i < n; i++) {
			harvest_one(s, batch[i], draining);
		}
		ior_cq_advance(s->ior, n);
		s->reaps++;
		s->cqes += n;
		if (n > s->cqes_max) {
			s->cqes_max = n;
		}
		total += n;
	}
	return total;
}

static int run_burst(accept_ctx *s)
{
	s->reaped = 0;
	s->taken = 0;
	atomic_store(&s->arrived, 0);
	uint32_t passes = 0;

	if (!s->armed && !s->cancelling) {
		if (!arm(s)) {
			return -ENOSPC;
		}
		ior_submit(s->ior);
	}
	bench_gate_post(&s->go, 1);

	while (s->reaped < s->burst || s->buffered > 0) {
		if (s->pass_us) {
			bench_sleep_us(s->pass_us);
		} else if (s->armed && s->buffered == 0 && s->reaped < s->burst) {
			ior_cqe *cqe = NULL;
			int ret = bench_wait_completion(s->ior, s->opts, &cqe, s->reaped);
			if (ret < 0 && ret != -EAGAIN && ret != -EINTR && ret != -ETIME) {
				return ret;
			}
		}
		passes++;
		reap_all(s, 0);
		take(s);
		if (atomic_load(&s->client_failed)) {
			return -EIO;
		}

		/* The cap: cancel past it, re-arm below half of it or once the
		 * multishot ended on its own, as long as the burst has more to
		 * come. */
		if (s->cap && s->armed && !s->cancelling && s->buffered > s->cap) {
			if (cancel(s)) {
				s->cancels++;
			}
		} else if (!s->armed && !s->cancelling && s->reaped < s->burst
				&& (!s->cap || s->buffered < s->cap / 2)) {
			if (arm(s)) {
				s->rearms++;
			}
		}
		ior_submit(s->ior);
	}

	s->bursts++;
	s->passes += passes;
	if (passes > s->passes_max) {
		s->passes_max = passes;
	}
	for (uint32_t k = 0; k < s->burst; k++) {
		bench_close_fd_abort(s->accepted[k]);
		bench_close_fd_abort(s->clients[k]);
	}
	return 0;
}

static int run_loop(accept_ctx *s)
{
	const bench_options *o = s->opts;
	uint64_t deadline_ns = o->ops ? 0 : bench_now_ns() + (uint64_t) (o->duration_s * 1e9);
	uint64_t conns = 0;

	while (1) {
		int ret = run_burst(s);
		if (ret < 0) {
			return ret;
		}
		conns += s->burst;
		int done = o->ops ? (conns >= o->ops) : (bench_now_ns() >= deadline_ns);
		if (done) {
			break;
		}
	}
	bench_metrics_stop(s->m);

	/* Drain: the multishot ends with -ECANCELED, its cancel with 0. */
	if (s->armed && !s->cancelling) {
		while (!cancel(s)) {
			ior_submit(s->ior);
		}
		ior_submit(s->ior);
	}
	while (s->inflight > 0) {
		if (reap_all(s, 1) == 0) {
			ior_cqe *cqe = NULL;
			int ret = bench_wait_completion(s->ior, s->opts, &cqe, conns);
			if (ret < 0 && ret != -EAGAIN && ret != -EINTR && ret != -ETIME) {
				return ret;
			}
		}
	}
	return 0;
}

int bench_run_accept(const bench_options *opts, bench_metrics *m, const char **backend_name_out)
{
	int ret = 0;
	accept_ctx s;
	memset(&s, 0, sizeof(s));
	s.opts = opts;
	s.m = m;
	bench_metrics_init(&s.take);
	s.burst = opts->conns ? opts->conns : 1;
	s.cap = opts->depth;
	s.takes = opts->takes;
	s.pass_us = opts->work_us;
	s.listener = IOR_INVALID_FD;
	atomic_init(&s.arrived, 0);
	atomic_init(&s.client_failed, 0);

	uint32_t sq = opts->sq_entries ? opts->sq_entries : 256;
	ret = ior_queue_init(sq, &s.ior);
	if (ret < 0) {
		return ret;
	}
	if (backend_name_out) {
		*backend_name_out = ior_get_backend_name(s.ior);
	}

	s.clients = calloc(s.burst, sizeof(*s.clients));
	s.accepted = calloc(s.burst, sizeof(*s.accepted));
	s.arrive_ns = calloc(s.burst, sizeof(*s.arrive_ns));
	s.reaped_ns = calloc(s.burst, sizeof(*s.reaped_ns));
	if (!s.clients || !s.accepted || !s.arrive_ns || !s.reaped_ns) {
		ret = -ENOMEM;
		goto out;
	}
	if (bench_make_listener(&s.listener, &s.addr, &s.addrlen) < 0) {
		ret = -EIO;
		goto out;
	}
	ret = bench_gate_init(&s.go);
	if (ret < 0) {
		goto out;
	}
	ret = bench_thread_start(&s.thread, client_main, &s);
	if (ret < 0) {
		bench_gate_destroy(&s.go);
		goto out;
	}

	bench_metrics_start(m);
	bench_metrics_start(&s.take);
	ret = run_loop(&s);
	if (m->wall_end_ns == 0) {
		bench_metrics_stop(m);
	}
	bench_metrics_stop(&s.take);

	bench_gate_close(&s.go);
	bench_thread_join(&s.thread);
	bench_gate_destroy(&s.go);

	fprintf(stderr,
			"accept outcomes: bursts=%llu passes/burst avg=%.1f max=%u reaps=%llu cqes/reap "
			"avg=%.1f max=%u cancels=%llu rearms=%llu ended_on_own=%llu nonempty=%llu/%llu\n",
			(unsigned long long) s.bursts, s.bursts ? (double) s.passes / (double) s.bursts : 0.0,
			s.passes_max, (unsigned long long) s.reaps,
			s.reaps ? (double) s.cqes / (double) s.reaps : 0.0, s.cqes_max,
			(unsigned long long) s.cancels, (unsigned long long) s.rearms,
			(unsigned long long) s.ended_on_own, (unsigned long long) s.nonempty,
			(unsigned long long) m->ops);
	fprintf(stderr, "accept arrival->take us: avg=%.1f p50=%.1f p99=%.1f max=%.1f\n",
			s.take.ops ? (double) (s.take.lat_sum_ns / s.take.ops) / 1000.0 : 0.0,
			(double) bench_metrics_percentile(&s.take, 0.50) / 1000.0,
			(double) bench_metrics_percentile(&s.take, 0.99) / 1000.0,
			(double) s.take.lat_max_ns / 1000.0);

out:
	free(s.clients);
	free(s.accepted);
	free(s.arrive_ns);
	free(s.reaped_ns);
	ior_queue_exit(s.ior);
	bench_close_fd(s.listener);
	return ret;
}
