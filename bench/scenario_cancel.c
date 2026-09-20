/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * scenario_cancel.c - async cancellation under load (IOR_OP_ASYNC_CANCEL).
 *
 * Models a provider tearing down the losing members of an Any op or a stream:
 * every connection posts a recv that has nothing to read, so it parks waiting
 * for readiness, and one loop iteration later a cancel is submitted for it.
 * With --race-pct, that fraction of rounds also sends the payload in the same
 * batch as the cancel, so readiness and cancellation race inside the backend
 * (poller versus cancel on the threads backend, poll arming versus
 * IORING_OP_ASYNC_CANCEL on io_uring, AFD aborts on IOCP).
 *
 * A round is one recv, its cancel and, when racing, the send. Correctness is
 * checked per round on the pairing of the two results, since either side may
 * win a race: a recv that ends -ECANCELED must have a cancel of 0 (or
 * -EALREADY, the cancel catching the op mid-attempt); a recv that delivered
 * data must have a cancel of -ENOENT or -EALREADY and an intact payload; any
 * other combination, a duplicate CQE or a lost one is an error. Data left
 * behind by a cancelled recv is accounted per connection and read by a later
 * round, so a "no race" round may still legitimately see data.
 *
 * Latency is the time from submitting the cancel to the recv completing, with
 * either outcome: what a provider waits for after cancelling. Throughput
 * counts rounds. The outcome split (cancelled, data won,
 * cancel results) is printed to stderr at the end of the run.
 */
#include "bench_platform.h"
#include "bench_scenario.h"
#include "bench_trace.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Op kinds, packed with the connection index into the SQE user_data. */
enum {
	K_RECV = 0,
	K_SEND = 1,
	K_CANCEL = 2,
	K_BITS = 2,
	K_MASK = (1u << K_BITS) - 1,
};

/* Per-connection next action, issued during the refill phase. */
enum {
	ACT_NONE = 0,
	ACT_RECV, /* start a round: post the recv */
	ACT_CANCEL, /* one iteration later: send (if racing) and cancel */
};

static inline void *make_tag(uint32_t conn, unsigned kind)
{
	return (void *) (((uintptr_t) conn << K_BITS) | (kind & K_MASK));
}
static inline uint32_t tag_conn(void *data)
{
	return (uint32_t) ((uintptr_t) data >> K_BITS);
}
static inline unsigned tag_kind(void *data)
{
	return (unsigned) ((uintptr_t) data & K_MASK);
}

typedef struct cconn {
	ior_fd_t client; /* the recv side */
	ior_fd_t server; /* the send side */
	char *rbuf;
	unsigned pending; /* ACT_* */
	int race; /* this round sends in the same batch as the cancel */
	unsigned awaiting; /* CQEs still expected this round */
	uint64_t cancel_ns; /* when the cancel was submitted */
	uint64_t unread; /* payload bytes sent but not yet received */
	int32_t recv_res;
	int32_t cancel_res;
	int seen_recv, seen_cancel, seen_send;
} cconn;

typedef struct cancel_ctx {
	ior_ctx *ior;
	const bench_options *opts;
	bench_metrics *m;
	cconn *conns;
	uint32_t nconns;
	char *send_buf;
	uint32_t msg_size;
	uint32_t race_pct;
	uint64_t rng;
	uint64_t inflight;
	uint64_t rounds;
	uint32_t *ready;
	uint32_t ready_count;
	int draining;

	/* Outcome split. */
	uint64_t n_cancelled; /* recv ended -ECANCELED */
	uint64_t n_data; /* recv delivered data */
	uint64_t n_c0, n_calready, n_cnoent; /* cancel results */
} cancel_ctx;

static void set_pending(cancel_ctx *s, uint32_t ci, unsigned act)
{
	s->conns[ci].pending = act;
	s->ready[s->ready_count++] = ci;
}

static int roll_race(cancel_ctx *s)
{
	s->rng = s->rng * 6364136223846793005ULL + 1442695040888963407ULL;
	return (uint32_t) ((s->rng >> 33) % 100) < s->race_pct;
}

/* Issue a connection's pending action; 0 if the SQ was full (retry later). */
static int issue_pending(cancel_ctx *s, uint32_t ci)
{
	cconn *c = &s->conns[ci];

	if (c->pending == ACT_RECV) {
		if (s->draining) {
			c->pending = ACT_NONE;
			return 1;
		}
		ior_sqe *sqe = ior_get_sqe(s->ior);
		if (!sqe) {
			return 0;
		}
		c->race = roll_race(s);
		c->awaiting = c->race ? 3 : 2;
		c->seen_recv = c->seen_cancel = c->seen_send = 0;
		ior_prep_recv(s->ior, sqe, c->client, c->rbuf, s->msg_size, 0);
		ior_sqe_set_data(s->ior, sqe, make_tag(ci, K_RECV));
		s->inflight++;
		c->pending = ACT_NONE;
		/* The cancel goes out on the next refill, once this batch is in. */
		set_pending(s, ci, ACT_CANCEL);
		return 1;
	}

	/* ACT_CANCEL: both entries or none, so a partial batch never leaves a
	 * round without its cancel. */
	ior_sqe *csqe = ior_get_sqe(s->ior);
	if (!csqe) {
		return 0;
	}
	ior_sqe *ssqe = NULL;
	if (c->race) {
		ssqe = ior_get_sqe(s->ior);
		if (!ssqe) {
			/* Give the cancel slot back by issuing it as a NOP-free retry:
			 * there is no un-get, so issue the cancel alone and drop the
			 * race for this round. */
			c->race = 0;
			c->awaiting = 2;
		}
	}
	if (ssqe) {
		ior_prep_send(s->ior, ssqe, c->server, s->send_buf, s->msg_size, 0);
		ior_sqe_set_data(s->ior, ssqe, make_tag(ci, K_SEND));
		c->unread += s->msg_size;
		s->inflight++;
	}
	c->cancel_ns = bench_now_ns();
	ior_prep_cancel(s->ior, csqe, make_tag(ci, K_RECV));
	ior_sqe_set_data(s->ior, csqe, make_tag(ci, K_CANCEL));
	s->inflight++;
	c->pending = ACT_NONE;
	return 1;
}

/* All CQEs of a round are in: check the pairing and account the outcome. */
static void finish_round(cancel_ctx *s, uint32_t ci)
{
	cconn *c = &s->conns[ci];
	int ok = 1;

	if (c->recv_res == -ECANCELED) {
		if (c->cancel_res == 0) {
			s->n_c0++;
		} else if (c->cancel_res == -EALREADY) {
			s->n_calready++;
		} else {
			ok = 0;
		}
		if (ok) {
			s->n_cancelled++;
			bench_metrics_record(s->m, bench_now_ns() - c->cancel_ns, 0);
		}
	} else if (c->recv_res > 0) {
		if (c->cancel_res == -ENOENT) {
			s->n_cnoent++;
		} else if (c->cancel_res == -EALREADY) {
			s->n_calready++;
		} else {
			ok = 0;
		}
		if ((uint64_t) c->recv_res > c->unread) {
			ok = 0; /* more data than was ever sent */
		} else {
			c->unread -= (uint64_t) c->recv_res;
		}
		for (int32_t i = 0; ok && i < c->recv_res; i++) {
			if (c->rbuf[i] != s->send_buf[0]) {
				ok = 0;
			}
		}
		if (ok) {
			s->n_data++;
			bench_metrics_record(s->m, bench_now_ns() - c->cancel_ns, (uint64_t) c->recv_res);
		}
	} else {
		ok = 0;
	}

	if (!ok) {
		bench_metrics_error(s->m);
	}
	s->rounds++;
	if (!s->draining) {
		set_pending(s, ci, ACT_RECV);
	}
}

static void harvest_one(cancel_ctx *s, ior_cqe *cqe)
{
	void *data = ior_cqe_get_data(s->ior, cqe);
	int32_t res = ior_cqe_get_res(s->ior, cqe);
	uint32_t ci = tag_conn(data);
	unsigned kind = tag_kind(data);
	cconn *c = &s->conns[ci];

	s->inflight--;
	BENCH_TRACE4(
			"cqe conn=%llu kind=%llu res=%lld inflight=%llu", ci, kind, (int64_t) res, s->inflight);

	int dup = 0;
	switch (kind) {
		case K_RECV:
			dup = c->seen_recv++;
			c->recv_res = res;
			break;
		case K_CANCEL:
			dup = c->seen_cancel++;
			c->cancel_res = res;
			break;
		case K_SEND:
			dup = c->seen_send++;
			if (res != (int32_t) s->msg_size) {
				bench_metrics_error(s->m);
			}
			break;
		default:
			bench_metrics_error(s->m);
			return;
	}
	if (dup || c->awaiting == 0) {
		bench_metrics_error(s->m); /* a CQE this round did not expect */
		return;
	}
	if (--c->awaiting == 0) {
		finish_round(s, ci);
	}
}

static int run_loop(cancel_ctx *s)
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
			int done = o->ops ? (s->rounds >= o->ops) : (bench_now_ns() >= deadline_ns);
			if (done) {
				s->draining = 1;
				bench_metrics_stop(s->m);
			}
		}

		/*
		 * Refill: recvs first so their batch is submitted before any cancel
		 * targets them; the cancels queued by that pass wait for the next
		 * iteration (the ready list is drained once per pass).
		 */
		uint32_t pass = s->ready_count;
		uint32_t kept = 0;
		for (uint32_t i = 0; i < pass; i++) {
			uint32_t ci = s->ready[i];
			if (!issue_pending(s, ci)) {
				s->ready[kept++] = ci; /* SQ full: retry after harvesting */
			}
		}
		/* Actions queued during this pass sit behind the kept ones. */
		for (uint32_t i = pass; i < s->ready_count; i++) {
			s->ready[kept++] = s->ready[i];
		}
		s->ready_count = kept;

		ior_submit(s->ior);

		if (s->draining && s->inflight == 0 && s->ready_count == 0) {
			break;
		}

		if (n == 0 && s->inflight > 0 && s->ready_count == 0) {
			ior_cqe *cqe = NULL;
			int ret = BENCH_WAIT_CQE(s->ior, &cqe, s->rounds);
			if (ret < 0 && ret != -EAGAIN && ret != -EINTR && ret != -ETIME) {
				return ret;
			}
		}
	}
	return 0;
}

int bench_run_cancel(const bench_options *opts, bench_metrics *m, const char **backend_name_out)
{
	int ret = 0;
	cancel_ctx s;
	memset(&s, 0, sizeof(s));
	s.opts = opts;
	s.m = m;
	s.nconns = opts->conns ? opts->conns : 1;
	s.msg_size = opts->msg_size ? opts->msg_size : 256;
	s.race_pct = opts->race_pct > 100 ? 100 : opts->race_pct;
	s.rng = 0x9e3779b97f4a7c15ULL;

	uint32_t sq = opts->sq_entries;
	if (sq < s.nconns * 4) {
		sq = s.nconns * 4;
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

	s.send_buf = malloc(s.msg_size);
	s.conns = calloc(s.nconns, sizeof(*s.conns));
	/* Every connection can be queued twice in one pass (recv, then cancel). */
	s.ready = calloc((size_t) s.nconns * 2, sizeof(*s.ready));
	if (!s.send_buf || !s.conns || !s.ready) {
		ret = -ENOMEM;
		goto out;
	}
	memset(s.send_buf, 0x5a, s.msg_size);

	uint32_t established = 0;
	for (uint32_t i = 0; i < s.nconns; i++) {
		ior_fd_t fds[2];
		if (bench_make_tcp_pair(fds) < 0) {
			ret = -EIO;
			break;
		}
		s.conns[i].server = fds[0];
		s.conns[i].client = fds[1];
		s.conns[i].rbuf = malloc(s.msg_size);
		if (!s.conns[i].rbuf) {
			bench_close_fd(fds[0]);
			bench_close_fd(fds[1]);
			ret = -ENOMEM;
			break;
		}
		established++;
	}
	s.nconns = established;
	if (established == 0) {
		if (ret == 0) {
			ret = -EIO;
		}
		goto out;
	}

	bench_metrics_start(m);
	for (uint32_t i = 0; i < s.nconns; i++) {
		set_pending(&s, i, ACT_RECV);
	}
	ret = run_loop(&s);
	if (m->wall_end_ns == 0) {
		bench_metrics_stop(m);
	}

	fprintf(stderr,
			"cancel outcomes: rounds=%llu cancelled=%llu data=%llu | cancel res: 0=%llu "
			"-EALREADY=%llu -ENOENT=%llu\n",
			(unsigned long long) s.rounds, (unsigned long long) s.n_cancelled,
			(unsigned long long) s.n_data, (unsigned long long) s.n_c0,
			(unsigned long long) s.n_calready, (unsigned long long) s.n_cnoent);

out:
	if (s.conns) {
		for (uint32_t i = 0; i < s.nconns; i++) {
			bench_close_fd(s.conns[i].server);
			bench_close_fd(s.conns[i].client);
			free(s.conns[i].rbuf);
		}
		free(s.conns);
	}
	free(s.ready);
	free(s.send_buf);
	ior_queue_exit(s.ior);
	return ret;
}
