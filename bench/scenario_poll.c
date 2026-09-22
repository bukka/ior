/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * scenario_poll.c - readiness-driven receive: multishot IOR_OP_POLL under load.
 *
 * Models an event loop that keeps a persistent poll on every connection and
 * reads only when told to: each connection's server socket carries one
 * multishot poll for the whole run, the client sends a message, the poll's
 * edge completion (IOR_CQE_F_MORE) triggers recvs until the message is in,
 * and the next message goes out. A round is send, edge, recv(s); its latency
 * runs from submitting the send to the last byte arriving.
 *
 * The client sends the next message only once the previous one was fully
 * read, so the socket is empty whenever the reader stops: that is the
 * drain-then-wait discipline an edge-triggered watch requires. An edge with
 * nothing outstanding to read is counted as stray, not as an error: the
 * level-triggered pollers (poll(2), WSAPoll) repeat readiness that persists,
 * and TCP may deliver one message as several segments. Every edge must carry
 * IOR_CQE_F_MORE and IOR_POLL_IN, and no poll may end on its own.
 *
 * With --timer linked each poll is guarded by a link timeout, which must stay
 * armed across the edges (firing is an error). Draining stops new sends, lets
 * the rounds finish, then cancels every poll: each must end with -ECANCELED
 * and no IOR_CQE_F_MORE, its cancel with 0, its link timeout with -ECANCELED.
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
	K_POLL = 0, /* the multishot poll on the server socket */
	K_LINK_TIMEOUT = 1, /* its guard, with --timer linked */
	K_SEND = 2, /* client: one message */
	K_RECV = 3, /* server: part of the message */
	K_CANCEL = 4, /* cancel of the poll, at drain */
	K_BITS = 3,
	K_MASK = (1u << K_BITS) - 1,
};

/* Pending actions, issued during the refill phase. */
enum {
	ACT_NONE = 0,
	ACT_POLL, /* arm the poll (and its guard) */
	ACT_SEND,
	ACT_RECV,
	ACT_CANCEL,
};

static inline void *make_tag(uint32_t ci, unsigned kind)
{
	return (void *) (((uintptr_t) ci << K_BITS) | (kind & K_MASK));
}
static inline uint32_t tag_conn(void *data)
{
	return (uint32_t) ((uintptr_t) data >> K_BITS);
}
static inline unsigned tag_kind(void *data)
{
	return (unsigned) ((uintptr_t) data & K_MASK);
}

typedef struct conn {
	ior_fd_t client; /* fds[1] */
	ior_fd_t server; /* fds[0] */
	char *rbuf; /* the message being received */
	uint32_t remaining; /* bytes of the current message still to read (0 = idle) */
	int recv_inflight;
	int polling; /* the multishot poll is armed */
	uint64_t rt_start_ns;
	unsigned pending;
} conn;

typedef struct poll_ctx {
	ior_ctx *ior;
	const bench_options *opts;
	bench_metrics *m;
	conn *conns;
	uint32_t nconns;
	char *send_buf;
	uint32_t msg_size;
	ior_timespec timeout;
	int linked;
	uint64_t inflight; /* ops (polls included) not yet finally completed */
	uint64_t rounds;
	uint64_t edges;
	uint64_t stray_edges;
	uint32_t *ready;
	uint32_t ready_count;
	uint32_t ready_cap;
	int draining; /* 1: no new sends; 2: rounds done, polls cancelled */
} poll_ctx;

/* A connection already listed (a stray edge repeating what a queued recv
 * will do) keeps its one entry. */
static void set_pending(poll_ctx *s, uint32_t ci, unsigned act)
{
	if (s->conns[ci].pending != ACT_NONE) {
		s->conns[ci].pending = act;
		return;
	}
	s->conns[ci].pending = act;
	if (s->ready_count < s->ready_cap) {
		s->ready[s->ready_count++] = ci;
	}
}

/* Issue one connection's pending action; 0 if the SQ was full (retry later). */
static int issue_pending(poll_ctx *s, uint32_t ci)
{
	conn *c = &s->conns[ci];
	unsigned act = c->pending;

	if (act == ACT_NONE || (act == ACT_SEND && s->draining)) {
		c->pending = ACT_NONE;
		return 1;
	}

	ior_sqe *sqe = ior_get_sqe(s->ior);
	if (!sqe) {
		return 0;
	}

	switch (act) {
		case ACT_POLL:
			ior_prep_poll_multishot(s->ior, sqe, c->server, IOR_POLL_IN);
			ior_sqe_set_data(s->ior, sqe, make_tag(ci, K_POLL));
			c->polling = 1;
			s->inflight++;
			if (s->linked) {
				ior_sqe *lsqe = ior_get_sqe(s->ior);
				if (lsqe) {
					ior_sqe_set_flags(s->ior, sqe, IOR_SQE_IO_LINK);
					ior_prep_link_timeout(s->ior, lsqe, &s->timeout, 0);
					ior_sqe_set_data(s->ior, lsqe, make_tag(ci, K_LINK_TIMEOUT));
					s->inflight++;
				}
			}
			break;

		case ACT_SEND:
			c->remaining = s->msg_size;
			c->rt_start_ns = bench_now_ns();
			ior_prep_send(s->ior, sqe, c->client, s->send_buf, s->msg_size, 0);
			ior_sqe_set_data(s->ior, sqe, make_tag(ci, K_SEND));
			s->inflight++;
			break;

		case ACT_RECV:
			ior_prep_recv(s->ior, sqe, c->server, c->rbuf + (s->msg_size - c->remaining),
					c->remaining, 0);
			ior_sqe_set_data(s->ior, sqe, make_tag(ci, K_RECV));
			c->recv_inflight = 1;
			s->inflight++;
			break;

		case ACT_CANCEL:
			ior_prep_cancel(s->ior, sqe, make_tag(ci, K_POLL));
			ior_sqe_set_data(s->ior, sqe, make_tag(ci, K_CANCEL));
			s->inflight++;
			break;

		default:
			ior_prep_nop(s->ior, sqe);
			ior_sqe_set_data(s->ior, sqe, make_tag(ci, K_MASK));
			s->inflight++;
			bench_metrics_error(s->m);
			break;
	}

	BENCH_TRACE3("issue conn=%llu act=%llu inflight=%llu", ci, act, s->inflight);
	c->pending = ACT_NONE;
	return 1;
}

static void harvest_one(poll_ctx *s, ior_cqe *cqe)
{
	void *data = ior_cqe_get_data(s->ior, cqe);
	int32_t res = ior_cqe_get_res(s->ior, cqe);
	uint32_t flags = ior_cqe_get_flags(s->ior, cqe);
	uint32_t ci = tag_conn(data);
	unsigned kind = tag_kind(data);
	conn *c = &s->conns[ci];

	BENCH_TRACE4("cqe conn=%llu kind=%llu res=%lld flags=%llu", ci, kind, (int64_t) res, flags);

	switch (kind) {
		case K_POLL:
			if (flags & IOR_CQE_F_MORE) {
				/* An edge: the poll stays armed. */
				s->edges++;
				if (res <= 0 || !(res & IOR_POLL_IN) || (res & (IOR_POLL_ERR | IOR_POLL_HUP))) {
					bench_metrics_error(s->m);
					break;
				}
				if (c->remaining > 0 && !c->recv_inflight) {
					set_pending(s, ci, ACT_RECV);
				} else {
					s->stray_edges++;
				}
				break;
			}
			/* The last completion: only the drain's cancel may cause it. */
			s->inflight--;
			c->polling = 0;
			if (res != -ECANCELED || s->draining != 2) {
				bench_metrics_error(s->m);
			}
			break;

		case K_LINK_TIMEOUT:
			s->inflight--;
			if (res != -ECANCELED) {
				bench_metrics_error(s->m); /* -ETIME: it fired */
			}
			break;

		case K_SEND:
			s->inflight--;
			if (res != (int32_t) s->msg_size) {
				bench_metrics_error(s->m);
				c->remaining = 0;
				if (!s->draining) {
					set_pending(s, ci, ACT_SEND);
				}
			}
			/* Otherwise the round goes on when the poll reports the data. */
			break;

		case K_RECV:
			s->inflight--;
			c->recv_inflight = 0;
			if (res <= 0 || (uint32_t) res > c->remaining) {
				bench_metrics_error(s->m);
				c->remaining = 0;
				if (!s->draining) {
					set_pending(s, ci, ACT_SEND);
				}
				break;
			}
			c->remaining -= (uint32_t) res;
			if (c->remaining > 0) {
				/* The rest may still be in flight; read on. */
				set_pending(s, ci, ACT_RECV);
				break;
			}
			if (memcmp(c->rbuf, s->send_buf, s->msg_size) != 0) {
				bench_metrics_error(s->m);
			} else {
				bench_metrics_record(s->m, bench_now_ns() - c->rt_start_ns, s->msg_size);
				s->rounds++;
			}
			if (!s->draining) {
				set_pending(s, ci, ACT_SEND);
			}
			break;

		case K_CANCEL:
			s->inflight--;
			if (res != 0) {
				bench_metrics_error(s->m);
			}
			break;

		default:
			s->inflight--;
			bench_metrics_error(s->m);
			break;
	}
}

static int rounds_idle(const poll_ctx *s)
{
	for (uint32_t i = 0; i < s->nconns; i++) {
		const conn *c = &s->conns[i];
		if (c->remaining > 0 || c->recv_inflight || c->pending != ACT_NONE) {
			return 0;
		}
	}
	return 1;
}

static int run_loop(poll_ctx *s)
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
		if (s->draining == 1 && rounds_idle(s)) {
			s->draining = 2;
			for (uint32_t i = 0; i < s->nconns; i++) {
				if (s->conns[i].polling) {
					set_pending(s, i, ACT_CANCEL);
				}
			}
		}

		/* Refill: one pass over the ready list; what it queues waits for the
		 * next iteration, what the full SQ refused is kept. */
		uint32_t pass = s->ready_count;
		uint32_t kept = 0;
		for (uint32_t i = 0; i < pass; i++) {
			uint32_t ci = s->ready[i];
			if (!issue_pending(s, ci)) {
				s->ready[kept++] = ci;
			}
		}
		for (uint32_t i = pass; i < s->ready_count; i++) {
			s->ready[kept++] = s->ready[i];
		}
		s->ready_count = kept;

		ior_submit(s->ior);

		if (s->draining == 2 && s->inflight == 0 && s->ready_count == 0) {
			break;
		}

		if (n == 0 && s->inflight > 0 && s->ready_count == 0) {
			ior_cqe *cqe = NULL;
			int ret = bench_wait_completion(s->ior, s->opts, &cqe, s->rounds);
			if (ret < 0 && ret != -EAGAIN && ret != -EINTR && ret != -ETIME) {
				return ret;
			}
		}
	}
	return 0;
}

int bench_run_poll(const bench_options *opts, bench_metrics *m, const char **backend_name_out)
{
	int ret = 0;
	poll_ctx s;
	memset(&s, 0, sizeof(s));
	s.opts = opts;
	s.m = m;
	s.nconns = opts->conns ? opts->conns : 1;
	s.msg_size = opts->msg_size ? opts->msg_size : 256;
	s.linked = opts->timer_mode == BENCH_TIMER_LINKED;
	s.timeout.tv_sec = opts->timeout_ms / 1000;
	s.timeout.tv_nsec = (long long) (opts->timeout_ms % 1000) * 1000000LL;

	/* Every connection holds its poll (and guard) plus a send or recv in
	 * flight, and the drain adds a cancel each. */
	uint32_t sq = opts->sq_entries;
	if (sq < s.nconns * 4 + 64) {
		sq = s.nconns * 4 + 64;
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

	s.ready_cap = 2 * s.nconns + 16;
	s.conns = calloc(s.nconns, sizeof(*s.conns));
	s.ready = calloc(s.ready_cap, sizeof(*s.ready));
	s.send_buf = malloc(s.msg_size);
	if (!s.conns || !s.ready || !s.send_buf) {
		ret = -ENOMEM;
		goto out;
	}
	for (uint32_t i = 0; i < s.msg_size; i++) {
		s.send_buf[i] = (char) ('a' + (i % 26));
	}
	for (uint32_t i = 0; i < s.nconns; i++) {
		s.conns[i].client = IOR_INVALID_FD;
		s.conns[i].server = IOR_INVALID_FD;
	}
	for (uint32_t i = 0; i < s.nconns; i++) {
		ior_fd_t fds[2];
		if (bench_make_tcp_pair(fds) < 0) {
			ret = -EIO;
			goto out;
		}
		s.conns[i].server = fds[0];
		s.conns[i].client = fds[1];
		s.conns[i].rbuf = malloc(s.msg_size);
		if (!s.conns[i].rbuf) {
			ret = -ENOMEM;
			goto out;
		}
	}

	bench_metrics_start(m);
	for (uint32_t i = 0; i < s.nconns; i++) {
		set_pending(&s, i, ACT_POLL);
	}
	/* The polls go out first so no send can beat its watch. */
	uint32_t pass = s.ready_count;
	s.ready_count = 0;
	for (uint32_t i = 0; i < pass; i++) {
		if (!issue_pending(&s, i)) {
			ret = -EAGAIN;
			goto out;
		}
	}
	ior_submit(s.ior);
	for (uint32_t i = 0; i < s.nconns; i++) {
		set_pending(&s, i, ACT_SEND);
	}
	ret = run_loop(&s);
	if (m->wall_end_ns == 0) {
		bench_metrics_stop(m);
	}

	fprintf(stderr, "poll outcomes: rounds=%llu edges=%llu stray_edges=%llu\n",
			(unsigned long long) s.rounds, (unsigned long long) s.edges,
			(unsigned long long) s.stray_edges);

out:
	ior_queue_exit(s.ior);
	if (s.conns) {
		for (uint32_t i = 0; i < s.nconns; i++) {
			if (bench_fd_is_valid(s.conns[i].client)) {
				bench_close_fd(s.conns[i].client);
			}
			if (bench_fd_is_valid(s.conns[i].server)) {
				bench_close_fd(s.conns[i].server);
			}
			free(s.conns[i].rbuf);
		}
		free(s.conns);
	}
	free(s.ready);
	free(s.send_buf);
	return ret;
}
