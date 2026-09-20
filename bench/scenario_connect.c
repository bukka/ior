/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * scenario_connect.c - connection churn: IOR_OP_ACCEPT and IOR_OP_CONNECT
 * under load.
 *
 * One listener keeps an accept parked per client slot. Every client slot
 * connects, sends its slot index over the new connection, and the accept side
 * receives it: that pairs the accepted socket with the client it serves (an
 * accept cannot otherwise be told which connect it took) and proves the
 * accepted socket does I/O (on IOCP, that its accept context was updated).
 * Both ends are then closed with a reset, so no TIME_WAIT builds up and the
 * ephemeral port range is not exhausted at churn rates, and the slot starts
 * over. Every round therefore also opens sockets whose handle values the OS
 * has just recycled, which is what caught the IOCP backend caching handle
 * associations across a close.
 *
 * A round is connect, accept, and the send and recv of the index. Latency is
 * from submitting the connect to the index arriving on the accepted socket:
 * the handshake plus the first byte. Draining stops new connects, lets rounds
 * in flight finish, then cancels the parked accepts, so every run also
 * exercises cancel on a parked accept (0 or -ENOENT, paired with -ECANCELED).
 * Numbers are dominated by the kernel's loopback handshake; this is a stress
 * and regression guard rather than a measure of ior itself.
 */
#include "bench_platform.h"
#include "bench_scenario.h"
#include "bench_trace.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Op kinds, packed with the slot index into the SQE user_data. */
enum {
	K_CONNECT = 0, /* client slot */
	K_SEND = 1, /* client slot: its index over the new connection */
	K_ACCEPT = 2, /* accept slot */
	K_RECV = 3, /* accept slot: the index, on the accepted socket */
	K_CANCEL = 4, /* accept slot: cancel of its parked accept, at drain */
	K_BITS = 3,
	K_MASK = (1u << K_BITS) - 1,
};

/* Pending actions, issued during the refill phase. */
enum {
	ACT_NONE = 0,
	ACT_CONNECT, /* client: start a round */
	ACT_SEND, /* client: connected, send the index */
	ACT_ACCEPT, /* accept slot: park an accept */
	ACT_RECV, /* accept slot: accepted, read the index */
	ACT_CANCEL, /* accept slot: cancel the parked accept (drain) */
};

/* Ready-list entries name a slot and its side. */
#define ENTRY_ACCEPT_SIDE 1u
#define ENTRY(idx, side) (((idx) << 1) | (side))
#define ENTRY_IDX(e) ((e) >> 1)
#define ENTRY_SIDE(e) ((e) & 1u)

static inline void *make_tag(uint32_t idx, unsigned kind)
{
	return (void *) (((uintptr_t) idx << K_BITS) | (kind & K_MASK));
}
static inline uint32_t tag_idx(void *data)
{
	return (uint32_t) ((uintptr_t) data >> K_BITS);
}
static inline unsigned tag_kind(void *data)
{
	return (unsigned) ((uintptr_t) data & K_MASK);
}

typedef struct client_slot {
	ior_fd_t fd; /* the connecting socket, IOR_INVALID_FD between rounds */
	uint32_t id_buf; /* payload: this slot's index */
	uint64_t start_ns; /* when the connect was submitted */
	int busy; /* a round is in progress (until its index is received) */
	unsigned pending;
} client_slot;

typedef struct accept_slot {
	ior_fd_t fd; /* the accepted socket while the index is being read */
	uint32_t id_buf;
	int parked; /* an accept is in flight */
	unsigned pending;
} accept_slot;

typedef struct connect_ctx {
	ior_ctx *ior;
	const bench_options *opts;
	bench_metrics *m;
	ior_fd_t listener;
	struct sockaddr_storage addr;
	socklen_t addrlen;
	client_slot *clients;
	accept_slot *accepts;
	uint32_t nslots;
	uint64_t inflight;
	uint64_t rounds;
	uint64_t port_stalls; /* connects the kernel had no ephemeral port for */
	uint32_t *ready;
	uint32_t ready_count;
	uint32_t ready_cap;
	int draining; /* 1: no new connects; 2: clients idle, accepts cancelled */
} connect_ctx;

static void set_client_pending(connect_ctx *s, uint32_t i, unsigned act)
{
	s->clients[i].pending = act;
	if (s->ready_count < s->ready_cap) {
		s->ready[s->ready_count++] = ENTRY(i, 0);
	}
}

static void set_accept_pending(connect_ctx *s, uint32_t i, unsigned act)
{
	s->accepts[i].pending = act;
	if (s->ready_count < s->ready_cap) {
		s->ready[s->ready_count++] = ENTRY(i, ENTRY_ACCEPT_SIDE);
	}
}

/* Finish a client slot's round (or abandon it) and start the next one. */
static void client_reset(connect_ctx *s, uint32_t i)
{
	client_slot *c = &s->clients[i];
	if (bench_fd_is_valid(c->fd)) {
		bench_close_fd_abort(c->fd);
		c->fd = IOR_INVALID_FD;
	}
	c->busy = 0;
	if (!s->draining) {
		set_client_pending(s, i, ACT_CONNECT);
	}
}

/* Release an accept slot's accepted socket and park a new accept. */
static void accept_reset(connect_ctx *s, uint32_t i)
{
	accept_slot *a = &s->accepts[i];
	if (bench_fd_is_valid(a->fd)) {
		bench_close_fd_abort(a->fd);
		a->fd = IOR_INVALID_FD;
	}
	if (s->draining < 2) {
		set_accept_pending(s, i, ACT_ACCEPT);
	}
}

/* Issue one slot's pending action; 0 if the SQ was full (retry later). */
static int issue_pending(connect_ctx *s, uint32_t entry)
{
	uint32_t i = ENTRY_IDX(entry);

	if (ENTRY_SIDE(entry) == 0) {
		client_slot *c = &s->clients[i];
		if (c->pending == ACT_CONNECT) {
			if (s->draining) {
				c->pending = ACT_NONE;
				return 1;
			}
			if (!bench_fd_is_valid(c->fd) && bench_make_tcp_socket(&c->fd) < 0) {
				bench_metrics_error(s->m);
				c->pending = ACT_NONE;
				return 1;
			}
			ior_sqe *sqe = ior_get_sqe(s->ior);
			if (!sqe) {
				return 0;
			}
			c->busy = 1;
			c->start_ns = bench_now_ns();
			ior_prep_connect(s->ior, sqe, c->fd, (const struct sockaddr *) &s->addr, s->addrlen);
			ior_sqe_set_data(s->ior, sqe, make_tag(i, K_CONNECT));
		} else {
			ior_sqe *sqe = ior_get_sqe(s->ior);
			if (!sqe) {
				return 0;
			}
			c->id_buf = i;
			ior_prep_send(s->ior, sqe, c->fd, &c->id_buf, sizeof(c->id_buf), 0);
			ior_sqe_set_data(s->ior, sqe, make_tag(i, K_SEND));
		}
		s->inflight++;
		c->pending = ACT_NONE;
		return 1;
	}

	accept_slot *a = &s->accepts[i];
	if (a->pending == ACT_ACCEPT) {
		if (s->draining == 2) {
			a->pending = ACT_NONE;
			return 1;
		}
		ior_sqe *sqe = ior_get_sqe(s->ior);
		if (!sqe) {
			return 0;
		}
		ior_prep_accept(s->ior, sqe, s->listener, NULL, NULL, 0);
		ior_sqe_set_data(s->ior, sqe, make_tag(i, K_ACCEPT));
		a->parked = 1;
	} else if (a->pending == ACT_RECV) {
		ior_sqe *sqe = ior_get_sqe(s->ior);
		if (!sqe) {
			return 0;
		}
		a->id_buf = UINT32_MAX;
		ior_prep_recv(s->ior, sqe, a->fd, &a->id_buf, sizeof(a->id_buf), 0);
		ior_sqe_set_data(s->ior, sqe, make_tag(i, K_RECV));
	} else {
		ior_sqe *sqe = ior_get_sqe(s->ior);
		if (!sqe) {
			return 0;
		}
		ior_prep_cancel(s->ior, sqe, make_tag(i, K_ACCEPT));
		ior_sqe_set_data(s->ior, sqe, make_tag(i, K_CANCEL));
	}
	s->inflight++;
	a->pending = ACT_NONE;
	return 1;
}

static void harvest_one(connect_ctx *s, ior_cqe *cqe)
{
	void *data = ior_cqe_get_data(s->ior, cqe);
	int32_t res = ior_cqe_get_res(s->ior, cqe);
	uint32_t i = tag_idx(data);
	unsigned kind = tag_kind(data);

	s->inflight--;
	BENCH_TRACE4(
			"cqe slot=%llu kind=%llu res=%lld inflight=%llu", i, kind, (int64_t) res, s->inflight);

	switch (kind) {
		case K_CONNECT:
			if (res != 0) {
				/* The ephemeral range cycles every couple of seconds at these
				 * rates, so the kernel occasionally has no port to give. That
				 * is the machine's limit rather than a failure of the op:
				 * count it and let the slot start over. */
				if (res == -EADDRNOTAVAIL) {
					s->port_stalls++;
				} else {
					bench_metrics_error(s->m);
				}
				client_reset(s, i);
				break;
			}
			set_client_pending(s, i, ACT_SEND);
			break;

		case K_SEND:
			if (res != (int32_t) sizeof(uint32_t)) {
				bench_metrics_error(s->m);
				client_reset(s, i);
			}
			/* Otherwise the round ends when the accept side reads the index. */
			break;

		case K_ACCEPT: {
			accept_slot *a = &s->accepts[i];
			a->parked = 0;
			if (res == -ECANCELED && s->draining == 2) {
				break; /* the drain's cancel */
			}
			if (res < 0) {
				bench_metrics_error(s->m);
				accept_reset(s, i);
				break;
			}
			a->fd = bench_fd_from_res(res);
			set_accept_pending(s, i, ACT_RECV);
			break;
		}

		case K_RECV: {
			accept_slot *a = &s->accepts[i];
			uint32_t id = a->id_buf;
			if (res != (int32_t) sizeof(uint32_t) || id >= s->nslots || !s->clients[id].busy) {
				bench_metrics_error(s->m);
			} else {
				bench_metrics_record(
						s->m, bench_now_ns() - s->clients[id].start_ns, sizeof(uint32_t));
				s->rounds++;
				client_reset(s, id);
			}
			accept_reset(s, i);
			break;
		}

		case K_CANCEL:
			/* -EALREADY: the accept was inside its non-blocking attempt. The
			 * cancel still claims it, so it finishes rather than parking
			 * again and the drain is not held up. */
			if (res != 0 && res != -ENOENT && res != -EALREADY) {
				bench_metrics_error(s->m);
			}
			break;

		default:
			bench_metrics_error(s->m);
			break;
	}
}

static int clients_idle(const connect_ctx *s)
{
	for (uint32_t i = 0; i < s->nslots; i++) {
		if (s->clients[i].busy || s->clients[i].pending != ACT_NONE) {
			return 0;
		}
	}
	return 1;
}

static int run_loop(connect_ctx *s)
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
		if (s->draining == 1 && clients_idle(s)) {
			/* No connection can arrive any more: take the parked accepts down. */
			s->draining = 2;
			for (uint32_t i = 0; i < s->nslots; i++) {
				if (s->accepts[i].parked) {
					set_accept_pending(s, i, ACT_CANCEL);
				}
			}
		}

		/* Refill: one pass over the ready list; what it queues waits for the
		 * next iteration, what the full SQ refused is kept. */
		uint32_t pass = s->ready_count;
		uint32_t kept = 0;
		for (uint32_t i = 0; i < pass; i++) {
			uint32_t e = s->ready[i];
			if (!issue_pending(s, e)) {
				s->ready[kept++] = e;
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

int bench_run_connect(const bench_options *opts, bench_metrics *m, const char **backend_name_out)
{
	int ret = 0;
	connect_ctx s;
	memset(&s, 0, sizeof(s));
	s.opts = opts;
	s.m = m;
	s.nslots = opts->conns ? opts->conns : 1;
	s.listener = IOR_INVALID_FD;

	uint32_t sq = opts->sq_entries;
	if (sq < s.nslots * 4 + 64) {
		sq = s.nslots * 4 + 64;
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

	s.ready_cap = 2 * s.nslots + 16;
	s.clients = calloc(s.nslots, sizeof(*s.clients));
	s.accepts = calloc(s.nslots, sizeof(*s.accepts));
	s.ready = calloc(s.ready_cap, sizeof(*s.ready));
	if (!s.clients || !s.accepts || !s.ready) {
		ret = -ENOMEM;
		goto out;
	}
	for (uint32_t i = 0; i < s.nslots; i++) {
		s.clients[i].fd = IOR_INVALID_FD;
		s.accepts[i].fd = IOR_INVALID_FD;
	}

	if (bench_make_listener(&s.listener, &s.addr, &s.addrlen) < 0) {
		ret = -EIO;
		goto out;
	}

	bench_metrics_start(m);
	for (uint32_t i = 0; i < s.nslots; i++) {
		set_accept_pending(&s, i, ACT_ACCEPT);
		set_client_pending(&s, i, ACT_CONNECT);
	}
	ret = run_loop(&s);
	if (m->wall_end_ns == 0) {
		bench_metrics_stop(m);
	}

	fprintf(stderr, "connect outcomes: rounds=%llu port_stalls=%llu\n", (unsigned long long) s.rounds,
			(unsigned long long) s.port_stalls);

out:
	if (s.clients) {
		for (uint32_t i = 0; i < s.nslots; i++) {
			if (bench_fd_is_valid(s.clients[i].fd)) {
				bench_close_fd_abort(s.clients[i].fd);
			}
		}
		free(s.clients);
	}
	if (s.accepts) {
		for (uint32_t i = 0; i < s.nslots; i++) {
			if (bench_fd_is_valid(s.accepts[i].fd)) {
				bench_close_fd_abort(s.accepts[i].fd);
			}
		}
		free(s.accepts);
	}
	free(s.ready);
	ior_queue_exit(s.ior);
	bench_close_fd(s.listener);
	return ret;
}
