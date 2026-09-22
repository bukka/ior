/* SPDX-License-Identifier: BSD-3-Clause */
#ifndef IOR_THREADS_POLLER_H
#define IOR_THREADS_POLLER_H

#include "config.h"
#include <stdint.h>

/*
 * Single-thread readiness multiplexer for the threads backend. All pending
 * IOR_OP_POLL requests (and, in the future, readiness gates for blocking I/O
 * ops) share one poller thread instead of each blocking a worker.
 *
 * Implementations selected at configure time: epoll on Linux
 * (ior_threads_poller_epoll.c), kqueue on BSD/macOS
 * (ior_threads_poller_kqueue.c), portable poll() elsewhere
 * (ior_threads_poller_poll.c).
 */

typedef struct ior_threads_poller ior_threads_poller;

/*
 * Mask bit of a process watch: fd is a pid and the request completes with
 * IOR_POLL_IN once that process exits. Only the kqueue poller (EVFILT_PROC)
 * takes it; on Linux a pidfd is polled instead and elsewhere the thread
 * backend never asks.
 */
#define IOR_THREADS_POLLER_PROC (1U << 31)

/*
 * Mask bit of a persistent (multishot) request: the callback runs with the
 * ready mask at every readiness edge and the request stays registered, until
 * it is cancelled, reaches its deadline or fails, which runs the callback a
 * last time with the negative result and drops it. The epoll and kqueue
 * pollers watch such a request edge-triggered (EPOLLET, EV_CLEAR), on a
 * dup(2) of the descriptor so that one-shot requests on the same descriptor
 * keep their level-triggered registration. The poll(2) poller cannot see
 * edges: it reports readiness that persists again after
 * IOR_THREADS_POLLER_MULTI_REARM_NS.
 */
#define IOR_THREADS_POLLER_MULTI (1U << 30)
#define IOR_THREADS_POLLER_MULTI_REARM_NS 1000000ULL

/*
 * Completion callback, invoked on the poller thread with no poller lock held.
 * res is the ready IOR_POLL_* mask (> 0), -ETIME (deadline reached),
 * -ECANCELED (cancelled or poller shutdown), or another negative errno (e.g.
 * -EBADF). `more` is non-zero for an edge of a multishot request, which stays
 * registered; the request is done with any other call (a multishot one can
 * also end with a positive res, its last readiness, when there are no edges
 * to watch). Must not block for long and must not call back into the poller.
 */
typedef void (*ior_threads_poller_cb)(void *owner, void *req, int res, int more);

/* Create the poller and start its thread. */
int ior_threads_poller_create(
		ior_threads_poller **poller_out, void *owner, ior_threads_poller_cb cb);

/*
 * Register a readiness request. ior_mask is an IOR_POLL_* mask, one-shot
 * unless IOR_THREADS_POLLER_MULTI is set; deadline_ns is an absolute monotonic
 * deadline (0 = none). Thread-safe against the poller thread, but not against
 * destroy().
 */
int ior_threads_poller_add(
		ior_threads_poller *poller, int fd, uint32_t ior_mask, uint64_t deadline_ns, void *req);

/*
 * Cancel a pending request. Returns 0 if it was found: it then completes with
 * -ECANCELED on the poller thread, whatever readiness it may see meanwhile.
 * Returns -ENOENT if it has already been dispatched (its callback has run or
 * is running) or was never added. Thread-safe against add() and the poller
 * thread, but not against destroy().
 */
int ior_threads_poller_cancel(ior_threads_poller *poller, void *req);

/*
 * Complete all pending requests with -ECANCELED, then stop and join the
 * poller thread. No add() or cancel() may run concurrently or after.
 */
void ior_threads_poller_destroy(ior_threads_poller *poller);

#endif /* IOR_THREADS_POLLER_H */
