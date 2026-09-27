/* SPDX-License-Identifier: BSD-3-Clause */
#include "config.h"

#ifdef IOR_HAVE_THREADS

#include "ior_backend.h"
#include "ior_threads.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <poll.h>
#include <limits.h>

/* Ensure size is power of 2 */
static uint32_t ior_threads_round_up_pow2(uint32_t n)
{
	if (n == 0) {
		return 1;
	}

	n--;
	n |= n >> 1;
	n |= n >> 2;
	n |= n >> 4;
	n |= n >> 8;
	n |= n >> 16;
	n++;

	return n;
}

/* Backend operations */

static int ior_threads_backend_init(void **backend_ctx, ior_params *params)
{
	if (!backend_ctx || !params) {
		return -EINVAL;
	}

	// Allocate context
	ior_ctx_threads *ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		return -ENOMEM;
	}

	ctx->flags = params->flags;

	// Determine ring sizes
	uint32_t sq_entries = params->sq_entries;
	if (sq_entries < IOR_THREADS_MIN_ENTRIES) {
		sq_entries = IOR_THREADS_MIN_ENTRIES;
	}
	sq_entries = ior_threads_round_up_pow2(sq_entries);

	uint32_t cq_entries = params->cq_entries;
	if (cq_entries == 0) {
		cq_entries = sq_entries * IOR_THREADS_CQ_MULTIPLIER;
	}
	if (cq_entries < IOR_THREADS_MIN_ENTRIES) {
		cq_entries = IOR_THREADS_MIN_ENTRIES;
	}
	cq_entries = ior_threads_round_up_pow2(cq_entries);

	// Initialize submission queue ring
	int ret = ior_threads_ring_init(&ctx->sq_ring, sq_entries, 1);
	if (ret < 0) {
		free(ctx);
		return ret;
	}

	// Initialize completion queue ring
	ret = ior_threads_ring_init(&ctx->cq_ring, cq_entries, 0);
	if (ret < 0) {
		ior_threads_ring_destroy(&ctx->sq_ring);
		free(ctx);
		return ret;
	}

	// Initialize event notification
	ret = ior_threads_event_init(&ctx->event);
	if (ret < 0) {
		ior_threads_ring_destroy(&ctx->cq_ring);
		ior_threads_ring_destroy(&ctx->sq_ring);
		free(ctx);
		return ret;
	}

	// Create thread pool (0 = auto-detect based on CPU count)
	ctx->pool = ior_threads_pool_create(ctx, 0);
	if (!ctx->pool) {
		ior_threads_event_destroy(&ctx->event);
		ior_threads_ring_destroy(&ctx->cq_ring);
		ior_threads_ring_destroy(&ctx->sq_ring);
		free(ctx);
		return -ENOMEM;
	}

	// Set supported features
	ctx->features = IOR_FEAT_WORK | IOR_FEAT_POLL_ADD;
#ifdef IOR_HAVE_SPLICE
	ctx->features |= IOR_FEAT_SPLICE;
#endif

	params->sq_entries = ctx->sq_ring.size;
	params->cq_entries = ctx->cq_ring.size;
	params->features = ctx->features;

	*backend_ctx = ctx;
	return 0;
}

static void ior_threads_backend_destroy(void *backend_ctx)
{
	if (!backend_ctx) {
		return;
	}

	ior_ctx_threads *ctx = backend_ctx;

	// Destroy thread pool first (waits for threads to finish)
	ior_threads_pool_destroy(ctx->pool);

	// Cleanup event notification
	ior_threads_event_destroy(&ctx->event);

	// Cleanup rings
	ior_threads_ring_destroy(&ctx->cq_ring);
	ior_threads_ring_destroy(&ctx->sq_ring);

	// Free context
	free(ctx);
}

/*
 * ior_queue_forget(): in a forked child, which has none of the pool's
 * threads. Their locks may have been held at the fork, so nothing that
 * takes one is called: the ring buffers are freed as they are.
 */
static void ior_threads_backend_forget(void *backend_ctx)
{
	ior_ctx_threads *ctx = backend_ctx;

	ior_threads_pool_forget(ctx->pool);
	ior_threads_event_destroy(&ctx->event);
	free(ctx->cq_ring.entries);
	free(ctx->sq_ring.entries);
	free(ctx);
}

static int ior_threads_backend_get_sqe(void *backend_ctx, ior_sqe **sqe_out)
{
	if (!backend_ctx) {
		return -EINVAL;
	}

	ior_ctx_threads *ctx = backend_ctx;

	/*
	 * Only the staging ring caps what may be handed out, as io_uring's SQ
	 * does: ops in flight are bounded by memory, and a completion that finds
	 * the CQ full waits on the overflow list (see ior_threads_pool_post).
	 */
	ior_sqe *sqe = ior_threads_ring_get_sqe(&ctx->sq_ring);
	if (!sqe) {
		return -ENOSPC;
	}
	*sqe_out = sqe;
	return 0;
}

static unsigned ior_threads_backend_sq_entries(void *backend_ctx)
{
	return ((ior_ctx_threads *) backend_ctx)->sq_ring.size;
}

static unsigned ior_threads_backend_cq_entries(void *backend_ctx)
{
	return ((ior_ctx_threads *) backend_ctx)->cq_ring.size;
}

static unsigned ior_threads_backend_sq_space_left(void *backend_ctx)
{
	return ior_threads_ring_sq_space_left(&((ior_ctx_threads *) backend_ctx)->sq_ring);
}

static unsigned ior_threads_backend_cq_space_left(void *backend_ctx)
{
	ior_ctx_threads *ctx = backend_ctx;
	uint32_t size = ctx->cq_ring.size;
	uint32_t ready = ior_threads_ring_count(&ctx->cq_ring);
	if (atomic_load_explicit(&ctx->pool->ovf_count, memory_order_acquire) || ready >= size) {
		return 0;
	}
	return size - ready;
}

static int ior_threads_backend_submit(void *backend_ctx)
{
	if (!backend_ctx) {
		return -EINVAL;
	}

	ior_ctx_threads *ctx = backend_ctx;

	// Number of staged (reserved-but-not-yet-dispatched) SQEs.
	uint32_t cached = atomic_load_explicit(&ctx->sq_ring.cached_tail, memory_order_acquire);
	uint32_t consumed = atomic_load_explicit(&ctx->sq_ring.consumed, memory_order_acquire);
	uint32_t count = cached - consumed;
	if (count == 0) {
		return 0;
	}

	// Copy staged SQEs into work items, dispatch them, and provision workers.
	return (int) ior_threads_pool_notify(ctx->pool);
}

// Completions ready for the consumer: in the ring or on the overflow list.
static uint32_t ior_threads_cq_ready(ior_ctx_threads *ctx)
{
	return ior_threads_ring_count(&ctx->cq_ring)
			+ atomic_load_explicit(&ctx->pool->ovf_count, memory_order_acquire);
}

// The next completion, moving overflowed ones into the ring if it is empty.
static ior_cqe *ior_threads_next_cqe(ior_ctx_threads *ctx)
{
	ior_cqe *cqe = ior_threads_ring_peek_cqe(&ctx->cq_ring);
	if (!cqe) {
		ior_threads_pool_flush_overflow(ctx->pool);
		cqe = ior_threads_ring_peek_cqe(&ctx->cq_ring);
	}
	return cqe;
}

/*
 * Wait for the completion event until at least `need` completions are in the
 * ring. A poster signals only while someone waits, once until the waiter it
 * woke has reset `signalled` (see ior_threads_pool_post_completion), so the
 * wait is announced first and the ring looked at after it, and the reset
 * comes before the caller looks at the ring again. A signal left over from
 * a wait that timed out keeps the event readable: the next wait consumes it
 * at once. Returns 1 when the completions are there without waiting, 0
 * after a wakeup, or the event wait's error (-ETIMEDOUT, -EINTR).
 */
static int ior_threads_wait_completions(ior_ctx_threads *ctx, uint32_t need, int timeout_ms)
{
	atomic_fetch_add_explicit(&ctx->waiters, 1, memory_order_seq_cst);
	int ret = 1;
	if (ior_threads_cq_ready(ctx) < need) {
		ret = ior_threads_event_wait(&ctx->event, timeout_ms);
		if (ret == 0) {
			ior_threads_event_clear(&ctx->event);
			atomic_store_explicit(&ctx->signalled, 0, memory_order_seq_cst);
		}
	}
	atomic_fetch_sub_explicit(&ctx->waiters, 1, memory_order_relaxed);
	return ret;
}

static int ior_threads_backend_submit_and_wait(void *backend_ctx, unsigned wait_nr)
{
	if (!backend_ctx) {
		return -EINVAL;
	}

	ior_ctx_threads *ctx = backend_ctx;

	IOR_LOG_TRACE("enter: wait_nr=%u, cq_count=%u", wait_nr, ior_threads_ring_count(&ctx->cq_ring));

	// Submit pending operations
	int submitted = ior_threads_backend_submit(backend_ctx);
	if (submitted < 0) {
		IOR_LOG_ERROR("submit failed: %d", submitted);
		return submitted;
	}

	// Wait for at least wait_nr completions, unless submit stopped at a bad
	// entry: io_uring then returns without waiting.
	if (wait_nr == 0 || ior_threads_ring_sq_space_left(&ctx->sq_ring) != ctx->sq_ring.size) {
		return submitted;
	}

	// Wait for completions to become available
	while (ior_threads_cq_ready(ctx) < wait_nr) {
		IOR_LOG_TRACE("event wait start");
		int ret = ior_threads_wait_completions(ctx, wait_nr, -1);
		IOR_LOG_TRACE("event wait done: ret=%d", ret);
		if (ret < 0) {
			return ret;
		}
	}

	return submitted;
}

static int ior_threads_backend_peek_cqe(void *backend_ctx, ior_cqe **cqe_out)
{
	if (!backend_ctx || !cqe_out) {
		return -EINVAL;
	}

	ior_ctx_threads *ctx = backend_ctx;

	ior_cqe *cqe = ior_threads_next_cqe(ctx);
	if (!cqe) {
		return -EAGAIN;
	}

	*cqe_out = cqe;
	return 0;
}

static int ior_threads_backend_wait_cqe(void *backend_ctx, ior_cqe **cqe_out)
{
	if (!backend_ctx || !cqe_out) {
		return -EINVAL;
	}

	ior_ctx_threads *ctx = backend_ctx;

	/*
	 * Block until a CQE is actually available. The event is only a wakeup hint
	 * (it may be stale, or the CQE may have been consumed already), so a wakeup
	 * with nothing in the ring is not an error - we re-check and wait again
	 * rather than returning -EAGAIN. event_wait(-1) sleeps in poll() until a
	 * producer signals, so this loop blocks rather than spins. The ring, not
	 * the event, is the source of truth, so the leading peek also covers any
	 * signal drained on a previous iteration.
	 */
	for (;;) {
		ior_cqe *cqe = ior_threads_next_cqe(ctx);
		if (cqe) {
			*cqe_out = cqe;
			return 0;
		}

		int ret = ior_threads_wait_completions(ctx, 1, -1);
		if (ret < 0) {
			return ret; // genuine error (e.g. -EINTR)
		}
	}
}

static int ior_threads_backend_wait_cqe_timeout(
		void *backend_ctx, ior_cqe **cqe_out, ior_timespec *timeout)
{
	if (!backend_ctx || !cqe_out) {
		return -EINVAL;
	}

	ior_ctx_threads *ctx = backend_ctx;

	// Turn the timeout into an absolute monotonic deadline so that spurious
	// wakeups can resume the wait without extending it. As on io_uring, a
	// negative one has already expired and a tv_nsec past a second adds up.
	int has_deadline = 0;
	uint64_t deadline_ns = 0;
	if (timeout) {
		struct timespec now;
		clock_gettime(CLOCK_MONOTONIC, &now);
		deadline_ns = (uint64_t) now.tv_sec * 1000000000ULL + (uint64_t) now.tv_nsec
				+ ior_timespec_ns(timeout);
		has_deadline = 1;
	}

	/*
	 * Same loop as ior_wait_cqe, but bounded by the deadline. A stale/spurious
	 * wakeup resumes the wait against the remaining time rather than returning;
	 * once the deadline passes we report -ETIME (matching io_uring), not -EAGAIN.
	 */
	for (;;) {
		ior_cqe *cqe = ior_threads_next_cqe(ctx);
		if (cqe) {
			*cqe_out = cqe;
			return 0;
		}

		int timeout_ms = -1;
		if (has_deadline) {
			struct timespec now;
			clock_gettime(CLOCK_MONOTONIC, &now);
			uint64_t now_ns = (uint64_t) now.tv_sec * 1000000000ULL + (uint64_t) now.tv_nsec;
			if (now_ns >= deadline_ns) {
				return -ETIME;
			}
			// Round remaining time up to whole milliseconds (poll's resolution),
			// clamped to int range.
			uint64_t rem_ms = (deadline_ns - now_ns + 999999ULL) / 1000000ULL;
			timeout_ms = rem_ms > (uint64_t) INT_MAX ? INT_MAX : (int) rem_ms;
		}

		int ret = ior_threads_wait_completions(ctx, 1, timeout_ms);
		if (ret == -ETIMEDOUT) {
			continue; // loop re-checks the ring, then the deadline -> -ETIME
		}
		if (ret < 0) {
			return ret; // genuine error (e.g. -EINTR)
		}
	}
}

static void ior_threads_backend_cqe_seen(void *backend_ctx, ior_cqe *cqe)
{
	if (!backend_ctx) {
		return;
	}

	ior_ctx_threads *ctx = backend_ctx;
	ior_threads_ring_cqe_seen(&ctx->cq_ring);
	// Room for a completion that found the ring full.
	ior_threads_pool_flush_overflow(ctx->pool);
}

static unsigned ior_threads_backend_peek_batch_cqe(void *backend_ctx, ior_cqe **cqes, unsigned max)
{
	if (!backend_ctx || !cqes || max == 0) {
		return 0;
	}

	ior_ctx_threads *ctx = backend_ctx;
	ior_threads_pool_flush_overflow(ctx->pool);
	return ior_threads_ring_peek_batch_cqe(&ctx->cq_ring, cqes, max);
}

static void ior_threads_backend_cq_advance(void *backend_ctx, unsigned nr)
{
	if (!backend_ctx || nr == 0) {
		return;
	}

	ior_ctx_threads *ctx = backend_ctx;
	ior_threads_ring_advance(&ctx->cq_ring, nr);
	ior_threads_pool_flush_overflow(ctx->pool);
}

/* SQE preparation helpers */

static void ior_threads_backend_prep_nop(ior_sqe *sqe)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_NOP;
	sqe->threads.fd = IOR_INVALID_FD;
}

static void ior_threads_backend_prep_read(
		ior_sqe *sqe, ior_fd_t fd, void *buf, unsigned nbytes, uint64_t offset)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_READ;
	sqe->threads.fd = fd;
	sqe->threads.addr = (uint64_t) (uintptr_t) buf;
	sqe->threads.len = nbytes;
	sqe->threads.off = offset;
}

static void ior_threads_backend_prep_write(
		ior_sqe *sqe, ior_fd_t fd, const void *buf, unsigned nbytes, uint64_t offset)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_WRITE;
	sqe->threads.fd = fd;
	sqe->threads.addr = (uint64_t) (uintptr_t) buf;
	sqe->threads.len = nbytes;
	sqe->threads.off = offset;
}

static void ior_threads_backend_prep_splice(ior_sqe *sqe, ior_fd_t fd_in, uint64_t off_in,
		ior_fd_t fd_out, uint64_t off_out, unsigned nbytes, unsigned flags)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_SPLICE;
	sqe->threads.fd = fd_out;
	sqe->threads.len = nbytes;
	sqe->threads.off = off_out;
	sqe->threads.splice_off_in = off_in;
	sqe->threads.splice_fd_in = fd_in;
	sqe->threads.splice_flags = flags;
}

static void ior_threads_backend_prep_timeout(
		ior_sqe *sqe, ior_timespec *ts, unsigned count, unsigned flags)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_TIMER;
	sqe->threads.fd = IOR_INVALID_FD;
	sqe->threads.addr = (uint64_t) (uintptr_t) ts;
	sqe->threads.len = 1;
	sqe->threads.off = count;
	sqe->threads.timeout_flags = flags;
}

static void ior_threads_backend_prep_link_timeout(ior_sqe *sqe, ior_timespec *ts, unsigned flags)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_LINK_TIMEOUT;
	sqe->threads.fd = IOR_INVALID_FD;
	sqe->threads.addr = (uint64_t) (uintptr_t) ts;
	sqe->threads.timeout_flags = flags;
}

static void ior_threads_backend_prep_send(
		ior_sqe *sqe, ior_fd_t sockfd, const void *buf, unsigned nbytes, int flags)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_SEND;
	sqe->threads.fd = sockfd;
	sqe->threads.addr = (uint64_t) (uintptr_t) buf;
	sqe->threads.len = nbytes;
	sqe->threads.rw_flags = (uint32_t) flags;
}

static void ior_threads_backend_prep_recv(
		ior_sqe *sqe, ior_fd_t sockfd, void *buf, unsigned nbytes, int flags)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_RECV;
	sqe->threads.fd = sockfd;
	sqe->threads.addr = (uint64_t) (uintptr_t) buf;
	sqe->threads.len = nbytes;
	sqe->threads.rw_flags = (uint32_t) flags;
}

static void ior_threads_backend_prep_poll_add(ior_sqe *sqe, ior_fd_t fd, uint32_t poll_mask)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_POLL;
	sqe->threads.fd = fd;
	sqe->threads.poll_events = poll_mask;
}

static void ior_threads_backend_prep_poll_multishot(ior_sqe *sqe, ior_fd_t fd, uint32_t poll_mask)
{
	ior_threads_backend_prep_poll_add(sqe, fd, poll_mask);
	sqe->threads.len = IOR_POLL_ADD_MULTI;
}

static void ior_threads_backend_prep_accept(
		ior_sqe *sqe, ior_fd_t fd, struct sockaddr *addr, socklen_t *addrlen, unsigned flags)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_ACCEPT;
	sqe->threads.fd = fd;
	sqe->threads.addr = (uint64_t) (uintptr_t) addr;
	sqe->threads.off = (uint64_t) (uintptr_t) addrlen;
	sqe->threads.rw_flags = flags;
}

static void ior_threads_backend_prep_accept_multishot(ior_sqe *sqe, ior_fd_t fd, unsigned flags)
{
	ior_threads_backend_prep_accept(sqe, fd, NULL, NULL, flags);
	sqe->threads.ioprio = IOR_ACCEPT_MULTISHOT;
}

static void ior_threads_backend_prep_connect(
		ior_sqe *sqe, ior_fd_t fd, const struct sockaddr *addr, socklen_t addrlen)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_CONNECT;
	sqe->threads.fd = fd;
	sqe->threads.addr = (uint64_t) (uintptr_t) addr;
	sqe->threads.off = addrlen;
}

static int ior_threads_backend_prep_waitpid(
		void *backend_ctx, ior_sqe *sqe, ior_pid_t pid, int *status, int options)
{
	(void) backend_ctx;
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_WAITPID;
	sqe->threads.fd = IOR_INVALID_FD;
	sqe->threads.addr = (uint64_t) (uintptr_t) status;
	sqe->threads.off = (uint64_t) (int64_t) pid;
	sqe->threads.len = (uint32_t) options;
	return 0;
}

static int ior_threads_backend_prep_sigwait(
		void *backend_ctx, ior_sqe *sqe, const ior_sigset_t *set, ior_siginfo_t *info)
{
	(void) backend_ctx;
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_SIGWAIT;
	sqe->threads.fd = IOR_INVALID_FD;
	sqe->threads.addr = (uint64_t) (uintptr_t) set;
	sqe->threads.off = (uint64_t) (uintptr_t) info;
	return 0;
}

static void ior_threads_backend_prep_cancel(ior_sqe *sqe, uint64_t user_data)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_ASYNC_CANCEL;
	sqe->threads.fd = IOR_INVALID_FD;
	sqe->threads.addr = user_data;
}

static void ior_threads_backend_prep_cancel_fd(ior_sqe *sqe, ior_fd_t fd)
{
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_ASYNC_CANCEL;
	sqe->threads.fd = fd;
	sqe->threads.cancel_flags = IOR_CANCEL_BY_FD;
}

static int ior_threads_backend_prep_work(void *backend_ctx, ior_sqe *sqe, ior_work_fn fn, void *arg)
{
	(void) backend_ctx;
	memset(sqe, 0, sizeof(*sqe));
	sqe->threads.opcode = IOR_OP_WORK;
	sqe->threads.fd = IOR_INVALID_FD;
	sqe->threads.addr = (uint64_t) (uintptr_t) fn;
	sqe->threads.off = (uint64_t) (uintptr_t) arg;
	return 0;
}

static void ior_threads_backend_sqe_set_data(ior_sqe *sqe, void *data)
{
	sqe->threads.user_data = (uint64_t) (uintptr_t) data;
}

static void ior_threads_backend_sqe_set_flags(ior_sqe *sqe, uint8_t flags)
{
	sqe->threads.flags = flags;
}

/* CQE accessors */

static void *ior_threads_backend_cqe_get_data(ior_cqe *cqe)
{
	return (void *) (uintptr_t) cqe->threads.user_data;
}

static int32_t ior_threads_backend_cqe_get_res(ior_cqe *cqe)
{
	return cqe->threads.res;
}

static uint32_t ior_threads_backend_cqe_get_flags(ior_cqe *cqe)
{
	return cqe->threads.flags;
}

/* Completion notification: the completion event, signalled for every
 * completion once handed out. */

static ior_fd_t ior_threads_backend_notify_fd(void *backend_ctx)
{
	if (!backend_ctx) {
		return IOR_INVALID_FD;
	}
	ior_ctx_threads *ctx = backend_ctx;
	/* Handed out: from now on every completion signals it, whether or not a
	 * thread waits in ior. Those already pending are announced here, as
	 * ior_notify_fd() promises: armed first and the ring looked at after it,
	 * so a completion posted meanwhile is signalled by one side or both. */
	if (!atomic_exchange_explicit(&ctx->notify_armed, 1, memory_order_seq_cst)
			&& ior_threads_cq_ready(ctx) > 0) {
		ior_threads_event_signal(&ctx->event);
	}
	return ior_threads_event_get_fd(&ctx->event);
}

static int ior_threads_backend_notify_clear(void *backend_ctx)
{
	if (!backend_ctx) {
		return -EINVAL;
	}
	ior_ctx_threads *ctx = backend_ctx;
	/* Not handed out yet: the event is ior's own wakeup, and consuming it
	 * could take a signal a waiting thread was sent. */
	if (!atomic_load_explicit(&ctx->notify_armed, memory_order_acquire)) {
		return -EINVAL;
	}
	int ret = ior_threads_event_clear(&ctx->event);
	return ret < 0 ? ret : 0;
}

/* Backend info */

static const char *ior_threads_backend_name(void)
{
	return "threads";
}

static uint32_t ior_threads_backend_get_features(void *backend_ctx)
{
	if (!backend_ctx) {
		return 0;
	}

	ior_ctx_threads *ctx = backend_ctx;
	return ctx->features;
}

/* Export vtable */
const ior_backend_ops ior_threads_ops = {
	.init = ior_threads_backend_init,
	.destroy = ior_threads_backend_destroy,
	.forget = ior_threads_backend_forget,
	.get_sqe = ior_threads_backend_get_sqe,
	.submit = ior_threads_backend_submit,
	.submit_and_wait = ior_threads_backend_submit_and_wait,
	.peek_cqe = ior_threads_backend_peek_cqe,
	.wait_cqe = ior_threads_backend_wait_cqe,
	.wait_cqe_timeout = ior_threads_backend_wait_cqe_timeout,
	.cqe_seen = ior_threads_backend_cqe_seen,
	.peek_batch_cqe = ior_threads_backend_peek_batch_cqe,
	.cq_advance = ior_threads_backend_cq_advance,
	.prep_nop = ior_threads_backend_prep_nop,
	.prep_read = ior_threads_backend_prep_read,
	.prep_write = ior_threads_backend_prep_write,
	.prep_splice = ior_threads_backend_prep_splice,
	.prep_timeout = ior_threads_backend_prep_timeout,
	.prep_link_timeout = ior_threads_backend_prep_link_timeout,
	.prep_send = ior_threads_backend_prep_send,
	.prep_recv = ior_threads_backend_prep_recv,
	.prep_poll_add = ior_threads_backend_prep_poll_add,
	.prep_poll_multishot = ior_threads_backend_prep_poll_multishot,
	.prep_accept = ior_threads_backend_prep_accept,
	.prep_accept_multishot = ior_threads_backend_prep_accept_multishot,
	.prep_connect = ior_threads_backend_prep_connect,
	.prep_cancel = ior_threads_backend_prep_cancel,
	.prep_cancel_fd = ior_threads_backend_prep_cancel_fd,
	.prep_waitpid = ior_threads_backend_prep_waitpid,
	.prep_sigwait = ior_threads_backend_prep_sigwait,
	.prep_work = ior_threads_backend_prep_work,
	.sqe_set_data = ior_threads_backend_sqe_set_data,
	.sqe_set_flags = ior_threads_backend_sqe_set_flags,
	.cqe_get_data = ior_threads_backend_cqe_get_data,
	.cqe_get_res = ior_threads_backend_cqe_get_res,
	.cqe_get_flags = ior_threads_backend_cqe_get_flags,
	.notify_fd = ior_threads_backend_notify_fd,
	.notify_clear = ior_threads_backend_notify_clear,
	.backend_name = ior_threads_backend_name,
	.get_features = ior_threads_backend_get_features,
	.sq_entries = ior_threads_backend_sq_entries,
	.cq_entries = ior_threads_backend_cq_entries,
	.sq_space_left = ior_threads_backend_sq_space_left,
	.cq_space_left = ior_threads_backend_cq_space_left,
};

#endif /* IOR_HAVE_THREADS */
