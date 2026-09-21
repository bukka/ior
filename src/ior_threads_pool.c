/* SPDX-License-Identifier: BSD-3-Clause */
#include "config.h"
#if defined(IOR_HAVE_SPLICE) || defined(IOR_HAVE_ACCEPT4)
#define _GNU_SOURCE
#include <fcntl.h>
#include <unistd.h>
#endif
#include "ior_backend.h"
#include "ior_threads_pool.h"
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <time.h>
#include <poll.h>
#include <limits.h>
#ifdef IOR_HAVE_PIDFD_OPEN
#include <sys/syscall.h>
#endif

#ifndef __linux__
typedef off_t loff_t;
#endif

// Forward declarations
static void ior_threads_pool_run_job(void *owner, ior_worker_pool_job *job);
static void ior_threads_pool_process_chain(ior_threads_pool *pool, ior_work *head);
static void ior_threads_pool_process_single_sqe(
		ior_threads_pool *pool, ior_work *w, ior_cqe *cqe, ior_work_token *token);
static void ior_threads_pool_post_completion(ior_threads_pool *pool, const ior_cqe *cqe);
static void ior_threads_pool_arm_timer(ior_threads_pool *pool, ior_work *work);
static int ior_threads_pool_timer_valid(const ior_work *work);
static void ior_threads_pool_finish_op(ior_threads_pool *pool, ior_work *work, const ior_cqe *cqe);
static void ior_threads_pool_finish_res(ior_threads_pool *pool, ior_work *work, int32_t res);
static int ior_threads_pool_cancel(ior_threads_pool *pool, ior_work *self);
#ifndef IOR_HAVE_SPLICE
static ssize_t ior_threads_pool_emulate_splice(
		int fd_in, loff_t *off_in, int fd_out, loff_t *off_out, size_t len, unsigned int flags);
#endif

// Round up to a power of two (>= 1).
static uint32_t ior_threads_pool_round_up_pow2(uint32_t n)
{
	if (n < 2) {
		return 1;
	}
	n--;
	n |= n >> 1;
	n |= n >> 2;
	n |= n >> 4;
	n |= n >> 8;
	n |= n >> 16;
	return n + 1;
}

// ===== Work-item pool and drain tracking =====
// The work-pool helpers below must be called with work_lock held; the drain
// helpers take drain_lock themselves. Dispatch itself (FIFO + worker wakeup)
// lives in the shared ior_worker_pool.

static ior_work *ior_threads_pool_work_alloc(ior_threads_pool *pool)
{
	ior_work *w = pool->work_free;
	pool->work_free = w->next;
	return w;
}

static void ior_threads_pool_work_release(ior_threads_pool *pool, ior_work *w)
{
	atomic_store_explicit(&w->state, IOR_WORK_FREE, memory_order_release);
	w->next = pool->work_free;
	pool->work_free = w;
}

/*
 * Move w to `state` unless a cancel has claimed it, in which case it must
 * complete with -ECANCELED instead (returns -ECANCELED).
 */
static int ior_threads_pool_enter(ior_work *w, int state)
{
	int expected = atomic_load_explicit(&w->state, memory_order_acquire);
	for (;;) {
		if (expected == IOR_WORK_CANCELLED) {
			return -ECANCELED;
		}
		if (atomic_compare_exchange_weak(&w->state, &expected, state)) {
			return 0;
		}
	}
}

// Mark a submission sequence completed and advance the contiguous drain front,
// waking any IO_DRAIN op waiting for earlier ops to finish.
static void ior_threads_pool_drain_complete(ior_threads_pool *pool, uint64_t seq)
{
	pthread_mutex_lock(&pool->drain_lock);
	pool->drain_done[seq & pool->drain_mask] = 1;
	while (pool->drain_done[pool->drain_upto & pool->drain_mask]) {
		pool->drain_done[pool->drain_upto & pool->drain_mask] = 0;
		pool->drain_upto++;
	}
	pthread_cond_broadcast(&pool->drain_cond);
	pthread_mutex_unlock(&pool->drain_lock);
}

// Block until every op submitted before w has completed (for IO_DRAIN), or
// until a cancel claims w (-ECANCELED).
static int ior_threads_pool_drain_wait(ior_threads_pool *pool, ior_work *w)
{
	if (ior_threads_pool_enter(w, IOR_WORK_DRAINING) < 0) {
		return -ECANCELED;
	}
	pthread_mutex_lock(&pool->drain_lock);
	while (pool->drain_upto < w->seq
			&& atomic_load_explicit(&w->state, memory_order_acquire) != IOR_WORK_CANCELLED) {
		pthread_cond_wait(&pool->drain_cond, &pool->drain_lock);
	}
	pthread_mutex_unlock(&pool->drain_lock);
	return atomic_load_explicit(&w->state, memory_order_acquire) == IOR_WORK_CANCELLED ? -ECANCELED
																					   : 0;
}

/*
 * Retire one operation: post its completion, record it for drain ordering,
 * return its work item to the pool, and drop the in-flight/outstanding counts
 * so get_sqe and worker provisioning see the freed capacity. The item is
 * marked DONE before the CQE is posted so that a cancel submitted by a
 * consumer who has seen the CQE finds nothing in flight (-ENOENT) rather
 * than a stale state.
 */
static void ior_threads_pool_finish_op(ior_threads_pool *pool, ior_work *work, const ior_cqe *cqe)
{
	atomic_store_explicit(&work->state, IOR_WORK_DONE, memory_order_release);

	ior_threads_pool_post_completion(pool, cqe);
	ior_threads_pool_drain_complete(pool, work->seq);

	pthread_mutex_lock(&pool->work_lock);
	ior_threads_pool_work_release(pool, work);
	pthread_mutex_unlock(&pool->work_lock);

	atomic_fetch_sub(&pool->outstanding, 1);
	atomic_fetch_sub(&pool->num_inflight, 1);
}

static void ior_threads_pool_finish_res(ior_threads_pool *pool, ior_work *work, int32_t res)
{
	ior_cqe cqe;
	memset(&cqe, 0, sizeof(cqe));
	cqe.threads.user_data = work->sqe.threads.user_data;
	cqe.threads.res = res;
	ior_threads_pool_finish_op(pool, work, &cqe);
}

/*
 * Hand a chain (head plus everything behind it) to the worker pool. Fails with
 * -ECANCELED once destroy has begun: the pool is going away, so the caller
 * completes the chain as cancelled instead. work_lock must be held.
 */
static int ior_threads_pool_dispatch_locked(ior_threads_pool *pool, ior_work *head)
{
	if (atomic_load_explicit(&pool->shutdown, memory_order_acquire)) {
		return -ECANCELED;
	}
	atomic_store_explicit(&head->state, IOR_WORK_QUEUED, memory_order_release);
	head->job.next = NULL;
	ior_worker_pool_submit(pool->wp, &head->job, &head->job, 1);
	return 0;
}

// Complete every op of a chain with -ECANCELED; returns how many.
static uint64_t ior_threads_pool_cancel_chain(ior_threads_pool *pool, ior_work *w)
{
	uint64_t count = 0;
	while (w) {
		ior_work *next = w->chain;
		ior_threads_pool_finish_res(pool, w, -ECANCELED);
		count++;
		w = next;
	}
	return count;
}

/*
 * Resolve an op handed off to the poller thread: a poll op, or a
 * read/write/send/recv gated on readiness. The chain layout is recovered from
 * the work item itself: an immediately following LINK_TIMEOUT is the guarding
 * pair, anything after belongs to the chain remainder, which resumes on the
 * worker pool on success or is cancelled on failure. A ready rw op resumes on
 * a worker as a whole (its syscall runs there); a ready poll op completes here.
 */
static void ior_threads_pool_poll_done(void *owner, void *req, int res)
{
	ior_threads_pool *pool = owner;
	ior_work *w = req;
	uint64_t count = 1;

	// Claim the op back from the poller. A cancel that raced the poller's
	// dispatch has promised -ECANCELED; honour it whatever the poller saw,
	// deadline included (the link timeout is then cancelled too).
	int prev = atomic_exchange(&w->state, IOR_WORK_RUNNING);
	if (prev == IOR_WORK_CANCELLED) {
		res = -ECANCELED;
	}

	// The poller has dropped its registration: the pidfd has served.
	if (w->pidfd >= 0) {
		close(w->pidfd);
		w->pidfd = -1;
	}

	ior_work *lt = NULL;
	if ((w->sqe.threads.flags & IOR_SQE_IO_LINK) && w->chain
			&& w->chain->sqe.threads.opcode == IOR_OP_LINK_TIMEOUT) {
		lt = w->chain;
	}
	ior_work *rest = lt ? lt->chain : w->chain;

	/*
	 * Ready ops resume on a worker. So does a process wait whose watch
	 * failed (ESRCH for a child that exited meanwhile, say): the worker
	 * collects the state, or waits for it itself.
	 */
	int resume = res > 0
			|| (w->sqe.threads.opcode == IOR_OP_WAITPID && res != -ECANCELED && res != -ETIME);
	if (resume && w->sqe.threads.opcode != IOR_OP_POLL) {
		pthread_mutex_lock(&pool->work_lock);
		w->ready = 1;
		int ret = ior_threads_pool_dispatch_locked(pool, w);
		pthread_mutex_unlock(&pool->work_lock);
		if (ret == 0) {
			return; // the worker owns w and its whole chain again
		}
		res = -ECANCELED;
	}

	int failed = res < 0;
	// A poller deadline is a fired link timeout: the guarded op is cancelled.
	ior_threads_pool_finish_res(pool, w, (res == -ETIME) ? -ECANCELED : res);

	if (lt) {
		ior_threads_pool_finish_res(pool, lt, (res == -ETIME) ? -ETIME : -ECANCELED);
		count++;
	}

	if (rest) {
		int ret = -ECANCELED;
		if (!failed) {
			pthread_mutex_lock(&pool->work_lock);
			ret = ior_threads_pool_dispatch_locked(pool, rest);
			pthread_mutex_unlock(&pool->work_lock);
		}
		if (ret < 0) {
			// A failed linked op cancels the remainder, matching io_uring.
			count += ior_threads_pool_cancel_chain(pool, rest);
		}
	}

	atomic_fetch_add(&pool->tasks_completed, count);
}

/* Get or lazily create the shared poller (NULL on allocation failure). */
static ior_threads_poller *ior_threads_pool_get_poller(ior_threads_pool *pool)
{
	ior_threads_poller *poller = atomic_load_explicit(&pool->poller, memory_order_acquire);
	if (poller) {
		return poller;
	}

	pthread_mutex_lock(&pool->work_lock);
	poller = atomic_load_explicit(&pool->poller, memory_order_relaxed);
	if (!poller) {
		if (ior_threads_poller_create(&poller, pool, ior_threads_pool_poll_done) < 0) {
			poller = NULL;
		} else {
			atomic_store_explicit(&pool->poller, poller, memory_order_release);
		}
	}
	pthread_mutex_unlock(&pool->work_lock);
	return poller;
}

ior_threads_pool *ior_threads_pool_create(ior_ctx_threads *ctx, uint32_t num_threads)
{
	ior_threads_pool_config config = {
		.min_threads = 0,
		.max_threads = num_threads > 0 ? num_threads : 32,
		.stack_size = 0,
		.thread_priority = 0,
	};

	return ior_threads_pool_create_ex(ctx, &config);
}

ior_threads_pool *ior_threads_pool_create_ex(
		ior_ctx_threads *ctx, const ior_threads_pool_config *config)
{
	if (!ctx || !config) {
		return NULL;
	}

	ior_threads_pool *pool = calloc(1, sizeof(*pool));
	if (!pool) {
		return NULL;
	}

	pool->ctx = ctx;
	atomic_init(&pool->tasks_completed, 0);
	atomic_init(&pool->poller, NULL);
	atomic_init(&pool->shutdown, 0);

	/*
	 * Work-item pool (free-at-submit). Capacity matches the CQ (the in-flight
	 * bound). The drain bitmap is sized past the worst-case in-flight seq span
	 * so sequence numbers never alias.
	 */
	pool->work_cap = ctx->cq_ring.size;
	pool->next_seq = 0;
	atomic_init(&pool->outstanding, 0);
	atomic_init(&pool->num_inflight, 0);
	pool->drain_upto = 0;

	if (pthread_mutex_init(&pool->work_lock, NULL) != 0) {
		free(pool);
		return NULL;
	}
	if (pthread_mutex_init(&pool->arm_lock, NULL) != 0) {
		pthread_mutex_destroy(&pool->work_lock);
		free(pool);
		return NULL;
	}

	pool->work_items = calloc(pool->work_cap, sizeof(*pool->work_items));
	uint32_t drain_cap = ior_threads_pool_round_up_pow2(pool->work_cap * 2);
	pool->drain_mask = drain_cap - 1;
	pool->drain_done = calloc(drain_cap, sizeof(*pool->drain_done));
	if (!pool->work_items || !pool->drain_done
			|| pthread_mutex_init(&pool->drain_lock, NULL) != 0) {
		free(pool->drain_done);
		free(pool->work_items);
		pthread_mutex_destroy(&pool->arm_lock);
		pthread_mutex_destroy(&pool->work_lock);
		free(pool);
		return NULL;
	}
	if (pthread_cond_init(&pool->drain_cond, NULL) != 0) {
		pthread_mutex_destroy(&pool->drain_lock);
		free(pool->drain_done);
		free(pool->work_items);
		pthread_mutex_destroy(&pool->arm_lock);
		pthread_mutex_destroy(&pool->work_lock);
		free(pool);
		return NULL;
	}
	for (uint32_t i = 0; i < pool->work_cap; i++) {
		atomic_init(&pool->work_items[i].state, IOR_WORK_FREE);
		pool->work_items[i].next = pool->work_free;
		pool->work_free = &pool->work_items[i];
	}

	// Worker lifecycle, dispatch FIFO and timers live in the shared pool.
	ior_worker_pool_config wp_config = {
		.min_threads = config->min_threads,
		.max_threads = config->max_threads > 0 ? config->max_threads : 32,
		.stack_size = config->stack_size,
	};
	pool->wp = ior_worker_pool_create(&wp_config, ior_threads_pool_run_job, pool);
	if (!pool->wp) {
		pthread_cond_destroy(&pool->drain_cond);
		pthread_mutex_destroy(&pool->drain_lock);
		free(pool->drain_done);
		free(pool->work_items);
		pthread_mutex_destroy(&pool->arm_lock);
		pthread_mutex_destroy(&pool->work_lock);
		free(pool);
		return NULL;
	}

	return pool;
}

void ior_threads_pool_notify(ior_threads_pool *pool, uint32_t count)
{
	(void) count;
	if (!pool) {
		return;
	}

	ior_ctx_threads *ctx = pool->ctx;

	pthread_mutex_lock(&pool->work_lock);

	/*
	 * Copy every newly staged SQE out of the ring into a work item, freeing the
	 * SQ slots immediately. Consecutive IO_LINK ops form one chain: only the
	 * head is enqueued, the rest hang off head->chain, so a worker drains a
	 * chain as a unit (no mid-chain race). Chain heads are collected into a
	 * local list here and handed to the worker pool in one submit below.
	 *
	 * A standalone cancel (no link or drain involvement) runs on this thread
	 * once the batch is dispatched, like io_uring executes it at submit: it
	 * then sees the ops submitted just before it.
	 */
	uint32_t consumed = atomic_load_explicit(&ctx->sq_ring.consumed, memory_order_relaxed);
	uint32_t cached = atomic_load_explicit(&ctx->sq_ring.cached_tail, memory_order_acquire);
	uint32_t n = cached - consumed;
	const ior_sqe *sqes = (const ior_sqe *) ctx->sq_ring.entries;

	ior_worker_pool_job *first = NULL;
	ior_worker_pool_job *last = NULL;
	uint32_t njobs = 0;
	ior_work *prev = NULL;
	int prev_link = 0;
	ior_work *cancels = NULL;
	ior_work *cancels_tail = NULL;
	for (uint32_t p = consumed; p != cached; p++) {
		ior_work *w = ior_threads_pool_work_alloc(pool);
		w->sqe = sqes[p & ctx->sq_ring.mask];
		w->seq = pool->next_seq++;
		w->chain = NULL;
		w->cur_token = NULL;
		w->deadline_ns = 0;
		w->ready = 0;
		w->connecting = 0;
		w->pidfd = -1;
		if (w->sqe.threads.opcode == IOR_OP_WORK) {
			atomic_init(&w->token.cancelled, 0);
			w->token.shutdown = &pool->wp->shutdown;
		}
		uint8_t flags = w->sqe.threads.flags;
		int has_link = (flags & IOR_SQE_IO_LINK) != 0;
		if (prev_link) {
			atomic_store_explicit(&w->state, IOR_WORK_LINKED, memory_order_release);
			prev->chain = w;
		} else if (w->sqe.threads.opcode == IOR_OP_ASYNC_CANCEL
				&& !(flags & (IOR_SQE_IO_LINK | IOR_SQE_IO_DRAIN))) {
			atomic_store_explicit(&w->state, IOR_WORK_RUNNING, memory_order_release);
			w->next = NULL;
			if (cancels_tail) {
				cancels_tail->next = w;
			} else {
				cancels = w;
			}
			cancels_tail = w;
		} else {
			atomic_store_explicit(&w->state, IOR_WORK_QUEUED, memory_order_release);
			w->job.next = NULL;
			if (last) {
				last->next = &w->job;
			} else {
				first = &w->job;
			}
			last = &w->job;
			njobs++;
		}
		prev = w;
		prev_link = has_link;
	}

	atomic_fetch_add(&pool->num_inflight, n);

	pthread_mutex_unlock(&pool->work_lock);

	if (njobs > 0) {
		ior_worker_pool_submit(pool->wp, first, last, njobs);
	}

	// Staging slots are now free for reuse by get_sqe.
	ior_threads_ring_consume(&ctx->sq_ring);

	uint64_t done = 0;
	while (cancels) {
		ior_work *c = cancels;
		cancels = c->next;
		ior_threads_pool_finish_res(pool, c, ior_threads_pool_cancel(pool, c));
		done++;
	}
	if (done) {
		atomic_fetch_add(&pool->tasks_completed, done);
	}
}

void ior_threads_pool_destroy(ior_threads_pool *pool)
{
	if (!pool) {
		return;
	}

	// From here on the poller fails ops instead of handing them back to the
	// worker pool, which is about to go away.
	pthread_mutex_lock(&pool->work_lock);
	atomic_store_explicit(&pool->shutdown, 1, memory_order_release);
	pthread_mutex_unlock(&pool->work_lock);

	// Shut down the shared pool: drains dispatched chains, drops pending
	// timers without posting completions, and joins all threads.
	ior_worker_pool_destroy(pool->wp);

	// Workers are joined, so no new poll registrations can arrive; pending
	// polls complete with -ECANCELED before the poller thread exits.
	ior_threads_poller_destroy(atomic_load(&pool->poller));

	// Cleanup
	pthread_cond_destroy(&pool->drain_cond);
	pthread_mutex_destroy(&pool->drain_lock);
	free(pool->drain_done);
	free(pool->work_items);
	pthread_mutex_destroy(&pool->arm_lock);
	pthread_mutex_destroy(&pool->work_lock);
	free(pool);
}

uint32_t ior_threads_pool_get_num_threads(ior_threads_pool *pool)
{
	if (!pool) {
		return 0;
	}

	return ior_worker_pool_num_threads(pool->wp);
}

void ior_threads_pool_get_stats(ior_threads_pool *pool, ior_threads_pool_stats *stats)
{
	if (!pool || !stats) {
		return;
	}

	memset(stats, 0, sizeof(*stats));

	ior_worker_pool_thread_stats(pool->wp, &stats->threads_active, &stats->threads_idle);

	stats->tasks_completed = atomic_load(&pool->tasks_completed);
	stats->tasks_pending = ior_threads_ring_count(&pool->ctx->sq_ring);
}

// ===== Worker-pool job trampoline =====

static void ior_threads_pool_run_job(void *owner, ior_worker_pool_job *job)
{
	ior_threads_pool *pool = owner;
	ior_work *head = (ior_work *) ((char *) job - offsetof(ior_work, job));

	ior_threads_pool_process_chain(pool, head);
}

// ===== Operation Processing =====

// poll(2) events an rw op waits for before its syscall, 0 for other opcodes.
static short ior_threads_pool_rw_events(const ior_sqe *sqe)
{
	switch (sqe->threads.opcode) {
		case IOR_OP_READ:
		case IOR_OP_RECV:
		case IOR_OP_ACCEPT:
			return POLLIN;
		case IOR_OP_WRITE:
		case IOR_OP_SEND:
		case IOR_OP_CONNECT:
			return POLLOUT;
		default:
			return 0;
	}
}

// MSG_DONTWAIT asks for -EAGAIN rather than a wait, as with io_uring.
static int ior_threads_pool_rw_nowait(const ior_sqe *sqe)
{
	return (sqe->threads.opcode == IOR_OP_SEND || sqe->threads.opcode == IOR_OP_RECV)
			&& (sqe->threads.rw_flags & MSG_DONTWAIT);
}

/*
 * XNU's sosend() decides on SS_NBIO (the O_NONBLOCK descriptor flag) alone, so
 * send(2) blocks on a full send buffer however it is asked not to. Its
 * soreceive() does honour MSG_DONTWAIT, so recv is unaffected.
 */
#ifdef IOR_PLATFORM_MACOS
#define IOR_SEND_HONOURS_DONTWAIT 0
#else
#define IOR_SEND_HONOURS_DONTWAIT 1
#endif

/*
 * Does the op need the descriptor put in non-blocking mode before its syscall?
 * Only one that cannot ask the kernel for a non-blocking attempt of its own: a
 * read or write at the current position (read(2) has no per-call non-blocking
 * flag and the descriptor may be a blocking socket or pipe), and send where
 * MSG_DONTWAIT is ignored. Elsewhere send/recv are issued with that flag
 * instead (one syscall, no descriptor state), and positioned I/O is
 * regular-file semantics that runs to completion, as on io_uring's worker
 * queue.
 */
static int ior_threads_pool_rw_needs_nonblock(const ior_sqe *sqe)
{
	if (!IOR_SEND_HONOURS_DONTWAIT && sqe->threads.opcode == IOR_OP_SEND) {
		return 1;
	}
	// accept(2) and connect(2) have no per-call non-blocking flag either.
	if (sqe->threads.opcode == IOR_OP_ACCEPT || sqe->threads.opcode == IOR_OP_CONNECT) {
		return 1;
	}
	return (sqe->threads.opcode == IOR_OP_READ || sqe->threads.opcode == IOR_OP_WRITE)
			&& sqe->threads.off == IOR_OFF_NONE;
}

/*
 * Take ownership of a descriptor's blocking mode, so its syscall reports
 * -EAGAIN instead of waiting. One ioctl, where the alternative costs a poll().
 * A probe cannot replace this for writes: poll() promises only SO_SNDLOWAT
 * bytes of room, while a blocking write does not return until all of len is
 * queued, so a write larger than the free space parks the worker however
 * ready the descriptor looked. Skipped entirely when the caller has declared
 * its descriptors non-blocking with IOR_SETUP_FD_NONBLOCK. Returns 0 if the
 * descriptor is non-blocking.
 */
static int ior_threads_pool_set_nonblock(int fd)
{
	int on = 1;
	return ioctl(fd, FIONBIO, &on) < 0 ? -1 : 0;
}

static int ior_threads_pool_res_would_block(uint8_t opcode, int32_t res)
{
	if (opcode == IOR_OP_CONNECT) {
		// The connection proceeds in the background: wait for writability.
		return res == -EINPROGRESS || res == -EALREADY;
	}
	return res == -EAGAIN || res == -EWOULDBLOCK;
}

/*
 * Would the syscall proceed without blocking? Errors and POLLNVAL count as
 * ready: the syscall reports them. Regular files are always ready. Only used
 * where the blocking mode could not be taken over.
 */
static int ior_threads_pool_fd_ready(int fd, short events)
{
	struct pollfd pfd = { .fd = fd, .events = events };
	int pret;
	do {
		pret = poll(&pfd, 1, 0);
	} while (pret < 0 && errno == EINTR);
	return pret != 0;
}

// Absolute deadline of a link timeout, 0 for none (NULL timespec).
static uint64_t ior_threads_pool_lt_deadline(const ior_work *lt)
{
	ior_timespec *ts = (ior_timespec *) (uintptr_t) lt->sqe.threads.addr;
	if (!ts) {
		return 0;
	}
	uint64_t ns = (uint64_t) ts->tv_sec * 1000000000ULL + (uint64_t) ts->tv_nsec;
	if (!(lt->sqe.threads.timeout_flags & IOR_TIMEOUT_ABS)) {
		ns += ior_worker_pool_monotonic_ns();
	}
	return ns ? ns : 1;
}

/*
 * Park w (with its guarding link timeout and chain remainder) on the poller
 * thread, which resumes or resolves it in ior_threads_pool_poll_done. The
 * registration happens under work_lock so that a cancel racing it either
 * finds the registration or, having claimed the op first, is honoured here.
 * Returns 0 once the poller owns the chain, else a negative errno and the
 * caller still owns it.
 */
static int ior_threads_pool_hand_to_poller(
		ior_threads_pool *pool, ior_work *w, ior_work *lt, int fd, uint32_t mask)
{
	if (lt && !w->deadline_ns) {
		w->deadline_ns = ior_threads_pool_lt_deadline(lt);
	}
	ior_threads_poller *poller = ior_threads_pool_get_poller(pool);
	if (!poller) {
		return -ENOMEM;
	}

	pthread_mutex_lock(&pool->work_lock);
	int ret = ior_threads_pool_enter(w, IOR_WORK_POLLING);
	if (ret == 0) {
		w->ready = 0;
		ret = ior_threads_poller_add(poller, fd, mask, w->deadline_ns, w);
		if (ret < 0) {
			// Still ours, and about to fail: not claimable any more.
			atomic_store_explicit(&w->state, IOR_WORK_RUNNING, memory_order_release);
		}
	}
	pthread_mutex_unlock(&pool->work_lock);
	return ret;
}

/*
 * Arbitration node for a work op guarded by a link timeout. Heap-allocated and
 * shared between the worker running the callback and the timer thread: `state`
 * decides how the link timeout resolves, the embedded token lets the callback
 * observe a fired deadline, and the refcount (worker + timer) keeps the node
 * alive until whichever side finishes last - the timer always fires or is
 * dropped eventually, even if the pair completed long before.
 */
typedef struct ior_threads_pool_lt_arb {
	struct ior_work_token token;
	_Atomic int state; /* 0 = armed, 1 = callback finished first, 2 = timer fired first */
	_Atomic int refs;
} ior_threads_pool_lt_arb;

static void ior_threads_pool_lt_arb_release(ior_threads_pool_lt_arb *arb)
{
	if (atomic_fetch_sub(&arb->refs, 1) == 1) {
		free(arb);
	}
}

// Timer-thread side: flag the token and claim the "fired first" outcome. Late
// firings (callback already resolved the pair) only touch the private node.
static void ior_threads_pool_lt_fired(void *owner, void *arg)
{
	(void) owner;
	ior_threads_pool_lt_arb *arb = arg;

	atomic_store_explicit(&arb->token.cancelled, 1, memory_order_release);
	int expected = 0;
	atomic_compare_exchange_strong(&arb->state, &expected, 2);

	ior_threads_pool_lt_arb_release(arb);
}

// Pool destroyed before the deadline: just drop the timer's reference.
static void ior_threads_pool_lt_dropped(void *owner, void *arg)
{
	(void) owner;
	ior_threads_pool_lt_arb_release(arg);
}

/*
 * Arm the deadline of a link timeout guarding a work op. The callback cannot
 * be poll-gated or killed, so the deadline only flags the token so it can
 * return early; returns the arbitration node (this worker's ref), or NULL
 * when there is no valid deadline (NULL/invalid ts, alloc failure), in which
 * case the pair degrades to "op finished first", like an unbounded poll gate.
 */
static ior_threads_pool_lt_arb *ior_threads_pool_lt_arb_arm(ior_threads_pool *pool, ior_work *lt)
{
	ior_timespec *ts = (ior_timespec *) (uintptr_t) lt->sqe.threads.addr;
	if (!ts || ts->tv_sec < 0 || ts->tv_nsec < 0 || ts->tv_nsec >= 1000000000L) {
		return NULL;
	}

	ior_threads_pool_lt_arb *arb = calloc(1, sizeof(*arb));
	if (!arb) {
		return NULL;
	}
	atomic_init(&arb->token.cancelled, 0);
	arb->token.shutdown = &pool->wp->shutdown;
	atomic_init(&arb->state, 0);
	atomic_init(&arb->refs, 2); // this worker + the timer thread

	uint64_t ts_ns = (uint64_t) ts->tv_sec * 1000000000ULL + (uint64_t) ts->tv_nsec;
	uint64_t deadline_ns = (lt->sqe.threads.timeout_flags & IOR_TIMEOUT_ABS)
			? ts_ns
			: ior_worker_pool_monotonic_ns() + ts_ns;

	if (ior_worker_pool_arm_timer(
				pool->wp, deadline_ns, ior_threads_pool_lt_fired, ior_threads_pool_lt_dropped, arb)
			< 0) {
		free(arb);
		return NULL;
	}
	return arb;
}

/*
 * Park a WAITPID op until pid exits, where the platform can watch a process
 * without a thread: a pidfd on Linux, EVFILT_PROC on kqueue. Returns 0 once
 * the poller owns the op, -ENOTSUP when there is nothing to watch with (no
 * such facility, or pidfd_open failed: an old kernel, or the process is
 * gone, which waitpid will say), else the hand-over's error, the op still
 * being the caller's.
 */
static int ior_threads_pool_watch_proc(ior_threads_pool *pool, ior_work *w, ior_work *lt, pid_t pid)
{
#if defined(IOR_HAVE_KQUEUE)
	return ior_threads_pool_hand_to_poller(pool, w, lt, (int) pid, IOR_THREADS_POLLER_PROC);
#elif defined(IOR_HAVE_PIDFD_OPEN) && defined(IOR_HAVE_EPOLL)
	int pidfd = (int) syscall(SYS_pidfd_open, pid, 0);
	if (pidfd < 0) {
		return -ENOTSUP;
	}
	w->pidfd = pidfd;
	int ret = ior_threads_pool_hand_to_poller(pool, w, lt, pidfd, IOR_POLL_IN);
	if (ret < 0) {
		close(pidfd);
		w->pidfd = -1;
	}
	return ret;
#else
	(void) pool;
	(void) w;
	(void) lt;
	(void) pid;
	return -ENOTSUP;
#endif
}

/*
 * One pass of a WAITPID op on a worker, entered as TRYING. It first asks
 * waitpid(2) without waiting, which also answers WNOHANG. If nothing has
 * changed yet, a single child is watched without a thread and the op parks
 * on the poller (*parked), coming back here with w->ready once the child
 * exited so the state can be collected. What the platform cannot watch (any
 * child, a process group, stop and continue reports, no pidfd) or a watch
 * that failed to deliver blocks this worker in waitpid(2) instead, as
 * RUNNING: a cancel then reports -EALREADY.
 */
static int32_t ior_threads_pool_waitpid(
		ior_threads_pool *pool, ior_work *w, ior_work *lt, int *parked)
{
	pid_t pid = (pid_t) (int64_t) w->sqe.threads.off;
	int *status = (int *) (uintptr_t) w->sqe.threads.addr;
	int options = (int) w->sqe.threads.len;

	pid_t r = waitpid(pid, status, options | WNOHANG);
	if (r != 0 || (options & WNOHANG)) {
		return r < 0 ? -errno : r;
	}
	if (!w->ready && pid > 0 && options == 0) {
		int ret = ior_threads_pool_watch_proc(pool, w, lt, pid);
		if (ret == 0) {
			*parked = 1;
			return 0;
		}
		if (ret != -ENOTSUP) {
			return ret;
		}
	}
	w->ready = 0;

	if (ior_threads_pool_enter(w, IOR_WORK_RUNNING) < 0) {
		return -ECANCELED;
	}
	do {
		r = waitpid(pid, status, options);
	} while (r < 0 && errno == EINTR);
	return r < 0 ? -errno : r;
}

/*
 * Run a work op guarded by a link timeout whose deadline `arb` (may be NULL)
 * was armed by ior_threads_pool_lt_arb_arm. Resolution (matching io_uring):
 *   - callback finishes first: work res = callback's return, LT = -ECANCELED;
 *   - deadline fires while the callback runs: work res = callback's return
 *     (posted when it returns), LT = -ETIME.
 * Returns non-zero if the deadline fired. The worker's arb ref is not
 * dropped here.
 */
static int ior_threads_pool_process_work_timed(ior_threads_pool *pool, ior_work *w, ior_work *lt,
		ior_threads_pool_lt_arb *arb, ior_cqe *gcqe, ior_cqe *lcqe)
{
	ior_threads_pool_process_single_sqe(pool, w, gcqe, w->cur_token);

	int fired = 0;
	if (arb) {
		int expected = 0;
		if (!atomic_compare_exchange_strong(&arb->state, &expected, 1)) {
			fired = 1; // timer thread claimed the deadline while the callback ran
		}
		// The caller drops the worker's ref once w is retired: until then a
		// cancel may still flag the token through w->cur_token.
	}

	memset(lcqe, 0, sizeof(*lcqe));
	lcqe->threads.user_data = lt->sqe.threads.user_data;
	lcqe->threads.res = fired ? -ETIME : -ECANCELED;

	return fired;
}

/*
 * Process one dispatched work chain. A chain is a run of IO_LINK ops claimed as
 * a unit (head plus head->chain->...), so the whole chain is owned by this one
 * worker - no other worker can run a mid-chain op out of order. Ops run in order
 * until one fails, after which the remainder are cancelled (-ECANCELED), matching
 * io_uring link semantics.
 *
 * Blocking read/write/send/recv never block a worker: an op whose descriptor
 * is not ready is parked on the poller (like io_uring's poll retry) and comes
 * back here once it is, so it stays cancellable and a non-blocking descriptor
 * never spins on EAGAIN.
 */
static void ior_threads_pool_process_chain(ior_threads_pool *pool, ior_work *head)
{
	uint64_t count = 0;
	ior_work *w = head;
	int cancel = 0; // a prior linked op failed/timed out; cancel the rest

	while (w) {
		ior_work *next = w->chain;
		uint8_t opcode = w->sqe.threads.opcode;
		int has_link = (w->sqe.threads.flags & IOR_SQE_IO_LINK) != 0;

		if (cancel) {
			ior_threads_pool_finish_res(pool, w, -ECANCELED);
			count++;
			w = next;
			continue;
		}

		// DRAIN: wait until every earlier-submitted op has completed.
		if ((w->sqe.threads.flags & IOR_SQE_IO_DRAIN) && ior_threads_pool_drain_wait(pool, w) < 0) {
			ior_threads_pool_finish_res(pool, w, -ECANCELED);
			count++;
			if (has_link) {
				cancel = 1;
			}
			w = next;
			continue;
		}

		// Timers run on the dedicated timer thread, which finishes the op on
		// expiry. A timeout completes with -ETIME, breaking any following link.
		if (opcode == IOR_OP_TIMER) {
			// An invalid timespec fails as a running op: a cancel then reports
			// -EALREADY rather than claiming an op that completes -EINVAL.
			int valid = ior_threads_pool_timer_valid(w);
			if (ior_threads_pool_enter(w, valid ? IOR_WORK_TIMER : IOR_WORK_RUNNING) < 0) {
				ior_threads_pool_finish_res(pool, w, -ECANCELED);
			} else if (!valid) {
				ior_threads_pool_finish_res(pool, w, -EINVAL);
			} else {
				ior_threads_pool_arm_timer(pool, w);
			}
			count++;
			if (has_link) {
				cancel = 1;
			}
			w = next;
			continue;
		}

		// A cancel that is part of a chain (or drained) runs here in order.
		if (opcode == IOR_OP_ASYNC_CANCEL) {
			int res = ior_threads_pool_enter(w, IOR_WORK_RUNNING) < 0
					? -ECANCELED
					: ior_threads_pool_cancel(pool, w);
			ior_threads_pool_finish_res(pool, w, res);
			count++;
			if (has_link && res < 0) {
				cancel = 1;
			}
			w = next;
			continue;
		}

		/*
		 * Link timeout: a linked op immediately followed by a LINK_TIMEOUT is
		 * bounded by the timeout's deadline; both are completed together. The
		 * pair is owned by this worker (claimed as one chain), so there is no
		 * race over the link-timeout slot.
		 */
		ior_work *lt = NULL;
		if (has_link && next && next->sqe.threads.opcode == IOR_OP_LINK_TIMEOUT) {
			lt = next;
		}
		// Capture the post-timeout successor before finishing (which frees the
		// work items and may hand them to another thread).
		ior_work *after = lt ? lt->chain : next;

		// A process wait: probe, park on the poller, or hold this worker.
		if (opcode == IOR_OP_WAITPID) {
			int32_t res;
			int parked = 0;
			if (ior_threads_pool_enter(w, IOR_WORK_TRYING) < 0) {
				res = -ECANCELED;
			} else {
				res = ior_threads_pool_waitpid(pool, w, lt, &parked);
				if (parked) {
					atomic_fetch_add(&pool->tasks_completed, count);
					return;
				}
			}
			ior_threads_pool_finish_res(pool, w, res);
			count++;
			if (lt) {
				ior_threads_pool_finish_res(pool, lt, -ECANCELED);
				count++;
			}
			if (has_link && res < 0) {
				cancel = 1;
			}
			w = after;
			continue;
		}

		/*
		 * Readiness gate: poll ops always wait on the poller. An rw op runs
		 * at once and parks below if it would block, which needs a descriptor
		 * that reports rather than waits; where the mode cannot be taken over
		 * a readiness probe stands in, and an unready descriptor is parked
		 * without attempting the syscall at all.
		 */
		short events = ior_threads_pool_rw_events(&w->sqe);
		int nowait = ior_threads_pool_rw_nowait(&w->sqe);
		int unready = 0;
		if (events && !w->ready && ior_threads_pool_rw_needs_nonblock(&w->sqe)
				&& !(pool->ctx->flags & IOR_SETUP_FD_NONBLOCK)
				&& ior_threads_pool_set_nonblock(w->sqe.threads.fd) < 0) {
			unready = !ior_threads_pool_fd_ready(w->sqe.threads.fd, events);
		}
		int gate = opcode == IOR_OP_POLL || (unready && !nowait);
		if (gate) {
			uint32_t mask = opcode == IOR_OP_POLL ? w->sqe.threads.poll_events
												  : (events == POLLIN ? IOR_POLL_IN : IOR_POLL_OUT);
			int ret = ior_threads_pool_hand_to_poller(pool, w, lt, w->sqe.threads.fd, mask);
			if (ret == 0) {
				// Ownership of w and its whole chain moved to the poller.
				atomic_fetch_add(&pool->tasks_completed, count);
				return;
			}

			// Registration failed (or a cancel claimed the op meanwhile): fail
			// the op here, cancel any linked rest.
			ior_threads_pool_finish_res(pool, w, ret);
			count++;
			if (lt) {
				ior_threads_pool_finish_res(pool, lt, -ECANCELED);
				count++;
			}
			if (has_link) {
				cancel = 1;
			}
			w = after;
			continue;
		}
		if (w->ready) {
			w->ready = 0;
		}

		/*
		 * Probed not ready with MSG_DONTWAIT asked for: answer -EAGAIN here,
		 * since the syscall would wait rather than report it. A cancel that
		 * claimed the op still wins.
		 */
		if (unready && nowait) {
			int res = ior_threads_pool_enter(w, IOR_WORK_RUNNING) < 0 ? -ECANCELED : -EAGAIN;
			ior_threads_pool_finish_res(pool, w, res);
			count++;
			if (lt) {
				ior_threads_pool_finish_res(pool, lt, -ECANCELED);
				count++;
			}
			if (has_link) {
				cancel = 1; // a failed linked op breaks the chain
			}
			w = after;
			continue;
		}

		/*
		 * A guarded work op cannot be poll-gated: the callback runs on this
		 * worker while the timer thread arbitrates the deadline and flags
		 * the token so the callback can bail out.
		 */
		if (lt && opcode == IOR_OP_WORK) {
			ior_cqe gcqe, lcqe;
			ior_threads_pool_lt_arb *arb = ior_threads_pool_lt_arb_arm(pool, lt);
			// An async cancel flags this token (w stays RUNNING, and this
			// worker holds arb, until the callback returns).
			w->cur_token = arb ? &arb->token : &w->token;
			if (ior_threads_pool_enter(w, IOR_WORK_RUNNING) < 0) {
				memset(&gcqe, 0, sizeof(gcqe));
				gcqe.threads.user_data = w->sqe.threads.user_data;
				gcqe.threads.res = -ECANCELED;
				memset(&lcqe, 0, sizeof(lcqe));
				lcqe.threads.user_data = lt->sqe.threads.user_data;
				lcqe.threads.res = -ECANCELED;
			} else {
				ior_threads_pool_process_work_timed(pool, w, lt, arb, &gcqe, &lcqe);
			}
			int failed = gcqe.threads.res < 0;
			ior_threads_pool_finish_op(pool, w, &gcqe);
			if (arb) {
				// Only now: a cancel dereferences w->cur_token under work_lock
				// while w is RUNNING, and finish_op took that lock to retire w.
				ior_threads_pool_lt_arb_release(arb);
			}
			ior_threads_pool_finish_op(pool, lt, &lcqe);
			count += 2;

			if (failed) {
				cancel = 1; // a failed linked op breaks the chain
			}
			w = after;
			continue;
		}

		/*
		 * Run the op now. The token pointer is published before the state so
		 * a cancel that sees RUNNING can flag it. An rw op that may still park
		 * (send/recv, positionless read/write) runs as TRYING instead: a
		 * cancel can claim that, and the claim is honoured below by finishing
		 * the op instead of parking it.
		 */
		ior_cqe cqe;
		w->cur_token = &w->token;
		int may_park = events && !nowait
				&& (opcode == IOR_OP_SEND || opcode == IOR_OP_RECV
						|| ior_threads_pool_rw_needs_nonblock(&w->sqe));
		if (ior_threads_pool_enter(w, may_park ? IOR_WORK_TRYING : IOR_WORK_RUNNING) < 0) {
			memset(&cqe, 0, sizeof(cqe));
			cqe.threads.user_data = w->sqe.threads.user_data;
			cqe.threads.res = -ECANCELED;
		} else {
			ior_threads_pool_process_single_sqe(pool, w, &cqe, &w->token);

			/*
			 * The op would block (send/recv are always issued with
			 * MSG_DONTWAIT; a non-blocking descriptor says so itself): park
			 * on the poller and retry once it is ready, unless the caller
			 * asked for MSG_DONTWAIT semantics. A cancel that claimed the op
			 * meanwhile makes hand_to_poller fail with -ECANCELED, which is
			 * then the result; a syscall that did complete keeps its real
			 * result.
			 */
			if (events && ior_threads_pool_res_would_block(opcode, cqe.threads.res)
					&& !ior_threads_pool_rw_nowait(&w->sqe)) {
				uint32_t mask = events == POLLIN ? IOR_POLL_IN : IOR_POLL_OUT;
				int ret = ior_threads_pool_hand_to_poller(pool, w, lt, w->sqe.threads.fd, mask);
				if (ret == 0) {
					atomic_fetch_add(&pool->tasks_completed, count);
					return;
				}
				cqe.threads.res = ret;
			}
		}
		ior_threads_pool_finish_op(pool, w, &cqe);
		count++;

		if (lt) {
			// The guarded op finished first: its link timeout is cancelled.
			ior_threads_pool_finish_res(pool, lt, -ECANCELED);
			count++;
		}

		if (has_link && cqe.threads.res < 0) {
			cancel = 1; // a failed linked op breaks the chain
		}
		w = after;
	}

	atomic_fetch_add(&pool->tasks_completed, count);
}

/*
 * Accept with accept4 semantics for the flags. Where accept4 is missing, or
 * where the accepted socket inherits the listener's mode (BSD), set exactly
 * the state the caller asked for: the listener is non-blocking because ior
 * made it so, which must not leak into the accepted socket.
 */
static int ior_threads_pool_accept(
		int fd, struct sockaddr *addr, socklen_t *addrlen, unsigned flags)
{
#ifdef IOR_HAVE_ACCEPT4
	int nfd = accept4(fd, addr, addrlen, (int) flags);
#else
	int nfd = accept(fd, addr, addrlen);
#endif
	if (nfd < 0) {
		return -errno;
	}
// accept4 is authoritative on Linux alone; elsewhere it may still hand back
// the listener's mode, and without it the flags need applying by hand.
#if !defined(IOR_HAVE_ACCEPT4) || !defined(__linux__)
	int fl = fcntl(nfd, F_GETFL, 0);
	if (fl >= 0) {
		fl = (flags & IOR_ACCEPT_NONBLOCK) ? (fl | O_NONBLOCK) : (fl & ~O_NONBLOCK);
		(void) fcntl(nfd, F_SETFL, fl);
	}
	if (flags & IOR_ACCEPT_CLOEXEC) {
		int fdfl = fcntl(nfd, F_GETFD, 0);
		if (fdfl >= 0) {
			(void) fcntl(nfd, F_SETFD, fdfl | FD_CLOEXEC);
		}
	}
#endif
	return nfd;
}

static void ior_threads_pool_process_single_sqe(
		ior_threads_pool *pool, ior_work *w, ior_cqe *cqe, ior_work_token *token)
{
	(void) pool;
	const ior_sqe *sqe = &w->sqe;

	memset(cqe, 0, sizeof(*cqe));
	cqe->threads.user_data = sqe->threads.user_data;
	cqe->threads.flags = 0;

	// Process based on operation type
	switch (sqe->threads.opcode) {
		case IOR_OP_NOP:
			cqe->threads.res = 0;
			break;

		case IOR_OP_WORK: {
			ior_work_fn fn = (ior_work_fn) (uintptr_t) sqe->threads.addr;
			void *arg = (void *) (uintptr_t) sqe->threads.off;
			IOR_LOG_TRACE("work start: fn=%p, arg=%p", (void *) (uintptr_t) sqe->threads.addr, arg);
			cqe->threads.res = fn(token, arg);
			IOR_LOG_TRACE("work end: res=%d", cqe->threads.res);
			break;
		}

		case IOR_OP_READ: {
			IOR_LOG_TRACE("read start: fd=%d, addr=%p, len=%u, flags=%lu", sqe->threads.fd,
					(void *) (uintptr_t) sqe->threads.addr, sqe->threads.len, sqe->threads.off);
			void *buf = (void *) (uintptr_t) sqe->threads.addr;
			ssize_t ret;
			/*
			 * Use pread() for seekable fds (regular files). For non-seekable
			 * fds (sockets, pipes, FIFOs) pread() fails with ESPIPE, so fall
			 * back to read(), which uses the fd's own position. The explicit
			 * IOR_OFF_NONE sentinel also selects read() directly. This matches
			 * io_uring, whose read op works uniformly on files and sockets.
			 */
			if (sqe->threads.off == IOR_OFF_NONE) {
				ret = read(sqe->threads.fd, buf, sqe->threads.len);
			} else {
				ret = pread(sqe->threads.fd, buf, sqe->threads.len, sqe->threads.off);
				if (ret < 0 && errno == ESPIPE) {
					ret = read(sqe->threads.fd, buf, sqe->threads.len);
				}
			}
			cqe->threads.res = (ret < 0) ? -errno : ret;
			IOR_LOG_TRACE("read end: res=%d", cqe->threads.res);
			break;
		}

		case IOR_OP_WRITE: {
			IOR_LOG_TRACE("write start: fd=%d, addr=%p, len=%u, flags=%lu", sqe->threads.fd,
					(void *) (uintptr_t) sqe->threads.addr, sqe->threads.len, sqe->threads.off);
			const void *buf = (const void *) (uintptr_t) sqe->threads.addr;
			ssize_t ret;
			/* See IOR_OP_READ above: pwrite() for seekable fds, write() for
			 * non-seekable ones (sockets/pipes) or the IOR_OFF_NONE sentinel. */
			if (sqe->threads.off == IOR_OFF_NONE) {
				ret = write(sqe->threads.fd, buf, sqe->threads.len);
			} else {
				ret = pwrite(sqe->threads.fd, buf, sqe->threads.len, sqe->threads.off);
				if (ret < 0 && errno == ESPIPE) {
					ret = write(sqe->threads.fd, buf, sqe->threads.len);
				}
			}
			cqe->threads.res = (ret < 0) ? -errno : ret;
			IOR_LOG_TRACE("write end: res=%d", cqe->threads.res);
			break;
		}

		case IOR_OP_SPLICE: {
			int fd_in = sqe->threads.splice_fd_in, fd_out = sqe->threads.fd;
			loff_t *off_in = sqe->threads.splice_off_in == IOR_OFF_NONE
					? NULL
					: (loff_t *) sqe->threads.splice_off_in;
			loff_t *off_out = sqe->threads.off == IOR_OFF_NONE ? NULL : (loff_t *) sqe->threads.off;
			IOR_LOG_TRACE("splice start: fd_in=%d, off_in=%lu, fd_out=%d, off_out=%lu, size=%u, "
						  "flags=%u",
					fd_in, sqe->threads.splice_off_in, fd_out, sqe->threads.off, sqe->threads.len,
					sqe->threads.splice_flags);
#ifdef IOR_HAVE_SPLICE
			ssize_t ret = splice(
					fd_in, off_in, fd_out, off_out, sqe->threads.len, sqe->threads.splice_flags);
#else
			// Emulate splice using read/write loop
			ssize_t ret = ior_threads_pool_emulate_splice(
					fd_in, off_in, fd_out, off_out, sqe->threads.len, sqe->threads.splice_flags);
#endif
			cqe->threads.res = (ret < 0) ? -errno : ret;
			IOR_LOG_TRACE("splice end: res=%d", cqe->threads.res);
			break;
		}

		case IOR_OP_SEND: {
			IOR_LOG_TRACE("send start: fd=%d, addr=%p, len=%u, flags=%u", sqe->threads.fd,
					(void *) (uintptr_t) sqe->threads.addr, sqe->threads.len,
					sqe->threads.rw_flags);
			const void *buf = (const void *) (uintptr_t) sqe->threads.addr;
			// Never block a worker: readiness is waited for on the poller.
			// Where the flag is ignored (see IOR_SEND_HONOURS_DONTWAIT) the
			// descriptor is put in non-blocking mode first, which XNU honours.
			ssize_t ret = send(sqe->threads.fd, buf, sqe->threads.len,
					(int) sqe->threads.rw_flags | MSG_DONTWAIT);
			cqe->threads.res = (ret < 0) ? -errno : ret;
			IOR_LOG_TRACE("send end: res=%d", cqe->threads.res);
			break;
		}

		case IOR_OP_RECV: {
			IOR_LOG_TRACE("recv start: fd=%d, addr=%p, len=%u, flags=%u", sqe->threads.fd,
					(void *) (uintptr_t) sqe->threads.addr, sqe->threads.len,
					sqe->threads.rw_flags);
			void *buf = (void *) (uintptr_t) sqe->threads.addr;
			// Never block a worker: readiness is waited for on the poller.
			ssize_t ret = recv(sqe->threads.fd, buf, sqe->threads.len,
					(int) sqe->threads.rw_flags | MSG_DONTWAIT);
			cqe->threads.res = (ret < 0) ? -errno : ret;
			IOR_LOG_TRACE("recv end: res=%d", cqe->threads.res);
			break;
		}

		case IOR_OP_LINK_TIMEOUT:
			// A link timeout is normally consumed alongside its guarded op in
			// process_sqe_chain. Reaching here means it was picked standalone;
			// resolve it as "guarded op already finished" rather than failing.
			cqe->threads.res = -ECANCELED;
			break;

		case IOR_OP_ACCEPT: {
			IOR_LOG_TRACE("accept start: fd=%d", sqe->threads.fd);
			cqe->threads.res = ior_threads_pool_accept(sqe->threads.fd,
					(struct sockaddr *) (uintptr_t) sqe->threads.addr,
					(socklen_t *) (uintptr_t) sqe->threads.off, sqe->threads.rw_flags);
			IOR_LOG_TRACE("accept end: res=%d", cqe->threads.res);
			break;
		}

		case IOR_OP_CONNECT: {
			IOR_LOG_TRACE("connect start: fd=%d", sqe->threads.fd);
			if (w->connecting) {
				// Writable after -EINPROGRESS: the outcome is in SO_ERROR.
				int err = 0;
				socklen_t errlen = sizeof(err);
				if (getsockopt(sqe->threads.fd, SOL_SOCKET, SO_ERROR, &err, &errlen) < 0) {
					err = errno;
				}
				cqe->threads.res = -err;
			} else {
				int ret = connect(sqe->threads.fd,
						(const struct sockaddr *) (uintptr_t) sqe->threads.addr,
						(socklen_t) sqe->threads.off);
				if (ret < 0 && errno == EINTR) {
					errno = EINPROGRESS; // the connection continues asynchronously
				}
				if (ret < 0 && errno == EINPROGRESS) {
					w->connecting = 1;
				}
				cqe->threads.res = (ret < 0) ? -errno : 0;
			}
			IOR_LOG_TRACE("connect end: res=%d", cqe->threads.res);
			break;
		}

		case IOR_OP_LISTEN:
		case IOR_OP_BIND:
			cqe->threads.res = -ENOSYS;
			break;

		default:
			cqe->threads.res = -EINVAL;
			break;
	}
}

static void ior_threads_pool_post_completion(ior_threads_pool *pool, const ior_cqe *cqe)
{
	ior_ctx_threads *ctx = pool->ctx;

	// Try to post CQE to completion ring
	int ret = ior_threads_ring_post_cqe(&ctx->cq_ring, cqe);

	if (ret == -EOVERFLOW) {
		IOR_LOG_WARN("cqe overlow");
		// CQ ring full - exponential backoff
		int backoff_us = 100;
		const int max_backoff_us = 10000;

		while (ior_threads_ring_post_cqe(&ctx->cq_ring, cqe) == -EOVERFLOW) {
			usleep(backoff_us);

			if (backoff_us < max_backoff_us) {
				backoff_us *= 2;
			}

			// Signal event to wake consumer
			ior_threads_event_signal(&ctx->event);
		}
	}

	IOR_LOG_TRACE("signaling completion");
	// Signal event to wake waiting thread
	ior_threads_event_signal(&ctx->event);
}

// ===== Timers =====

// Fires on the worker pool's timer thread when a timeout op expires. A cancel
// that claimed the op after it was popped from the heap wins: -ECANCELED.
static void ior_threads_pool_timer_fired(void *owner, void *arg)
{
	ior_threads_pool *pool = owner;
	ior_work *work = arg;

	int prev = atomic_exchange(&work->state, IOR_WORK_RUNNING);
	ior_threads_pool_finish_res(pool, work, prev == IOR_WORK_CANCELLED ? -ECANCELED : -ETIME);
}

static int ior_threads_pool_timer_valid(const ior_work *work)
{
	ior_timespec *ts = (ior_timespec *) (uintptr_t) work->sqe.threads.addr;
	return ts && ts->tv_sec >= 0 && ts->tv_nsec >= 0 && ts->tv_nsec < 1000000000L;
}

/*
 * Arm a timeout (already validated) on the shared pool's timer thread, which
 * finishes it on expiry. A heap allocation failure finishes inline so the
 * caller never has to special-case it. Arming happens under arm_lock so that
 * a cancel racing it either finds the armed timer or, having claimed the op
 * first, is honoured here - including when arming failed.
 */
static void ior_threads_pool_arm_timer(ior_threads_pool *pool, ior_work *work)
{
	ior_timespec *ts = (ior_timespec *) (uintptr_t) work->sqe.threads.addr;
	int err = 0;

	// IOR_TIMEOUT_ABS: ts is an absolute CLOCK_MONOTONIC deadline; otherwise
	// it is a relative duration from now.
	uint64_t ts_ns = (uint64_t) ts->tv_sec * 1000000000ULL + (uint64_t) ts->tv_nsec;
	uint64_t deadline_ns = (work->sqe.threads.timeout_flags & IOR_TIMEOUT_ABS)
			? ts_ns
			: ior_worker_pool_monotonic_ns() + ts_ns;

	pthread_mutex_lock(&pool->arm_lock);
	int ret = ior_worker_pool_arm_timer(
			pool->wp, deadline_ns, ior_threads_pool_timer_fired, NULL, work);
	int claimed = atomic_load_explicit(&work->state, memory_order_acquire) == IOR_WORK_CANCELLED;
	if (ret < 0) {
		err = claimed ? ECANCELED : ENOMEM;
	} else if (claimed && ior_worker_pool_cancel_timer(pool->wp, work) == 0) {
		// Claimed between enter and arm; the timer never fires.
		err = ECANCELED;
	}
	pthread_mutex_unlock(&pool->arm_lock);

	if (err) {
		ior_threads_pool_finish_res(pool, work, -err);
	}
}

// ===== Async cancel =====

static int ior_threads_pool_op_has_fd(uint8_t opcode)
{
	switch (opcode) {
		case IOR_OP_READ:
		case IOR_OP_WRITE:
		case IOR_OP_SEND:
		case IOR_OP_RECV:
		case IOR_OP_POLL:
		case IOR_OP_SPLICE:
		case IOR_OP_ACCEPT:
		case IOR_OP_CONNECT:
			return 1;
		default:
			return 0;
	}
}

static int ior_threads_pool_cancel_match(const ior_work *w, const ior_sqe *c)
{
	if (c->threads.cancel_flags & IOR_CANCEL_BY_FD) {
		return ior_threads_pool_op_has_fd(w->sqe.threads.opcode)
				&& w->sqe.threads.fd == c->threads.fd;
	}
	return w->sqe.threads.user_data == c->threads.addr;
}

/*
 * Try to cancel one in-flight op (work_lock held). Ops that this call takes
 * away from their owner (a queued chain, an armed timer) are pushed on `done`
 * for completion once the lock is released; ops claimed from a waiting owner
 * complete on that owner's thread. Returns the cancel result for this op:
 * 0, -EALREADY, or -ENOENT when it completed meanwhile.
 */
static int ior_threads_pool_cancel_one(ior_threads_pool *pool, ior_work *w, ior_work **done)
{
	for (;;) {
		int state = atomic_load_explicit(&w->state, memory_order_acquire);
		int expected = state;
		switch (state) {
			case IOR_WORK_QUEUED:
				if (ior_worker_pool_cancel_job(pool->wp, &w->job) == 0) {
					// The chain is ours now: no worker will touch it.
					for (ior_work *m = w; m; m = m->chain) {
						atomic_store_explicit(&m->state, IOR_WORK_CANCELLED, memory_order_release);
					}
					w->next = *done;
					*done = w;
					return 0;
				}
				// A worker popped it but has not started it: claim it so its
				// first state transition fails and it completes as cancelled.
				if (atomic_compare_exchange_strong(&w->state, &expected, IOR_WORK_CANCELLED)) {
					return 0;
				}
				continue;

			case IOR_WORK_DRAINING:
				if (atomic_compare_exchange_strong(&w->state, &expected, IOR_WORK_CANCELLED)) {
					pthread_mutex_lock(&pool->drain_lock);
					pthread_cond_broadcast(&pool->drain_cond);
					pthread_mutex_unlock(&pool->drain_lock);
					return 0;
				}
				continue;

			case IOR_WORK_TRYING:
				// A non-blocking attempt in progress: like io_uring's transient
				// poll ownership, report it as running. The claim still keeps
				// the worker from parking it: the attempt either completes
				// with its real result or ends as -ECANCELED.
				if (atomic_compare_exchange_strong(&w->state, &expected, IOR_WORK_CANCELLED)) {
					return -EALREADY;
				}
				continue;

			case IOR_WORK_TIMER: {
				// Under arm_lock the worker is either before or after its
				// arm-and-check, never in between (see arm_timer).
				pthread_mutex_lock(&pool->arm_lock);
				if (ior_worker_pool_cancel_timer(pool->wp, w) == 0) {
					atomic_store_explicit(&w->state, IOR_WORK_CANCELLED, memory_order_release);
					pthread_mutex_unlock(&pool->arm_lock);
					w->chain = NULL; // a linked rest was already finished by the worker
					w->next = *done;
					*done = w;
					return 0;
				}
				// Firing, or not armed yet: whoever finishes it sees the claim.
				int claimed
						= atomic_compare_exchange_strong(&w->state, &expected, IOR_WORK_CANCELLED);
				pthread_mutex_unlock(&pool->arm_lock);
				if (claimed) {
					return 0;
				}
				continue;
			}

			case IOR_WORK_POLLING: {
				ior_threads_poller *poller
						= atomic_load_explicit(&pool->poller, memory_order_acquire);
				if (poller && ior_threads_poller_cancel(poller, w) == 0) {
					return 0;
				}
				// Being dispatched, or not registered yet: same as above.
				if (atomic_compare_exchange_strong(&w->state, &expected, IOR_WORK_CANCELLED)) {
					return 0;
				}
				continue;
			}

			case IOR_WORK_RUNNING:
				// A syscall or callback in progress cannot be interrupted; a
				// work callback can at least observe the request.
				if (w->cur_token) {
					atomic_store_explicit(&w->cur_token->cancelled, 1, memory_order_release);
				}
				return -EALREADY;

			case IOR_WORK_CANCELLED:
				return -EALREADY;

			default:
				return -ENOENT;
		}
	}
}

/*
 * Execute a cancel op: find the first in-flight match and resolve it per
 * io_uring semantics (0 cancelled, -EALREADY running, -ENOENT none). The
 * scan runs under work_lock, which also keeps the matched item allocated
 * until its owner has been dealt with.
 */
static int ior_threads_pool_cancel(ior_threads_pool *pool, ior_work *self)
{
	const ior_sqe *c = &self->sqe;

	if ((c->threads.cancel_flags & IOR_CANCEL_BY_FD) && c->threads.fd == IOR_INVALID_FD) {
		return -EBADF;
	}

	int ret = -ENOENT;
	ior_work *done = NULL;

	pthread_mutex_lock(&pool->work_lock);
	for (uint32_t i = 0; i < pool->work_cap; i++) {
		ior_work *w = &pool->work_items[i];
		if (w == self) {
			continue;
		}
		int state = atomic_load_explicit(&w->state, memory_order_acquire);
		if (state == IOR_WORK_FREE || state == IOR_WORK_LINKED || state == IOR_WORK_DONE) {
			continue;
		}
		if (!ior_threads_pool_cancel_match(w, c)) {
			continue;
		}
		ret = ior_threads_pool_cancel_one(pool, w, &done);
		if (ret != -ENOENT) {
			break;
		}
	}
	pthread_mutex_unlock(&pool->work_lock);

	uint64_t count = 0;
	while (done) {
		ior_work *d = done;
		done = d->next;
		count += ior_threads_pool_cancel_chain(pool, d);
	}
	if (count) {
		atomic_fetch_add(&pool->tasks_completed, count);
	}

	return ret;
}

// ===== Splice Emulation =====

#ifndef IOR_HAVE_SPLICE
static ssize_t ior_threads_pool_emulate_splice(
		int fd_in, loff_t *off_in, int fd_out, loff_t *off_out, size_t len, unsigned int flags)
{
	(void) flags;

	const size_t BUFFER_SIZE = 65536; // 64KB buffer
	char *buffer = malloc(BUFFER_SIZE);
	if (!buffer) {
		errno = ENOMEM;
		return -1;
	}

	size_t total_transferred = 0;

	while (total_transferred < len) {
		size_t to_read = len - total_transferred;
		if (to_read > BUFFER_SIZE) {
			to_read = BUFFER_SIZE;
		}

		ssize_t nread;
		if (off_in) {
			nread = pread(fd_in, buffer, to_read, *off_in);
			if (nread > 0) {
				*off_in += nread;
			}
		} else {
			nread = read(fd_in, buffer, to_read);
		}

		if (nread < 0) {
			free(buffer);
			return -1;
		}

		if (nread == 0) {
			break; // EOF
		}

		ssize_t nwritten;
		if (off_out) {
			nwritten = pwrite(fd_out, buffer, nread, *off_out);
			if (nwritten > 0) {
				*off_out += nwritten;
			}
		} else {
			nwritten = write(fd_out, buffer, nread);
		}

		if (nwritten < 0) {
			free(buffer);
			return -1;
		}

		if (nwritten != nread) {
			// Partial write
			total_transferred += nwritten;
			break;
		}

		total_transferred += nwritten;
	}

	free(buffer);
	return (ssize_t) total_transferred;
}
#endif
