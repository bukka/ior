/* SPDX-License-Identifier: BSD-3-Clause */
#include "test_utils.h"
#include <pthread.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <time.h>

#define NOP_TAG(i) ((void *) (uintptr_t) (0x100 + (i)))
#define RECV_TAG(i) ((void *) (uintptr_t) (0x1000 + (i)))
#define CANCEL_TAG(i) ((void *) (uintptr_t) (0x2000 + (i)))
#define POLL_TAG ((void *) (uintptr_t) 0x3000)

/* Not assert_uint_in_range: cmocka 1.x lacks it, 2.x deprecates assert_in_range. */
#define assert_nop_tag(tag) assert_true((uintptr_t) (tag) >= 0x100 && (uintptr_t) (tag) <= 0x11f)

// Test forcing threads backend
static void test_threads_backend(void **state)
{
	(void) state;

	ior_ctx *ctx;
	ior_params params = {
		.sq_entries = 32,
		.cq_entries = 64,
		.flags = 0,
		.backend = IOR_BACKEND_THREADS, // Force threads
	};

	int ret = ior_queue_init_params(32, &ctx, &params);
	assert_return_code(ret, 0);

	// Verify it's using threads backend
	assert_int_equal(ior_get_backend_type(ctx), IOR_BACKEND_THREADS);
	assert_string_equal(ior_get_backend_name(ctx), "threads");

	ior_queue_exit(ctx);
}

static ior_ctx *init_threads(uint32_t sq_entries, uint32_t cq_entries)
{
	ior_ctx *ctx;
	ior_params params = {
		.sq_entries = sq_entries,
		.cq_entries = cq_entries,
		.backend = IOR_BACKEND_THREADS,
	};
	assert_return_code(ior_queue_init_params(sq_entries, &ctx, &params), 0);
	return ctx;
}

static void submit_nops(ior_ctx *ctx, int n)
{
	for (int i = 0; i < n; i++) {
		ior_sqe *sqe = ior_get_sqe(ctx);
		assert_non_null(sqe);
		ior_prep_nop(ctx, sqe);
		ior_sqe_set_data(ctx, sqe, NOP_TAG(i));
	}
	assert_int_equal(ior_submit_and_wait(ctx, (unsigned) n), n);
}

/* Reap one completion within a second, returning its res and tag. */
static int32_t reap_one(ior_ctx *ctx, void **tag, uint32_t *flags)
{
	ior_timespec ts = { .tv_sec = 1, .tv_nsec = 0 };
	ior_cqe *cqe = NULL;
	assert_return_code(ior_wait_cqe_timeout(ctx, &cqe, &ts), 0);
	int32_t res = ior_cqe_get_res(ctx, cqe);
	if (tag) {
		*tag = ior_cqe_get_data(ctx, cqe);
	}
	if (flags) {
		*flags = ior_cqe_get_flags(ctx, cqe);
	}
	ior_cqe_seen(ctx, cqe);
	return res;
}

/*
 * The completion queue holds a slot for every submitted operation until its
 * completion is reaped: once it is full of unreaped completions, ior_get_sqe
 * returns NULL rather than letting a later completion find no room, and a
 * cancel submitted after reaping completes without waiting on the reaper.
 */
static void test_cq_full_get_sqe(void **state)
{
	(void) state;
	ior_ctx *ctx = init_threads(32, 32);

	submit_nops(ctx, 32);
	assert_null(ior_get_sqe(ctx));

	// Nops complete on workers, in no particular order.
	void *tag;
	assert_int_equal(reap_one(ctx, &tag, NULL), 0);
	assert_nop_tag(tag);

	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_cancel(ctx, sqe, (void *) (uintptr_t) 0xdead);
	ior_sqe_set_data(ctx, sqe, CANCEL_TAG(0));
	assert_int_equal(ior_submit(ctx), 1);
	assert_null(ior_get_sqe(ctx));

	for (int i = 1; i < 32; i++) {
		assert_int_equal(reap_one(ctx, &tag, NULL), 0);
		assert_nop_tag(tag);
	}
	assert_int_equal(reap_one(ctx, &tag, NULL), -ENOENT);
	assert_ptr_equal(tag, CANCEL_TAG(0));

	// Every slot is free again.
	submit_nops(ctx, 32);
	ior_cq_advance(ctx, 32);
	ior_queue_exit(ctx);
}

/*
 * Teardown shape: more parked ops than half the completion queue, each
 * cancelled without reaping in between. A cancel is refused an SQE while the
 * queue is full and goes through once completions have been reaped; nothing
 * waits on the submitting thread.
 */
static void test_cq_full_cancel_parked(void **state)
{
	(void) state;
	enum { N = 40 };
	ior_ctx *ctx = init_threads(32, 64);
	ior_fd_t sock[2];
	assert_return_code(test_make_socketpair(sock), 0);

	char bufs[N][8];
	for (int i = 0; i < N; i++) {
		ior_sqe *sqe = ior_get_sqe(ctx);
		if (!sqe) {
			assert_true(ior_submit(ctx) > 0);
			sqe = ior_get_sqe(ctx);
			assert_non_null(sqe);
		}
		ior_prep_recv(ctx, sqe, sock[1], bufs[i], sizeof(bufs[i]), 0);
		ior_sqe_set_data(ctx, sqe, RECV_TAG(i));
	}
	assert_true(ior_submit(ctx) > 0);

	int cancels_done = 0, recvs_done = 0, refused = 0;
	for (int i = 0; i < N; i++) {
		ior_sqe *sqe = ior_get_sqe(ctx);
		while (!sqe) {
			assert_true(ior_submit(ctx) >= 0);
			void *tag;
			int32_t res = reap_one(ctx, &tag, NULL);
			if ((uintptr_t) tag >= 0x2000) {
				// -EALREADY: the recv was in its non-blocking attempt; it
				// still ends -ECANCELED, the claim keeps it from parking.
				assert_true(res == 0 || res == -EALREADY);
				cancels_done++;
			} else {
				assert_int_equal(res, -ECANCELED);
				recvs_done++;
			}
			refused++;
			sqe = ior_get_sqe(ctx);
		}
		ior_prep_cancel(ctx, sqe, RECV_TAG(i));
		ior_sqe_set_data(ctx, sqe, CANCEL_TAG(i));
		assert_int_equal(ior_submit(ctx), 1);
	}
	assert_true(refused > 0);

	while (cancels_done < N || recvs_done < N) {
		void *tag;
		int32_t res = reap_one(ctx, &tag, NULL);
		if ((uintptr_t) tag >= 0x2000) {
			assert_true(res == 0 || res == -EALREADY);
			cancels_done++;
		} else {
			assert_int_equal(res, -ECANCELED);
			recvs_done++;
		}
	}

	test_close_fd(sock[0]);
	test_close_fd(sock[1]);
	ior_queue_exit(ctx);
}

/*
 * A multishot poll whose edge finds no free completion slot ends with that
 * edge as its last completion (no IOR_CQE_F_MORE), in the slot the op holds,
 * and reports nothing afterwards; the slots are all free once reaped.
 */
static void test_cq_full_multishot_ends(void **state)
{
	(void) state;
	ior_ctx *ctx = init_threads(32, 32);
	ior_fd_t sock[2];
	assert_return_code(test_make_socketpair(sock), 0);

	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_poll_multishot(ctx, sqe, sock[1], IOR_POLL_IN);
	ior_sqe_set_data(ctx, sqe, POLL_TAG);
	assert_int_equal(ior_submit(ctx), 1);

	submit_nops(ctx, 31);
	assert_null(ior_get_sqe(ctx));

	assert_int_equal(send(sock[0], "x", 1, 0), 1);

	// The edge is reported while every slot is still taken: wait for its
	// completion to land before reaping anything.
	assert_int_equal(ior_submit_and_wait(ctx, 32), 0);
	void *tag;
	uint32_t flags;
	int got_poll = 0;
	for (int i = 0; i < 32; i++) {
		int32_t res = reap_one(ctx, &tag, &flags);
		if (tag == POLL_TAG) {
			assert_true(res & IOR_POLL_IN);
			assert_false(flags & IOR_CQE_F_MORE);
			got_poll++;
		} else {
			assert_int_equal(res, 0);
			assert_nop_tag(tag);
		}
	}
	assert_int_equal(got_poll, 1);

	// The poll is gone: a further edge reports nothing, a cancel finds nothing.
	assert_int_equal(send(sock[0], "y", 1, 0), 1);
	ior_timespec ts = { .tv_sec = 0, .tv_nsec = 100000000L };
	ior_cqe *cqe = NULL;
	assert_int_equal(ior_wait_cqe_timeout(ctx, &cqe, &ts), -ETIME);
	sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_cancel(ctx, sqe, POLL_TAG);
	ior_sqe_set_data(ctx, sqe, CANCEL_TAG(0));
	assert_int_equal(ior_submit(ctx), 1);
	assert_int_equal(reap_one(ctx, &tag, NULL), -ENOENT);

	submit_nops(ctx, 32);
	ior_cq_advance(ctx, 32);

	test_close_fd(sock[0]);
	test_close_fd(sock[1]);
	ior_queue_exit(ctx);
}

typedef struct edge_feeder {
	ior_fd_t sock[2];
	atomic_int stop;
} edge_feeder;

/* Make a readiness edge on sock[1] over and over, draining it between. */
static void *feed_edges(void *arg)
{
	edge_feeder *f = arg;
	char buf[64];
	while (!atomic_load(&f->stop)) {
		(void) send(f->sock[0], "x", 1, MSG_DONTWAIT);
		(void) recv(f->sock[1], buf, sizeof(buf), MSG_DONTWAIT);
	}
	return NULL;
}

static uint64_t now_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t) ts.tv_sec * 1000 + (uint64_t) ts.tv_nsec / 1000000;
}

/*
 * ior_get_sqe and the poller thread (reserving a slot for a multishot edge)
 * promise completion slots concurrently. Keep the queue at its limit while
 * edges keep coming: a slot promised twice would end with a completion
 * posted into a full ring and lost, which shows as a poll that never
 * reports its end or a slot that is never given back.
 */
static void test_cq_reserve_race(void **state)
{
	(void) state;
	enum { SIZE = 32 };
	ior_ctx *ctx = init_threads(SIZE, SIZE);
	edge_feeder f;
	assert_return_code(test_make_socketpair(f.sock), 0);
	atomic_init(&f.stop, 0);
	pthread_t t;
	assert_return_code(pthread_create(&t, NULL, feed_edges, &f), 0);

	int poll_armed = 0;
	uint64_t end = now_ms() + 300;
	while (now_ms() < end) {
		ior_sqe *sqe;
		if (!poll_armed && (sqe = ior_get_sqe(ctx))) {
			ior_prep_poll_multishot(ctx, sqe, f.sock[1], IOR_POLL_IN);
			ior_sqe_set_data(ctx, sqe, POLL_TAG);
			poll_armed = 1;
		}
		int nops = 0;
		while ((sqe = ior_get_sqe(ctx))) {
			ior_prep_nop(ctx, sqe);
			ior_sqe_set_data(ctx, sqe, NOP_TAG(0));
			nops++;
		}
		assert_true(ior_submit(ctx) >= 0);

		// Let the nops land and edges pile up behind them before reaping,
		// so the ring runs full.
		ior_cqe *cqes[SIZE];
		uint64_t wait_end = now_ms() + 1000;
		while (ior_peek_batch_cqe(ctx, cqes, SIZE) < (unsigned) nops) {
			if (now_ms() >= wait_end) {
				fail_msg("a completion was lost");
			}
		}

		while (nops > 0) {
			void *tag;
			uint32_t flags;
			int32_t res = reap_one(ctx, &tag, &flags);
			if (tag == POLL_TAG) {
				assert_true(res > 0);
				if (!(flags & IOR_CQE_F_MORE)) {
					poll_armed = 0;
				}
			} else {
				assert_int_equal(res, 0);
				nops--;
			}
		}
		// Edges posted since, and possibly the poll's end.
		unsigned n;
		while ((n = ior_peek_batch_cqe(ctx, cqes, SIZE)) > 0) {
			for (unsigned i = 0; i < n; i++) {
				assert_ptr_equal(ior_cqe_get_data(ctx, cqes[i]), POLL_TAG);
				if (!(ior_cqe_get_flags(ctx, cqes[i]) & IOR_CQE_F_MORE)) {
					poll_armed = 0;
				}
			}
			ior_cq_advance(ctx, n);
		}
	}

	atomic_store(&f.stop, 1);
	pthread_join(t, NULL);

	// A live poll ends with -ECANCELED; one reported ended must be gone.
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_cancel(ctx, sqe, POLL_TAG);
	ior_sqe_set_data(ctx, sqe, CANCEL_TAG(0));
	assert_int_equal(ior_submit(ctx), 1);
	int cancel_seen = 0;
	while (!cancel_seen || poll_armed) {
		void *tag;
		uint32_t flags;
		int32_t res = reap_one(ctx, &tag, &flags);
		if (tag == CANCEL_TAG(0)) {
			assert_int_equal(res, poll_armed ? 0 : -ENOENT);
			cancel_seen = 1;
		} else {
			assert_ptr_equal(tag, POLL_TAG);
			if (!(flags & IOR_CQE_F_MORE)) {
				poll_armed = 0;
			}
		}
	}

	// Every slot is free again.
	submit_nops(ctx, SIZE);
	ior_cq_advance(ctx, SIZE);

	test_close_fd(f.sock[0]);
	test_close_fd(f.sock[1]);
	ior_queue_exit(ctx);
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test_threads_backend),
		cmocka_unit_test(test_cq_full_get_sqe),
		cmocka_unit_test(test_cq_full_cancel_parked),
		cmocka_unit_test(test_cq_full_multishot_ends),
		cmocka_unit_test(test_cq_reserve_race),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
