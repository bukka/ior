/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_capacity.c - the effective queue sizes and the free-entry counts, and
 * what bounds what, as on io_uring: only a full submission queue refuses an
 * entry (it wants a submit); a full completion queue refuses nothing, its
 * overflowing completions wait, in order, for a reap; and operations in
 * flight are bounded by neither.
 */
#include "test_utils.h"

#define SQ_DEPTH 32

#define TAG_NOP ((void *) 0x1)
#define TAG_RECV ((void *) 0x2)

/* Reap n completions, waiting for each with a bound. */
static void reap(ior_ctx *ctx, unsigned n)
{
	while (n > 0) {
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = 3, .tv_nsec = 0 };
		int ret = ior_wait_cqe_timeout(ctx, &cqe, &to);
		if (ret == -EAGAIN || ret == -EINTR) {
			continue;
		}
		if (ret == -ETIME) {
			fail_msg("no completion within timeout");
		}
		assert_return_code(ret, 0);
		assert_non_null(cqe);
		ior_cqe_seen(ctx, cqe);
		n--;
	}
}

static void stage_nops(ior_ctx *ctx, unsigned n)
{
	for (unsigned i = 0; i < n; i++) {
		ior_sqe *sqe = NULL;
		assert_int_equal(ior_get_sqe_ex(ctx, &sqe), 0);
		assert_non_null(sqe);
		ior_prep_nop(ctx, sqe);
		ior_sqe_set_data(ctx, sqe, TAG_NOP);
	}
}

static void test_sizes_default(void **state)
{
	(void) state;
	ior_ctx *ctx = NULL;
	assert_return_code(ior_queue_init(SQ_DEPTH, &ctx), 0);

	/* Every backend keeps a power-of-two request and gives the CQ twice it. */
	assert_int_equal(ior_sq_entries(ctx), SQ_DEPTH);
	assert_int_equal(ior_cq_entries(ctx), 2 * SQ_DEPTH);
	assert_int_equal(ior_sq_space_left(ctx), SQ_DEPTH);
	assert_int_equal(ior_cq_space_left(ctx), 2 * SQ_DEPTH);

	ior_queue_exit(ctx);
}

static void test_sizes_rounded_and_reported(void **state)
{
	(void) state;
	ior_ctx *ctx = NULL;
	ior_params params = { .sq_entries = 50, .cq_entries = 200 };
	assert_return_code(ior_queue_init_params(1, &ctx, &params), 0);

	/* Rounded up to a power of two, and written back to the params. */
	assert_int_equal(ior_sq_entries(ctx), 64);
	assert_int_equal(ior_cq_entries(ctx), 256);
	assert_int_equal(params.sq_entries, 64);
	assert_int_equal(params.cq_entries, 256);

	ior_queue_exit(ctx);
}

static void test_params_untouched_on_failure(void **state)
{
	(void) state;
	ior_ctx *ctx = NULL;
	ior_params params = { .backend = (ior_backend_type) 99 };
	assert_int_equal(ior_queue_init_params(SQ_DEPTH, &ctx, &params), -ENOSYS);
	assert_null(ctx);
	assert_int_equal(params.sq_entries, 0);
	assert_int_equal(params.cq_entries, 0);
	assert_int_equal(params.features, 0);
}

static void test_null_ctx(void **state)
{
	(void) state;
	ior_sqe *sqe = (ior_sqe *) 0x1;
	assert_int_equal(ior_get_sqe_ex(NULL, &sqe), -EINVAL);
	assert_null(sqe);
	assert_int_equal(ior_sq_entries(NULL), 0);
	assert_int_equal(ior_cq_entries(NULL), 0);
	assert_int_equal(ior_sq_space_left(NULL), 0);
	assert_int_equal(ior_cq_space_left(NULL), 0);
}

static void test_sq_full_wants_submit(void **state)
{
	(void) state;
	ior_ctx *ctx = NULL;
	assert_return_code(ior_queue_init(SQ_DEPTH, &ctx), 0);

	/* Staging counts down the free entries and holds a completion slot each. */
	for (unsigned i = 0; i < SQ_DEPTH; i++) {
		assert_int_equal(ior_sq_space_left(ctx), SQ_DEPTH - i);
		stage_nops(ctx, 1);
	}
	assert_int_equal(ior_sq_space_left(ctx), 0);
	assert_true(ior_cq_space_left(ctx) >= SQ_DEPTH);

	/* The completion queue has room, so this is a full submission queue. */
	ior_sqe *sqe = (ior_sqe *) 0x1;
	assert_int_equal(ior_get_sqe_ex(ctx, &sqe), -ENOSPC);
	assert_null(sqe);
	assert_null(ior_get_sqe(ctx));

	/* A submit frees the staging entries, whatever is still in flight. */
	assert_int_equal(ior_submit(ctx), SQ_DEPTH);
	assert_int_equal(ior_sq_space_left(ctx), SQ_DEPTH);
	assert_int_equal(ior_get_sqe_ex(ctx, &sqe), 0);
	assert_non_null(sqe);
	ior_prep_nop(ctx, sqe);
	assert_int_equal(ior_submit(ctx), 1);

	reap(ctx, SQ_DEPTH + 1);
	assert_int_equal(ior_cq_space_left(ctx), 2 * SQ_DEPTH);

	ior_queue_exit(ctx);
}

/*
 * A completion queue full of unreaped completions refuses nothing, as on
 * io_uring: the entries go through, their completions overflow and wait
 * until reaping makes room, and none is lost.
 */
static void test_cq_full_overflows(void **state)
{
	(void) state;
	ior_ctx *ctx = NULL;
	ior_params params = { .sq_entries = SQ_DEPTH, .cq_entries = SQ_DEPTH };
	assert_return_code(ior_queue_init_params(SQ_DEPTH, &ctx, &params), 0);
	assert_int_equal(ior_cq_entries(ctx), SQ_DEPTH);

	stage_nops(ctx, SQ_DEPTH);
	assert_int_equal(ior_submit_and_wait(ctx, SQ_DEPTH), SQ_DEPTH);
	assert_int_equal(ior_cq_space_left(ctx), 0);

	// Still takes entries: their completions have to wait for room.
	stage_nops(ctx, SQ_DEPTH);
	assert_int_equal(ior_submit(ctx), SQ_DEPTH);
	assert_int_equal(ior_cq_space_left(ctx), 0);

	reap(ctx, 2 * SQ_DEPTH);
	ior_cqe *cqe = NULL;
	assert_int_equal(ior_peek_cqe(ctx, &cqe), -EAGAIN);
	assert_int_equal(ior_cq_space_left(ctx), SQ_DEPTH);

	ior_queue_exit(ctx);
}

/* Completions that overflow keep the order they were posted in. */
static void test_overflow_keeps_order(void **state)
{
	(void) state;
	enum { N = 2 * SQ_DEPTH };
	ior_ctx *ctx = NULL;
	ior_params params = { .sq_entries = SQ_DEPTH, .cq_entries = SQ_DEPTH };
	assert_return_code(ior_queue_init_params(SQ_DEPTH, &ctx, &params), 0);

	// Timeouts a millisecond apart complete in deadline order.
	ior_timespec ts[N];
	for (unsigned i = 0; i < N; i++) {
		ior_sqe *sqe = ior_get_sqe(ctx);
		if (!sqe) {
			assert_true(ior_submit(ctx) > 0);
			sqe = ior_get_sqe(ctx);
			assert_non_null(sqe);
		}
		ts[i].tv_sec = 0;
		ts[i].tv_nsec = (long long) (i + 1) * 1000000;
		ior_prep_timeout(ctx, sqe, &ts[i], 0, 0);
		ior_sqe_set_data(ctx, sqe, (void *) (uintptr_t) (i + 1));
	}
	assert_true(ior_submit_and_wait(ctx, N) >= 0);

	for (uintptr_t i = 1; i <= N; i++) {
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = 3, .tv_nsec = 0 };
		assert_return_code(ior_wait_cqe_timeout(ctx, &cqe, &to), 0);
		assert_int_equal((uintptr_t) ior_cqe_get_data(ctx, cqe), i);
		assert_int_equal(ior_cqe_get_res(ctx, cqe), -ETIME);
		ior_cqe_seen(ctx, cqe);
	}
	ior_queue_exit(ctx);
}

/*
 * Operations in flight are not bounded by the completion queue, on any
 * backend: three times as many parked receives as it has entries are all
 * taken, leave every slot free while they wait, and all complete.
 */
static void test_in_flight_beyond_cq(void **state)
{
	(void) state;
	enum { N = 3 * SQ_DEPTH };
	ior_ctx *ctx = NULL;
	ior_params params = { .sq_entries = SQ_DEPTH, .cq_entries = SQ_DEPTH };
	assert_return_code(ior_queue_init_params(SQ_DEPTH, &ctx, &params), 0);

	ior_fd_t sock[2];
	assert_return_code(test_make_socketpair(sock), 0);
	char buf[N][8];

	for (unsigned i = 0; i < N; i++) {
		ior_sqe *sqe = NULL;
		int ret = ior_get_sqe_ex(ctx, &sqe);
		if (ret == -ENOSPC) {
			assert_true(ior_submit(ctx) > 0);
			ret = ior_get_sqe_ex(ctx, &sqe);
		}
		assert_int_equal(ret, 0);
		ior_prep_recv(ctx, sqe, sock[1], buf[i], sizeof(buf[i]), 0);
		ior_sqe_set_data(ctx, sqe, TAG_RECV);
	}
	assert_true(ior_submit(ctx) > 0);
	assert_int_equal(ior_cq_space_left(ctx), SQ_DEPTH);

	ior_sqe *sqe = NULL;
	assert_int_equal(ior_get_sqe_ex(ctx, &sqe), 0);
	ior_prep_nop(ctx, sqe);
	ior_sqe_set_data(ctx, sqe, TAG_NOP);
	assert_int_equal(ior_submit(ctx), 1);
	reap(ctx, 1);

	// Closing the peer ends every recv, more than the queue holds at once.
	test_close_fd(sock[0]);
	reap(ctx, N);
	assert_int_equal(ior_cq_space_left(ctx), SQ_DEPTH);

	test_close_fd(sock[1]);
	ior_queue_exit(ctx);
}

/*
 * With the notification descriptor handed out first, completions of more
 * operations than the completion queue holds are all announced and reaped:
 * wherever a backend keeps the ones the queue has no room for, none is lost.
 */
static void test_in_flight_beyond_cq_notify(void **state)
{
	(void) state;
	enum { N = 3 * SQ_DEPTH };
	ior_ctx *ctx = NULL;
	ior_params params = { .sq_entries = SQ_DEPTH, .cq_entries = SQ_DEPTH };
	assert_return_code(ior_queue_init_params(SQ_DEPTH, &ctx, &params), 0);
	ior_fd_t nfd = ior_notify_fd(ctx);
	assert_true(test_fd_is_valid(nfd));

	ior_fd_t sock[2];
	assert_return_code(test_make_socketpair(sock), 0);
	char buf[N][8];
	for (unsigned i = 0; i < N; i++) {
		ior_sqe *sqe = NULL;
		int ret = ior_get_sqe_ex(ctx, &sqe);
		if (ret == -ENOSPC) {
			assert_true(ior_submit(ctx) > 0);
			ret = ior_get_sqe_ex(ctx, &sqe);
		}
		assert_int_equal(ret, 0);
		ior_prep_recv(ctx, sqe, sock[1], buf[i], sizeof(buf[i]), 0);
		ior_sqe_set_data(ctx, sqe, TAG_RECV);
	}
	assert_true(ior_submit(ctx) > 0);

	test_close_fd(sock[0]);
	unsigned got = 0;
	while (got < N) {
		assert_int_equal(test_wait_readable(nfd, 3000), 1);
		assert_return_code(ior_notify_clear(ctx), 0);
		ior_cqe *cqe = NULL;
		while (got < N && ior_peek_cqe(ctx, &cqe) == 0) {
			assert_ptr_equal(ior_cqe_get_data(ctx, cqe), TAG_RECV);
			ior_cqe_seen(ctx, cqe);
			got++;
		}
	}

	test_close_fd(sock[1]);
	ior_queue_exit(ctx);
}

/*
 * A cancel finds its target however many operations are in flight beside
 * it, beyond what the completion queue holds: each of them, by its own
 * user_data, the oldest and the newest alike.
 */
static void test_cancel_beyond_cq(void **state)
{
	(void) state;
	enum { N = 3 * SQ_DEPTH };
	ior_ctx *ctx = NULL;
	ior_params params = { .sq_entries = SQ_DEPTH, .cq_entries = SQ_DEPTH };
	assert_return_code(ior_queue_init_params(SQ_DEPTH, &ctx, &params), 0);

	ior_timespec ts = { .tv_sec = 30, .tv_nsec = 0 };
	for (uintptr_t i = 0; i < 2 * N; i++) {
		ior_sqe *sqe = NULL;
		int ret = ior_get_sqe_ex(ctx, &sqe);
		if (ret == -ENOSPC) {
			assert_true(ior_submit(ctx) > 0);
			ret = ior_get_sqe_ex(ctx, &sqe);
		}
		assert_int_equal(ret, 0);
		if (i < N) {
			ior_prep_timeout(ctx, sqe, &ts, 0, 0);
			ior_sqe_set_data(ctx, sqe, (void *) (0x1000 + i));
		} else {
			// Newest first, so the scan meets the target last among the rest.
			ior_prep_cancel(ctx, sqe, (void *) (0x1000 + (2 * N - 1 - i)));
			ior_sqe_set_data(ctx, sqe, (void *) (0x2000 + (2 * N - 1 - i)));
		}
	}
	assert_true(ior_submit(ctx) > 0);

	int32_t timeout_res[N];
	int32_t cancel_res[N];
	for (unsigned i = 0; i < N; i++) {
		timeout_res[i] = 1;
		cancel_res[i] = 1;
	}
	for (unsigned n = 0; n < 2 * N; n++) {
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = 3, .tv_nsec = 0 };
		assert_return_code(ior_wait_cqe_timeout(ctx, &cqe, &to), 0);
		uintptr_t data = (uintptr_t) ior_cqe_get_data(ctx, cqe);
		if (data >= 0x2000) {
			cancel_res[data - 0x2000] = ior_cqe_get_res(ctx, cqe);
		} else {
			timeout_res[data - 0x1000] = ior_cqe_get_res(ctx, cqe);
		}
		ior_cqe_seen(ctx, cqe);
	}
	for (unsigned i = 0; i < N; i++) {
		assert_int_equal(cancel_res[i], 0);
		assert_int_equal(timeout_res[i], -ECANCELED);
	}
	ior_queue_exit(ctx);
}

/*
 * IO_DRAIN waits for every earlier operation, however many completed around
 * a long-lived one meanwhile: far more than the queue holds.
 */
static void test_drain_behind_long_op(void **state)
{
	(void) state;
	enum { N = 200 };
	ior_ctx *ctx = NULL;
	ior_params params = { .sq_entries = SQ_DEPTH, .cq_entries = SQ_DEPTH };
	assert_return_code(ior_queue_init_params(SQ_DEPTH, &ctx, &params), 0);

	ior_fd_t sock[2];
	assert_return_code(test_make_socketpair(sock), 0);
	char buf[8];
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_recv(ctx, sqe, sock[1], buf, sizeof(buf), 0);
	ior_sqe_set_data(ctx, sqe, TAG_RECV);
	assert_int_equal(ior_submit(ctx), 1);

	for (unsigned done = 0; done < N; done += 8) {
		stage_nops(ctx, 8);
		assert_int_equal(ior_submit(ctx), 8);
		reap(ctx, 8);
	}

	sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_nop(ctx, sqe);
	ior_sqe_set_data(ctx, sqe, TAG_NOP);
	ior_sqe_set_flags(ctx, sqe, IOR_SQE_IO_DRAIN);
	assert_int_equal(ior_submit(ctx), 1);

	ior_cqe *cqe = NULL;
	ior_timespec to = { .tv_sec = 0, .tv_nsec = 100000000 };
	assert_int_equal(ior_wait_cqe_timeout(ctx, &cqe, &to), -ETIME);

	test_close_fd(sock[0]);
	for (int i = 0; i < 2; i++) {
		ior_timespec wait = { .tv_sec = 3, .tv_nsec = 0 };
		assert_return_code(ior_wait_cqe_timeout(ctx, &cqe, &wait), 0);
		// The recv first: the drained nop waited for it.
		assert_ptr_equal(ior_cqe_get_data(ctx, cqe), i == 0 ? TAG_RECV : TAG_NOP);
		ior_cqe_seen(ctx, cqe);
	}

	test_close_fd(sock[1]);
	ior_queue_exit(ctx);
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test_sizes_default),
		cmocka_unit_test(test_sizes_rounded_and_reported),
		cmocka_unit_test(test_params_untouched_on_failure),
		cmocka_unit_test(test_null_ctx),
		cmocka_unit_test(test_sq_full_wants_submit),
		cmocka_unit_test(test_cq_full_overflows),
		cmocka_unit_test(test_overflow_keeps_order),
		cmocka_unit_test(test_in_flight_beyond_cq),
		cmocka_unit_test(test_in_flight_beyond_cq_notify),
		cmocka_unit_test(test_cancel_beyond_cq),
		cmocka_unit_test(test_drain_behind_long_op),
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
