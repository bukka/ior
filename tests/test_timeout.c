/* SPDX-License-Identifier: BSD-3-Clause */
#include "test_utils.h"

static void test_timeout_basic(void **state)
{
	test_state *ts = (test_state *) *state;

	ior_timespec timeout = {
		.tv_sec = 0,
		.tv_nsec = 100000000 // 100ms
	};

	ior_sqe *sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);

	ior_prep_timeout(ts->ctx, sqe, &timeout, 0, 0);
	ior_sqe_set_data(ts->ctx, sqe, NULL);

	int ret = ior_submit_and_wait(ts->ctx, 1);
	assert_true(ret >= 0);

	ior_cqe *cqe;
	ret = ior_wait_cqe(ts->ctx, &cqe);
	assert_return_code(ret, 0);

	int32_t res = ior_cqe_get_res(ts->ctx, cqe);
	assert_true(res == -ETIME || res == -ETIMEDOUT);

	ior_cqe_seen(ts->ctx, cqe);
}

static void test_wait_cqe_timeout(void **state)
{
	test_state *ts = (test_state *) *state;

	ior_timespec timeout = {
		.tv_sec = 0,
		.tv_nsec = 100000000 // 100ms
	};

	// Submit a timeout operation that will complete after (takes 2s) the wait timeout
	ior_timespec long_timeout = { .tv_sec = 2, .tv_nsec = 0 };

	ior_sqe *sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);

	ior_prep_timeout(ts->ctx, sqe, &long_timeout, 0, 0);
	ior_sqe_set_data(ts->ctx, sqe, NULL);

	int ret = ior_submit(ts->ctx);
	assert_true(ret >= 0);

	// Now wait with short timeout - should timeout before the operation completes
	ior_cqe *cqe;
	ret = ior_wait_cqe_timeout(ts->ctx, &cqe, &timeout);

	// The 2s operation outlasts the 100ms wait, so the wait must time out with
	// the canonical -ETIME (same on every backend).
	assert_int_equal(ret, -ETIME);

	// Clean up - the timeout operation will still complete eventually
	// We can just exit the context to cancel it
}

/*
 * Absolute timeout: an IOR_TIMEOUT_ABS timeout whose deadline is now + 100ms (in
 * the backend's monotonic clock) must fire with -ETIME. This also discriminates
 * abs from relative handling: the absolute deadline is ~uptime seconds, so if it
 * were (mis)treated as a relative duration the wait would far exceed the test
 * timeout instead of completing in ~100ms.
 */
static void test_timeout_abs(void **state)
{
	test_state *ts = (test_state *) *state;

	uint64_t deadline = test_monotonic_now_ns() + 100000000ULL; /* now + 100ms */
	ior_timespec abs = {
		.tv_sec = (int64_t) (deadline / 1000000000ULL),
		.tv_nsec = (long long) (deadline % 1000000000ULL),
	};

	ior_sqe *sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);
	ior_prep_timeout(ts->ctx, sqe, &abs, 0, IOR_TIMEOUT_ABS);
	ior_sqe_set_data(ts->ctx, sqe, NULL);

	int ret = ior_submit_and_wait(ts->ctx, 1);
	assert_true(ret >= 0);

	ior_cqe *cqe;
	ret = ior_wait_cqe(ts->ctx, &cqe);
	assert_return_code(ret, 0);
	int32_t res = ior_cqe_get_res(ts->ctx, cqe);
	assert_true(res == -ETIME || res == -ETIMEDOUT);
	ior_cqe_seen(ts->ctx, cqe);
}

/*
 * Submit a timeout whose timespec lives in this frame: the promise is that
 * ior_submit() has read it by the time it returns, on every backend. With
 * drain_ms > 0 a plain timeout of that length goes first and the timeout
 * under test waits behind an IO_DRAIN barrier, so a backend that reads the
 * timespec when it arms the timer does so long after this frame is gone.
 */
static void submit_timeout_from_scope(ior_ctx *ctx, int64_t ns, unsigned flags, int drain_ms)
{
	ior_timespec first = { .tv_sec = 0, .tv_nsec = (long long) drain_ms * 1000000LL };
	ior_timespec ts = {
		.tv_sec = ns / 1000000000LL,
		.tv_nsec = ns % 1000000000LL,
	};
	if (drain_ms > 0) {
		ior_sqe *sqe = ior_get_sqe(ctx);
		assert_non_null(sqe);
		ior_prep_timeout(ctx, sqe, &first, 0, 0);
		ior_sqe_set_data(ctx, sqe, NULL);
	}
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_timeout(ctx, sqe, &ts, 0, flags);
	ior_sqe_set_data(ctx, sqe, NULL);
	if (drain_ms > 0) {
		ior_sqe_set_flags(ctx, sqe, IOR_SQE_IO_DRAIN);
	}
	assert_true(ior_submit(ctx) >= 0);
}

/* Overwrite the stack the frame above used, so a late read finds garbage. */
static void clobber_stack(void)
{
	volatile unsigned char junk[8192];
	for (size_t i = 0; i < sizeof(junk); i++) {
		junk[i] = 0x55;
	}
}

/* Reap the one timeout and return how long it took, in ms. */
static uint64_t reap_timeout_ms(ior_ctx *ctx, uint64_t start_ns)
{
	ior_cqe *cqe;
	assert_return_code(ior_wait_cqe(ctx, &cqe), 0);
	int32_t res = ior_cqe_get_res(ctx, cqe);
	assert_true(res == -ETIME || res == -ETIMEDOUT);
	ior_cqe_seen(ctx, cqe);
	return (test_monotonic_now_ns() - start_ns) / 1000000ULL;
}

/*
 * The timespec may go out of scope once ior_submit() returns. The thread
 * backend used to read it when the timer was armed, on a worker, after
 * submit returned: held behind a drain barrier that read the overwritten
 * frame and the timeout was rejected or fired at once. Here it must fire
 * after the barrier's 150 ms plus its own 100 ms.
 */
static void test_timeout_ts_out_of_scope(void **state)
{
	test_state *ts = (test_state *) *state;

	uint64_t start = test_monotonic_now_ns();
	submit_timeout_from_scope(ts->ctx, 100000000LL, 0, 150);
	clobber_stack();

	uint64_t first_ms = reap_timeout_ms(ts->ctx, start);
	assert_true(first_ms >= 100 && first_ms < 2000);
	uint64_t ms = reap_timeout_ms(ts->ctx, start);
	assert_true(ms >= 200 && ms < 3000);
}

/* An absolute deadline on the wall clock (IOR_TIMEOUT_REALTIME). */
static void test_timeout_abs_realtime(void **state)
{
	test_state *ts = (test_state *) *state;

	uint64_t start = test_monotonic_now_ns();
	submit_timeout_from_scope(ts->ctx, (int64_t) (test_realtime_now_ns() + 100000000ULL),
			IOR_TIMEOUT_ABS | IOR_TIMEOUT_REALTIME, 0);

	uint64_t ms = reap_timeout_ms(ts->ctx, start);
	assert_true(ms >= 50 && ms < 2000);
}

/* A wall-clock deadline already in the past fires at once. */
static void test_timeout_abs_realtime_past(void **state)
{
	test_state *ts = (test_state *) *state;

	uint64_t start = test_monotonic_now_ns();
	submit_timeout_from_scope(ts->ctx, (int64_t) (test_realtime_now_ns() - 1000000000ULL),
			IOR_TIMEOUT_ABS | IOR_TIMEOUT_REALTIME, 0);

	uint64_t ms = reap_timeout_ms(ts->ctx, start);
	assert_true(ms < 500);
}

/* An absolute deadline on the boot-time clock (IOR_TIMEOUT_BOOTTIME). */
static void test_timeout_abs_boottime(void **state)
{
	test_state *ts = (test_state *) *state;

	uint64_t start = test_monotonic_now_ns();
	submit_timeout_from_scope(ts->ctx, (int64_t) (test_boottime_now_ns() + 100000000ULL),
			IOR_TIMEOUT_ABS | IOR_TIMEOUT_BOOTTIME, 0);

	uint64_t ms = reap_timeout_ms(ts->ctx, start);
	assert_true(ms >= 50 && ms < 2000);
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test_setup_teardown(test_timeout_basic, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(test_wait_cqe_timeout, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(test_timeout_abs, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(
				test_timeout_ts_out_of_scope, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(test_timeout_abs_realtime, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(
				test_timeout_abs_realtime_past, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(test_timeout_abs_boottime, setup_ior_ctx, teardown_ior_ctx),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
