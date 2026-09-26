/* SPDX-License-Identifier: BSD-3-Clause */
#include "test_utils.h"
#include <stdatomic.h>

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

/*
 * Submit semantics follow io_uring: an entry the kernel refuses to take (bad
 * timespec, two clocks, a link timeout with nothing to guard) fails its whole
 * chain, its own error on it and -ECANCELED on the rest, and submission stops
 * right after it unless it links on; what follows stays staged.
 */

static ior_timespec bad_ts = { .tv_sec = -1, .tv_nsec = 0 };

static void stage_nop(ior_ctx *ctx, uintptr_t id, uint8_t flags)
{
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_nop(ctx, sqe);
	ior_sqe_set_data(ctx, sqe, (void *) id);
	ior_sqe_set_flags(ctx, sqe, flags);
}

static void stage_timeout(
		ior_ctx *ctx, uintptr_t id, ior_timespec *ts, unsigned tflags, uint8_t flags)
{
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_timeout(ctx, sqe, ts, 0, tflags);
	ior_sqe_set_data(ctx, sqe, (void *) id);
	ior_sqe_set_flags(ctx, sqe, flags);
}

static void stage_link_timeout(ior_ctx *ctx, uintptr_t id, ior_timespec *ts, uint8_t flags)
{
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_link_timeout(ctx, sqe, ts, 0);
	ior_sqe_set_data(ctx, sqe, (void *) id);
	ior_sqe_set_flags(ctx, sqe, flags);
}

/* Reap n completions into res[id] (ids 1..8), then check none is left. */
static void reap_ids(ior_ctx *ctx, unsigned n, int32_t res[9])
{
	for (unsigned i = 0; i < 9; i++) {
		res[i] = 1; // not seen
	}
	for (unsigned i = 0; i < n; i++) {
		ior_cqe *cqe;
		assert_return_code(ior_wait_cqe(ctx, &cqe), 0);
		uintptr_t id = (uintptr_t) ior_cqe_get_data(ctx, cqe);
		assert_true(id >= 1 && id <= 8);
		assert_int_equal(res[id], 1);
		res[id] = ior_cqe_get_res(ctx, cqe);
		ior_cqe_seen(ctx, cqe);
	}
	ior_cqe *cqe;
	assert_int_equal(ior_peek_cqe(ctx, &cqe), -EAGAIN);
}

static void test_submit_stops_at_invalid_entry(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];

	stage_nop(ts->ctx, 1, 0);
	stage_timeout(ts->ctx, 2, &bad_ts, 0, 0);
	stage_nop(ts->ctx, 3, 0);

	assert_int_equal(ior_submit(ts->ctx), 2);
	reap_ids(ts->ctx, 2, res);
	assert_int_equal(res[1], 0);
	assert_int_equal(res[2], -EINVAL);

	// The entry after it was left staged: the next submit sends it.
	assert_int_equal(ior_submit(ts->ctx), 1);
	reap_ids(ts->ctx, 1, res);
	assert_int_equal(res[3], 0);
}

static void test_invalid_entry_fails_chain(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];

	stage_nop(ts->ctx, 1, IOR_SQE_IO_LINK);
	stage_timeout(ts->ctx, 2, &bad_ts, 0, 0);
	stage_nop(ts->ctx, 3, 0);

	assert_int_equal(ior_submit(ts->ctx), 2);
	reap_ids(ts->ctx, 2, res);
	assert_int_equal(res[1], -ECANCELED);
	assert_int_equal(res[2], -EINVAL);

	assert_int_equal(ior_submit(ts->ctx), 1);
	reap_ids(ts->ctx, 1, res);
	assert_int_equal(res[3], 0);
}

// An invalid entry that links on fails its chain but does not stop submission.
static void test_invalid_linked_entry_goes_on(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];

	stage_nop(ts->ctx, 1, IOR_SQE_IO_LINK);
	stage_timeout(ts->ctx, 2, &bad_ts, 0, IOR_SQE_IO_LINK);
	stage_nop(ts->ctx, 3, 0);
	stage_nop(ts->ctx, 4, 0);

	assert_int_equal(ior_submit(ts->ctx), 4);
	reap_ids(ts->ctx, 4, res);
	assert_int_equal(res[1], -ECANCELED);
	assert_int_equal(res[2], -EINVAL);
	assert_int_equal(res[3], -ECANCELED);
	assert_int_equal(res[4], 0);
}

static void test_timeout_checks(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];
	ior_timespec ok = { .tv_sec = 0, .tv_nsec = 1000000 };

	stage_timeout(ts->ctx, 1, NULL, 0, 0);
	assert_int_equal(ior_submit(ts->ctx), 1);
	reap_ids(ts->ctx, 1, res);
	assert_int_equal(res[1], -EFAULT);

	stage_timeout(ts->ctx, 2, &ok, IOR_TIMEOUT_BOOTTIME | IOR_TIMEOUT_REALTIME, 0);
	assert_int_equal(ior_submit(ts->ctx), 1);
	reap_ids(ts->ctx, 1, res);
	assert_int_equal(res[2], -EINVAL);
}

// A tv_nsec past a second is a longer timeout, not an invalid one.
static void test_timeout_nsec_past_second(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];
	ior_timespec longer = { .tv_sec = 0, .tv_nsec = 1500000000LL };

	stage_timeout(ts->ctx, 1, &longer, 0, 0);
	assert_int_equal(ior_submit(ts->ctx), 1);

	ior_sqe *sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);
	ior_prep_cancel(ts->ctx, sqe, (void *) 1);
	ior_sqe_set_data(ts->ctx, sqe, (void *) 2);
	assert_int_equal(ior_submit(ts->ctx), 1);

	reap_ids(ts->ctx, 2, res);
	assert_int_equal(res[1], -ECANCELED);
	assert_int_equal(res[2], 0);
}

static void test_link_timeout_without_head(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];
	ior_timespec ok = { .tv_sec = 1, .tv_nsec = 0 };

	stage_link_timeout(ts->ctx, 1, &ok, 0);
	stage_nop(ts->ctx, 2, 0);
	assert_int_equal(ior_submit(ts->ctx), 1);
	reap_ids(ts->ctx, 1, res);
	assert_int_equal(res[1], -EINVAL);

	assert_int_equal(ior_submit(ts->ctx), 1);
	reap_ids(ts->ctx, 1, res);
	assert_int_equal(res[2], 0);

	// Nor may a link timeout follow another one.
	stage_nop(ts->ctx, 3, IOR_SQE_IO_LINK);
	stage_link_timeout(ts->ctx, 4, &ok, IOR_SQE_IO_LINK);
	stage_link_timeout(ts->ctx, 5, &ok, 0);
	assert_int_equal(ior_submit(ts->ctx), 3);
	reap_ids(ts->ctx, 3, res);
	assert_int_equal(res[3], -ECANCELED);
	assert_int_equal(res[4], -ECANCELED);
	assert_int_equal(res[5], -EINVAL);
}

// With entries left staged, submit_and_wait returns without waiting.
static void test_submit_and_wait_stops_short(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];

	stage_nop(ts->ctx, 1, 0);
	stage_timeout(ts->ctx, 2, &bad_ts, 0, 0);
	stage_nop(ts->ctx, 3, 0);

	assert_int_equal(ior_submit_and_wait(ts->ctx, 3), 2);
	reap_ids(ts->ctx, 2, res);
	assert_int_equal(ior_submit_and_wait(ts->ctx, 1), 1);
	reap_ids(ts->ctx, 1, res);
	assert_int_equal(res[3], 0);
}

static _Atomic int work_ran;

static int32_t mark_work(ior_work_token *token, void *arg)
{
	(void) token;
	(void) arg;
	atomic_store(&work_ran, 1);
	return 0;
}

// A work op in a failed chain is cancelled, never run.
static void test_invalid_link_timeout_on_work(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];

	atomic_store(&work_ran, 0);
	ior_sqe *sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);
	assert_return_code(ior_prep_work(ts->ctx, sqe, mark_work, NULL), 0);
	ior_sqe_set_data(ts->ctx, sqe, (void *) 1);
	ior_sqe_set_flags(ts->ctx, sqe, IOR_SQE_IO_LINK);
	stage_link_timeout(ts->ctx, 2, &bad_ts, 0);

	assert_int_equal(ior_submit(ts->ctx), 2);
	reap_ids(ts->ctx, 2, res);
	assert_int_equal(res[1], -ECANCELED);
	assert_int_equal(res[2], -EINVAL);
	assert_int_equal(atomic_load(&work_ran), 0);
}

// Accept flags beyond NONBLOCK and CLOEXEC are refused as io_uring does.
static void test_accept_bad_flags(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];

	ior_sqe *sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);
	ior_prep_accept(ts->ctx, sqe, IOR_INVALID_FD, NULL, NULL, 1U << 30);
	ior_sqe_set_data(ts->ctx, sqe, (void *) 1);
	stage_nop(ts->ctx, 2, 0);

	assert_int_equal(ior_submit(ts->ctx), 1);
	reap_ids(ts->ctx, 1, res);
	assert_int_equal(res[1], -EINVAL);
	assert_int_equal(ior_submit(ts->ctx), 1);
	reap_ids(ts->ctx, 1, res);
	assert_int_equal(res[2], 0);
}

/*
 * A wait timeout reads as on io_uring: a negative one has already expired
 * (-ETIME, or a completion that is ready), a tv_nsec past a second adds up.
 */
static void test_wait_timeout_like_uring(void **state)
{
	test_state *ts = (test_state *) *state;
	ior_timespec neg_sec = { .tv_sec = -1, .tv_nsec = 0 };
	ior_timespec neg_nsec = { .tv_sec = 0, .tv_nsec = -5 };
	ior_timespec longer = { .tv_sec = 0, .tv_nsec = 1050000000LL };
	ior_cqe *cqe;

	uint64_t start = test_monotonic_now_ns();
	assert_int_equal(ior_wait_cqe_timeout(ts->ctx, &cqe, &neg_sec), -ETIME);
	assert_int_equal(ior_wait_cqe_timeout(ts->ctx, &cqe, &neg_nsec), -ETIME);
	assert_true(test_monotonic_now_ns() - start < 500000000ULL);

	start = test_monotonic_now_ns();
	assert_int_equal(ior_wait_cqe_timeout(ts->ctx, &cqe, &longer), -ETIME);
	assert_true(test_monotonic_now_ns() - start >= 1000000000ULL);

	stage_nop(ts->ctx, 1, 0);
	assert_int_equal(ior_submit_and_wait(ts->ctx, 1), 1);
	assert_int_equal(ior_wait_cqe_timeout(ts->ctx, &cqe, &neg_sec), 0);
	ior_cqe_seen(ts->ctx, cqe);
}

/*
 * ior registers no files, so IOR_SQE_FIXED_FILE fails an op that takes a
 * descriptor with -EBADF when it runs (breaking its link), as on io_uring;
 * an op without one ignores the flag.
 */
static void test_fixed_file_is_ebadf(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];
	ior_fd_t sv[2];
	char buf[8];
	assert_int_equal(test_make_socketpair(sv), 0);

	stage_nop(ts->ctx, 1, IOR_SQE_FIXED_FILE);

	ior_sqe *sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);
	ior_prep_recv(ts->ctx, sqe, sv[0], buf, sizeof(buf), 0);
	ior_sqe_set_data(ts->ctx, sqe, (void *) 2);
	ior_sqe_set_flags(ts->ctx, sqe, IOR_SQE_FIXED_FILE | IOR_SQE_IO_LINK);
	stage_nop(ts->ctx, 3, 0);

	sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);
	ior_prep_poll_add(ts->ctx, sqe, sv[0], IOR_POLL_IN);
	ior_sqe_set_data(ts->ctx, sqe, (void *) 4);
	ior_sqe_set_flags(ts->ctx, sqe, IOR_SQE_FIXED_FILE);

	sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);
	ior_prep_cancel_fd(ts->ctx, sqe, sv[0]);
	ior_sqe_set_data(ts->ctx, sqe, (void *) 5);
	ior_sqe_set_flags(ts->ctx, sqe, IOR_SQE_FIXED_FILE);

	assert_int_equal(ior_submit(ts->ctx), 5);
	reap_ids(ts->ctx, 5, res);
	assert_int_equal(res[1], 0);
	assert_int_equal(res[2], -EBADF);
	assert_int_equal(res[3], -ECANCELED);
	assert_int_equal(res[4], -EBADF);
	assert_int_equal(res[5], -EBADF);

	test_close_fd(sv[0]);
	test_close_fd(sv[1]);
}

// An op done at once still gets its link timeout resolved, as cancelled.
static void test_link_timeout_on_nop(void **state)
{
	test_state *ts = (test_state *) *state;
	int32_t res[9];
	ior_timespec ok = { .tv_sec = 5, .tv_nsec = 0 };

	stage_nop(ts->ctx, 1, IOR_SQE_IO_LINK);
	stage_link_timeout(ts->ctx, 2, &ok, 0);
	assert_int_equal(ior_submit(ts->ctx), 2);
	reap_ids(ts->ctx, 2, res);
	assert_int_equal(res[1], 0);
	assert_int_equal(res[2], -ECANCELED);
}

// Waiting for a completion does not send staged entries.
static void test_wait_does_not_submit(void **state)
{
	test_state *ts = (test_state *) *state;
	ior_timespec wait = { .tv_sec = 0, .tv_nsec = 50000000 };

	ior_sqe *sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);
	ior_prep_nop(ts->ctx, sqe);

	ior_cqe *cqe;
	assert_int_equal(ior_peek_cqe(ts->ctx, &cqe), -EAGAIN);
	assert_int_equal(ior_wait_cqe_timeout(ts->ctx, &cqe, &wait), -ETIME);

	assert_int_equal(ior_submit(ts->ctx), 1);
	assert_return_code(ior_wait_cqe(ts->ctx, &cqe), 0);
	assert_int_equal(ior_cqe_get_res(ts->ctx, cqe), 0);
	ior_cqe_seen(ts->ctx, cqe);
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
		cmocka_unit_test_setup_teardown(
				test_submit_stops_at_invalid_entry, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(
				test_invalid_entry_fails_chain, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(
				test_invalid_linked_entry_goes_on, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(test_timeout_checks, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(
				test_timeout_nsec_past_second, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(
				test_link_timeout_without_head, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(
				test_submit_and_wait_stops_short, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(
				test_invalid_link_timeout_on_work, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(test_accept_bad_flags, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(test_fixed_file_is_ebadf, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(test_link_timeout_on_nop, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(
				test_wait_timeout_like_uring, setup_ior_ctx, teardown_ior_ctx),
		cmocka_unit_test_setup_teardown(test_wait_does_not_submit, setup_ior_ctx, teardown_ior_ctx),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
