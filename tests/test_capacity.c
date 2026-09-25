/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_capacity.c - the effective queue sizes, the free-entry counts and the
 * reason ior_get_sqe() refuses an entry, which must be told apart: a full
 * submission queue wants a submit, a full completion queue wants a reap.
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

static void test_cq_full_wants_reap(void **state)
{
	(void) state;
	ior_ctx *ctx = NULL;
	ior_params params = { .sq_entries = SQ_DEPTH, .cq_entries = SQ_DEPTH };
	assert_return_code(ior_queue_init_params(SQ_DEPTH, &ctx, &params), 0);
	assert_int_equal(ior_cq_entries(ctx), SQ_DEPTH);

	/* Fill the completion queue with unreaped completions. */
	stage_nops(ctx, SQ_DEPTH);
	assert_int_equal(ior_submit_and_wait(ctx, SQ_DEPTH), SQ_DEPTH);
	ior_cqe *cqes[SQ_DEPTH];
	assert_int_equal(ior_peek_batch_cqe(ctx, cqes, SQ_DEPTH), SQ_DEPTH);
	assert_int_equal(ior_cq_space_left(ctx), 0);
	assert_int_equal(ior_sq_space_left(ctx), SQ_DEPTH);

	/* The submission queue is empty, so this is a full completion queue. */
	ior_sqe *sqe = (ior_sqe *) 0x1;
	assert_int_equal(ior_get_sqe_ex(ctx, &sqe), -EBUSY);
	assert_null(sqe);
	assert_null(ior_get_sqe(ctx));

	/* Reaping one frees exactly one slot. */
	ior_cq_advance(ctx, 1);
	assert_int_equal(ior_cq_space_left(ctx), 1);
	assert_int_equal(ior_get_sqe_ex(ctx, &sqe), 0);
	assert_non_null(sqe);
	ior_prep_nop(ctx, sqe);
	ior_sqe_set_data(ctx, sqe, TAG_NOP);
	assert_int_equal(ior_submit(ctx), 1);

	reap(ctx, SQ_DEPTH);
	assert_int_equal(ior_cq_space_left(ctx), SQ_DEPTH);

	ior_queue_exit(ctx);
}

/*
 * Whether an operation in flight takes a completion slot is the documented
 * backend difference: it does on the thread and IOCP backends, which never
 * post into a full queue, and not on io_uring, where the kernel buffers an
 * overflow and only unreaped completions count.
 */
static void test_in_flight_slots(void **state)
{
	(void) state;
	ior_ctx *ctx = NULL;
	ior_params params = { .sq_entries = SQ_DEPTH, .cq_entries = SQ_DEPTH };
	assert_return_code(ior_queue_init_params(SQ_DEPTH, &ctx, &params), 0);

	ior_fd_t sock[2];
	assert_return_code(test_make_socketpair(sock), 0);
	char buf[8];

	/* Park as many recvs as there are completion slots. */
	for (unsigned i = 0; i < SQ_DEPTH; i++) {
		ior_sqe *sqe = NULL;
		assert_int_equal(ior_get_sqe_ex(ctx, &sqe), 0);
		ior_prep_recv(ctx, sqe, sock[1], buf, sizeof(buf), 0);
		ior_sqe_set_data(ctx, sqe, TAG_RECV);
	}
	assert_int_equal(ior_submit(ctx), SQ_DEPTH);

	int uring = ior_get_backend_type(ctx) == IOR_BACKEND_IOURING;
	ior_sqe *sqe = NULL;
	unsigned expected = SQ_DEPTH;
	if (uring) {
		assert_int_equal(ior_cq_space_left(ctx), SQ_DEPTH);
		assert_int_equal(ior_get_sqe_ex(ctx, &sqe), 0);
		assert_non_null(sqe);
		ior_prep_nop(ctx, sqe);
		ior_sqe_set_data(ctx, sqe, TAG_NOP);
		assert_int_equal(ior_submit(ctx), 1);
		expected++;
	} else {
		assert_int_equal(ior_cq_space_left(ctx), 0);
		assert_int_equal(ior_get_sqe_ex(ctx, &sqe), -EBUSY);
		assert_null(sqe);
	}

	/* Closing the peer ends every recv; reaping gives the slots back. */
	test_close_fd(sock[0]);
	reap(ctx, expected);
	assert_int_equal(ior_cq_space_left(ctx), SQ_DEPTH);
	assert_int_equal(ior_get_sqe_ex(ctx, &sqe), 0);
	assert_non_null(sqe);

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
		cmocka_unit_test(test_cq_full_wants_reap),
		cmocka_unit_test(test_in_flight_slots),
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
