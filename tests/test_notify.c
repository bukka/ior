/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_notify.c - ior_notify_fd() / ior_notify_clear() on every backend.
 *
 * The descriptor must become readable once a completion is posted, whatever
 * thread posts it (the submitter inline, a worker, the timer thread, the
 * kernel), stay readable until cleared, and not be readable while nothing has
 * completed.
 */
#include "test_utils.h"

typedef struct notify_state {
	ior_ctx *ctx;
	ior_fd_t sock[2];
	ior_fd_t nfd;
} notify_state;

static int setup_notify(void **state)
{
	notify_state *s = calloc(1, sizeof(*s));
	assert_non_null(s);
	assert_return_code(ior_queue_init(32, &s->ctx), 0);
	assert_return_code(test_make_socketpair(s->sock), 0);
	s->nfd = ior_notify_fd(s->ctx);
	assert_true(test_fd_is_valid(s->nfd));
	*state = s;
	return 0;
}

static int teardown_notify(void **state)
{
	notify_state *s = (notify_state *) *state;
	if (s) {
		test_close_fd(s->sock[0]);
		test_close_fd(s->sock[1]);
		ior_queue_exit(s->ctx);
		free(s);
	}
	return 0;
}

// Reap n completions with peek only (never ior_wait_cqe), all tagged `tag`.
static void reap_peek(notify_state *s, int n, void *tag)
{
	for (int got = 0; got < n;) {
		ior_cqe *cqe = NULL;
		int ret = ior_peek_cqe(s->ctx, &cqe);
		if (ret == -EAGAIN) {
			assert_int_equal(test_wait_readable(s->nfd, 2000), 1);
			continue;
		}
		assert_return_code(ret, 0);
		assert_int_equal((uintptr_t) ior_cqe_get_data(s->ctx, cqe), (uintptr_t) tag);
		ior_cqe_seen(s->ctx, cqe);
		got++;
	}
}

// The descriptor is stable and idle until something completes.
static void test_notify_idle(void **state)
{
	notify_state *s = (notify_state *) *state;
	assert_true(ior_notify_fd(s->ctx) == s->nfd);
	assert_int_equal(test_wait_readable(s->nfd, 50), 0);
	assert_return_code(ior_notify_clear(s->ctx), 0);
	assert_int_equal(test_wait_readable(s->nfd, 0), 0);
}

// A nop completes inline at submit; the signal must still arrive.
static void test_notify_nop(void **state)
{
	notify_state *s = (notify_state *) *state;
	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	ior_prep_nop(s->ctx, sqe);
	ior_sqe_set_data(s->ctx, sqe, (void *) 0x1);
	assert_true(ior_submit(s->ctx) >= 0);

	assert_int_equal(test_wait_readable(s->nfd, 2000), 1);
	reap_peek(s, 1, (void *) 0x1);
	assert_return_code(ior_notify_clear(s->ctx), 0);
	assert_int_equal(test_wait_readable(s->nfd, 0), 0);
}

// A recv with nothing to read does not signal; the send that feeds it does,
// and both completions are then visible to peeks. Cleared before reaping, as
// the contract says: the two completions come from different threads, and a
// signal for one already reaped may still land after a clear that follows the
// reap (a harmless spurious wakeup, not something to assert against).
static void test_notify_recv_late(void **state)
{
	notify_state *s = (notify_state *) *state;
	char buf[16];
	ior_sqe *r = ior_get_sqe(s->ctx);
	assert_non_null(r);
	ior_prep_recv(s->ctx, r, s->sock[1], buf, sizeof(buf), 0);
	ior_sqe_set_data(s->ctx, r, (void *) 0x2);
	assert_true(ior_submit(s->ctx) >= 0);
	assert_int_equal(test_wait_readable(s->nfd, 50), 0);

	ior_sqe *w = ior_get_sqe(s->ctx);
	assert_non_null(w);
	ior_prep_send(s->ctx, w, s->sock[0], "hi", 2, 0);
	ior_sqe_set_data(s->ctx, w, (void *) 0x2);
	assert_true(ior_submit(s->ctx) >= 0);

	assert_int_equal(test_wait_readable(s->nfd, 2000), 1);
	assert_return_code(ior_notify_clear(s->ctx), 0);
	reap_peek(s, 2, (void *) 0x2);
}

static int32_t notify_work_fn(ior_work_token *token, void *arg)
{
	(void) token;
	return (int32_t) (intptr_t) arg;
}

// A work callback completes from the pool (msg_ring on io_uring).
static void test_notify_work(void **state)
{
	notify_state *s = (notify_state *) *state;
	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	assert_return_code(ior_prep_work(s->ctx, sqe, notify_work_fn, (void *) 7), 0);
	ior_sqe_set_data(s->ctx, sqe, (void *) 0x3);
	assert_true(ior_submit(s->ctx) >= 0);

	assert_int_equal(test_wait_readable(s->nfd, 2000), 1);
	ior_cqe *cqe = NULL;
	while (ior_peek_cqe(s->ctx, &cqe) == -EAGAIN) {
		assert_int_equal(test_wait_readable(s->nfd, 2000), 1);
	}
	assert_int_equal(ior_cqe_get_res(s->ctx, cqe), 7);
	ior_cqe_seen(s->ctx, cqe);
	assert_return_code(ior_notify_clear(s->ctx), 0);
}

// A timeout completes from the timer thread.
static void test_notify_timer(void **state)
{
	notify_state *s = (notify_state *) *state;
	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	ior_timespec ts = { .tv_sec = 0, .tv_nsec = 30000000 };
	ior_prep_timeout(s->ctx, sqe, &ts, 0, 0);
	ior_sqe_set_data(s->ctx, sqe, (void *) 0x4);
	assert_true(ior_submit(s->ctx) >= 0);

	assert_int_equal(test_wait_readable(s->nfd, 2000), 1);
	reap_peek(s, 1, (void *) 0x4);
	assert_return_code(ior_notify_clear(s->ctx), 0);
}

// Clearing while completions are still unreaped loses nothing: they remain
// visible to peeks, and the next completion signals again.
static void test_notify_clear_then_more(void **state)
{
	notify_state *s = (notify_state *) *state;
	for (int i = 0; i < 2; i++) {
		ior_sqe *sqe = ior_get_sqe(s->ctx);
		assert_non_null(sqe);
		ior_prep_nop(s->ctx, sqe);
		ior_sqe_set_data(s->ctx, sqe, (void *) 0x5);
	}
	assert_true(ior_submit(s->ctx) >= 0);
	assert_int_equal(test_wait_readable(s->nfd, 2000), 1);
	assert_return_code(ior_notify_clear(s->ctx), 0);
	reap_peek(s, 2, (void *) 0x5);

	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	ior_prep_nop(s->ctx, sqe);
	ior_sqe_set_data(s->ctx, sqe, (void *) 0x5);
	assert_true(ior_submit(s->ctx) >= 0);
	assert_int_equal(test_wait_readable(s->nfd, 2000), 1);
	reap_peek(s, 1, (void *) 0x5);
}

// A completion posted before the first ior_notify_fd() call is announced
// by that call (a fresh context that has not requested the descriptor yet).
static void test_notify_late_request(void **state)
{
	(void) state;
	ior_ctx *ctx;
	assert_return_code(ior_queue_init(32, &ctx), 0);
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_nop(ctx, sqe);
	ior_sqe_set_data(ctx, sqe, (void *) 0x7);
	assert_true(ior_submit(ctx) >= 0);

	ior_fd_t nfd = ior_notify_fd(ctx);
	assert_true(test_fd_is_valid(nfd));
	assert_int_equal(test_wait_readable(nfd, 2000), 1);
	assert_return_code(ior_notify_clear(ctx), 0);
	ior_cqe *cqe = NULL;
	assert_return_code(ior_peek_cqe(ctx, &cqe), 0);
	assert_int_equal((uintptr_t) ior_cqe_get_data(ctx, cqe), 0x7);
	ior_cqe_seen(ctx, cqe);
	ior_queue_exit(ctx);
}

// ior_wait_cqe() still works alongside the exported descriptor.
static void test_notify_mixed_wait(void **state)
{
	notify_state *s = (notify_state *) *state;
	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	ior_prep_nop(s->ctx, sqe);
	ior_sqe_set_data(s->ctx, sqe, (void *) 0x6);
	assert_true(ior_submit_and_wait(s->ctx, 1) >= 0);
	ior_cqe *cqe = NULL;
	assert_return_code(ior_wait_cqe(s->ctx, &cqe), 0);
	ior_cqe_seen(s->ctx, cqe);
	assert_return_code(ior_notify_clear(s->ctx), 0);

	sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	ior_prep_nop(s->ctx, sqe);
	ior_sqe_set_data(s->ctx, sqe, (void *) 0x6);
	assert_true(ior_submit(s->ctx) >= 0);
	assert_int_equal(test_wait_readable(s->nfd, 2000), 1);
	reap_peek(s, 1, (void *) 0x6);
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test_setup_teardown(test_notify_idle, setup_notify, teardown_notify),
		cmocka_unit_test_setup_teardown(test_notify_nop, setup_notify, teardown_notify),
		cmocka_unit_test_setup_teardown(test_notify_recv_late, setup_notify, teardown_notify),
		cmocka_unit_test_setup_teardown(test_notify_work, setup_notify, teardown_notify),
		cmocka_unit_test_setup_teardown(test_notify_timer, setup_notify, teardown_notify),
		cmocka_unit_test_setup_teardown(test_notify_clear_then_more, setup_notify, teardown_notify),
		cmocka_unit_test(test_notify_late_request),
		cmocka_unit_test_setup_teardown(test_notify_mixed_wait, setup_notify, teardown_notify),
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
