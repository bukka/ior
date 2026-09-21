/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_cancel.c - IOR_OP_ASYNC_CANCEL on every backend.
 *
 * The contract mirrors io_uring: the cancel is an op with its own CQE (0,
 * -ENOENT or -EALREADY) and the target completes separately with -ECANCELED,
 * as do its link timeout and the rest of its link chain. Targets are chosen
 * so that they cannot complete on their own: a recv with no data, a poll on
 * an idle socket, a long timeout, a work callback parked on a flag.
 */
#include "test_utils.h"
#include <stdatomic.h>

#define TAG_OP ((void *) 0x1) // the target
#define TAG_TMO ((void *) 0x2) // its link timeout
#define TAG_NEXT ((void *) 0x3) // the op linked after it
#define TAG_CANCEL ((void *) 0x10) // the cancel itself
#define TAG_CANCEL2 ((void *) 0x11) // a second cancel in the same batch
#define TAG_OP2 ((void *) 0x4) // a second target
#define TAG_TIMER ((void *) 0x5)
#define TAG_NEW_SEND ((void *) 0x6) // I/O on a socket opened after a close
#define TAG_NEW_RECV ((void *) 0x7)

typedef struct cancel_state {
	ior_ctx *ctx;
	ior_fd_t sock[2];
} cancel_state;

static int setup_cancel(void **state)
{
	cancel_state *s = calloc(1, sizeof(*s));
	assert_non_null(s);

	int ret = ior_queue_init(64, &s->ctx);
	assert_return_code(ret, 0);
	assert_non_null(s->ctx);

	ret = test_make_socketpair(s->sock);
	assert_return_code(ret, 0);

	*state = s;
	return 0;
}

static int teardown_cancel(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	if (s) {
		if (test_fd_is_valid(s->sock[0])) {
			test_close_fd(s->sock[0]);
		}
		if (test_fd_is_valid(s->sock[1])) {
			test_close_fd(s->sock[1]);
		}
		if (s->ctx) {
			ior_queue_exit(s->ctx);
		}
		free(s);
	}
	return 0;
}

static void cancel_msleep(unsigned ms)
{
#ifdef _WIN32
	Sleep(ms);
#else
	struct timespec ts = { .tv_sec = ms / 1000, .tv_nsec = (long) (ms % 1000) * 1000000L };
	nanosleep(&ts, NULL);
#endif
}

/*
 * Reap exactly n completions into res[] indexed by tag (tags are small
 * integers), failing on a duplicate or an unexpected tag, and failing fast if
 * one never arrives rather than hanging until the test timeout.
 */
#define MAX_TAG 0x20

static void reap_tags(ior_ctx *ctx, int n, int32_t res[MAX_TAG], char seen[MAX_TAG])
{
	memset(seen, 0, MAX_TAG);
	for (int got = 0; got < n;) {
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = 3, .tv_nsec = 0 };
		int ret = ior_wait_cqe_timeout(ctx, &cqe, &to);
		if (ret == -EAGAIN || ret == -EINTR) {
			continue;
		}
		if (ret == -ETIME) {
			char have[MAX_TAG * 6] = "";
			for (int t = 0; t < MAX_TAG; t++) {
				if (seen[t]) {
					char one[8];
					snprintf(one, sizeof(one), " %#x", t);
					strcat(have, one);
				}
			}
			fail_msg("missing completion after %d/%d (have:%s)", got, n, have);
		}
		assert_return_code(ret, 0);
		assert_non_null(cqe);

		uintptr_t tag = (uintptr_t) ior_cqe_get_data(ctx, cqe);
		assert_true(tag < MAX_TAG);
		assert_int_equal(seen[tag], 0);
		seen[tag] = 1;
		res[tag] = ior_cqe_get_res(ctx, cqe);
		ior_cqe_seen(ctx, cqe);
		got++;
	}
}

static void submit_recv(cancel_state *s, char *buf, size_t len, void *tag, uint8_t flags)
{
	ior_sqe *r = ior_get_sqe(s->ctx);
	assert_non_null(r);
	ior_prep_recv(s->ctx, r, s->sock[1], buf, (unsigned) len, 0);
	ior_sqe_set_data(s->ctx, r, tag);
	if (flags) {
		ior_sqe_set_flags(s->ctx, r, flags);
	}
}

static void submit_cancel(cancel_state *s, void *target)
{
	ior_sqe *c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_cancel(s->ctx, c, target);
	ior_sqe_set_data(s->ctx, c, TAG_CANCEL);
}

// A recv with nothing to read is cancelled; both CQEs arrive.
static void test_cancel_recv(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	char buf[64];
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	submit_recv(s, buf, sizeof(buf), TAG_OP, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20); // let the backend arm it

	submit_cancel(s, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 2, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);
}

// Target and cancel in the same submit batch: the cancel still finds it,
// either queued (0) or in its first non-blocking attempt (-EALREADY, but
// the recv is then finished as cancelled instead of parked).
static void test_cancel_same_batch(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	char buf[64];
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	submit_recv(s, buf, sizeof(buf), TAG_OP, 0);
	submit_cancel(s, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 2, res, seen);
	assert_true(res[(uintptr_t) TAG_CANCEL] == 0 || res[(uintptr_t) TAG_CANCEL] == -EALREADY);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);
}

// Nothing in flight matches: -ENOENT and no other CQE.
static void test_cancel_not_found(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	submit_cancel(s, (void *) 0x7);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 1, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], -ENOENT);
}

// A target that already completed (and was reaped) is not found.
static void test_cancel_completed(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	ior_sqe *n = ior_get_sqe(s->ctx);
	assert_non_null(n);
	ior_prep_nop(s->ctx, n);
	ior_sqe_set_data(s->ctx, n, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_OP], 0);

	submit_cancel(s, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], -ENOENT);
}

// An armed timeout is cancelled instead of firing.
static void test_cancel_timeout(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	ior_sqe *t = ior_get_sqe(s->ctx);
	assert_non_null(t);
	ior_timespec ts = { .tv_sec = 5, .tv_nsec = 0 };
	ior_prep_timeout(s->ctx, t, &ts, 0, 0);
	ior_sqe_set_data(s->ctx, t, TAG_TIMER);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20);

	submit_cancel(s, TAG_TIMER);
	assert_true(ior_submit(s->ctx) >= 0);

	uint64_t start = test_monotonic_now_ns();
	reap_tags(s->ctx, 2, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_int_equal(res[(uintptr_t) TAG_TIMER], -ECANCELED);
	// Far short of the 5s deadline.
	assert_true(test_monotonic_now_ns() - start < 2000000000ULL);
}

// A poll on an idle socket is cancelled.
static void test_cancel_poll(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	ior_sqe *p = ior_get_sqe(s->ctx);
	assert_non_null(p);
	ior_prep_poll_add(s->ctx, p, s->sock[1], IOR_POLL_IN);
	ior_sqe_set_data(s->ctx, p, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20);

	submit_cancel(s, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 2, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);
}

// Cancelling a guarded op also cancels its link timeout (not -ETIME).
static void test_cancel_guarded(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	char buf[64];
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	submit_recv(s, buf, sizeof(buf), TAG_OP, IOR_SQE_IO_LINK);
	ior_sqe *t = ior_get_sqe(s->ctx);
	assert_non_null(t);
	ior_timespec ts = { .tv_sec = 5, .tv_nsec = 0 };
	ior_prep_link_timeout(s->ctx, t, &ts, 0);
	ior_sqe_set_data(s->ctx, t, TAG_TMO);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20);

	submit_cancel(s, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);

	uint64_t start = test_monotonic_now_ns();
	reap_tags(s->ctx, 3, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);
	assert_int_equal(res[(uintptr_t) TAG_TMO], -ECANCELED);
	assert_true(test_monotonic_now_ns() - start < 2000000000ULL);
}

// Cancelling the head of a link chain cancels what is linked behind it.
static void test_cancel_chain(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	char buf[64];
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	submit_recv(s, buf, sizeof(buf), TAG_OP, IOR_SQE_IO_LINK);
	ior_sqe *n = ior_get_sqe(s->ctx);
	assert_non_null(n);
	ior_prep_nop(s->ctx, n);
	ior_sqe_set_data(s->ctx, n, TAG_NEXT);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20);

	submit_cancel(s, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 3, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);
	assert_int_equal(res[(uintptr_t) TAG_NEXT], -ECANCELED);
}

// Cancel by descriptor takes one op per call: two cancels for two recvs, then
// -ENOENT once the descriptor is idle.
static void test_cancel_fd(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	char buf[64], buf2[64];
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	submit_recv(s, buf, sizeof(buf), TAG_OP, 0);
	submit_recv(s, buf2, sizeof(buf2), TAG_OP2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20);

	ior_sqe *c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_cancel_fd(s->ctx, c, s->sock[1]);
	ior_sqe_set_data(s->ctx, c, TAG_CANCEL);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 2, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	int first = seen[(uintptr_t) TAG_OP] ? (int) (uintptr_t) TAG_OP : (int) (uintptr_t) TAG_OP2;
	assert_int_equal(res[first], -ECANCELED);

	c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_cancel_fd(s->ctx, c, s->sock[1]);
	ior_sqe_set_data(s->ctx, c, TAG_CANCEL);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 2, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	int second = first == (int) (uintptr_t) TAG_OP ? (int) (uintptr_t) TAG_OP2
												   : (int) (uintptr_t) TAG_OP;
	assert_int_equal(res[second], -ECANCELED);

	c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_cancel_fd(s->ctx, c, s->sock[1]);
	ior_sqe_set_data(s->ctx, c, TAG_CANCEL);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], -ENOENT);
}

/*
 * Two recvs on one socket cancelled in the same batch. Every CQE must arrive
 * promptly: each cancel finds its target (0) or a completion already queued
 * for it (-ENOENT, which IOCP answers for the second: AFD aborts every pending
 * recv on the socket when the first is cancelled), but a cancel that answers
 * -ENOENT must never leave its target back in flight with no CQE.
 */
static void test_cancel_two_same_socket_batch(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	char buf[64], buf2[64];
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	submit_recv(s, buf, sizeof(buf), TAG_OP, 0);
	submit_recv(s, buf2, sizeof(buf2), TAG_OP2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20);

	submit_cancel(s, TAG_OP);
	ior_sqe *c2 = ior_get_sqe(s->ctx);
	assert_non_null(c2);
	ior_prep_cancel(s->ctx, c2, TAG_OP2);
	ior_sqe_set_data(s->ctx, c2, TAG_CANCEL2);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 4, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_true(res[(uintptr_t) TAG_CANCEL2] == 0 || res[(uintptr_t) TAG_CANCEL2] == -ENOENT);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);
	assert_int_equal(res[(uintptr_t) TAG_OP2], -ECANCELED);
}

/*
 * The socket is closed while a cancel's side effects are still queued, and
 * new sockets are opened at once (on Windows they get the closed handle's
 * value back). The second recv, which the cancel of the first took down with
 * it on IOCP, must be reported as cancelled - never replayed onto whatever now
 * carries that descriptor value - and the new socket's own recv must get its
 * data. The POSIX backends do not complete an op on a closed descriptor
 * (io_uring keeps the file alive, the poller loses its registration), and
 * closing one with cancels still in flight is the misuse ior_prep_cancel_fd()
 * warns about, so there both recvs are cancelled and reaped before the close;
 * the reopen and the new socket's I/O are checked the same way.
 */
static void test_cancel_then_close_and_reopen(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	char buf[64], buf2[64], nbuf[64];
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	submit_recv(s, buf, sizeof(buf), TAG_OP, 0);
	submit_recv(s, buf2, sizeof(buf2), TAG_OP2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20);
	submit_cancel(s, TAG_OP);
#ifndef _WIN32
	// No abort-on-close here: cancel the neighbour too, and let both cancels
	// take effect before the descriptor goes away, as callers must.
	ior_sqe *c2 = ior_get_sqe(s->ctx);
	assert_non_null(c2);
	ior_prep_cancel(s->ctx, c2, TAG_OP2);
	ior_sqe_set_data(s->ctx, c2, TAG_CANCEL2);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 4, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL2], 0);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);
	assert_int_equal(res[(uintptr_t) TAG_OP2], -ECANCELED);
#else
	assert_true(ior_submit(s->ctx) >= 0);
#endif

	// Close the socket and open new ones (with a cancel's side effects still
	// queued on IOCP).
	test_close_fd(s->sock[1]);
	s->sock[1] = IOR_TEST_INVALID_FD;
	ior_fd_t fresh[2];
	assert_return_code(test_make_socketpair(fresh), 0);

	ior_sqe *w = ior_get_sqe(s->ctx);
	assert_non_null(w);
	ior_prep_send(s->ctx, w, fresh[0], "x", 1, 0);
	ior_sqe_set_data(s->ctx, w, TAG_NEW_SEND);
	ior_sqe *r = ior_get_sqe(s->ctx);
	assert_non_null(r);
	memset(nbuf, 0, sizeof(nbuf));
	ior_prep_recv(s->ctx, r, fresh[1], nbuf, sizeof(nbuf), 0);
	ior_sqe_set_data(s->ctx, r, TAG_NEW_RECV);
	assert_true(ior_submit(s->ctx) >= 0);

#ifndef _WIN32
	reap_tags(s->ctx, 2, res, seen);
#else
	reap_tags(s->ctx, 5, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);
	assert_int_equal(res[(uintptr_t) TAG_OP2], -ECANCELED);
#endif
	assert_int_equal(res[(uintptr_t) TAG_NEW_SEND], 1);
	assert_int_equal(res[(uintptr_t) TAG_NEW_RECV], 1);
	assert_int_equal(nbuf[0], 'x');

	test_close_fd(fresh[0]);
	test_close_fd(fresh[1]);
}

/*
 * An abort the library did not cause must leave no mark on whatever the
 * process opens next under the aborted handle's value. On IOCP such an abort
 * (ERROR_OPERATION_ABORTED with the handle's cancel generation untouched)
 * comes from the caller's own CancelIoEx or from the issuing thread exiting;
 * a close does not produce one, AFD and the pipe driver report those as
 * ERROR_CONNECTION_ABORTED and ERROR_BROKEN_PIPE. The backend used to
 * re-validate every such abort's handle with the kernel, and once the value
 * had been closed and recycled the probe associated the caller's new socket
 * with this queue's port for good, so no other port could ever take it. The
 * value is caught here on a client socket this queue never sees, and a second
 * queue must connect, send and recv on it like on any new socket.
 *
 * On POSIX the recv is cancelled through the queue and reaped before the
 * close, and the second queue is checked the same way.
 */
static void test_foreign_abort_reopen_other_queue(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	char buf[64], nbuf[64];
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	// The listener is made first so that the socket opened right after the
	// close is the client.
	ior_fd_t listener;
	struct sockaddr_storage addr;
	socklen_t addrlen;
	assert_return_code(test_make_listener(&listener, &addr, &addrlen), 0);

	submit_recv(s, buf, sizeof(buf), TAG_OP, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20);
#ifdef _WIN32
	// Abort it behind the queue's back: its completion is queued now, and is
	// reaped only once the value belongs to the new socket.
	assert_true(CancelIoEx((HANDLE) s->sock[1], NULL));
#else
	submit_cancel(s, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);
#endif

	test_close_fd(s->sock[1]);
	s->sock[1] = IOR_TEST_INVALID_FD;
	ior_fd_t client;
	assert_return_code(test_make_tcp_socket(&client), 0);

#ifdef _WIN32
	reap_tags(s->ctx, 1, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);
#endif

	// The connect goes first on its own: a socket bound to another port is
	// refused right there (-EBADF), and an accept would then never complete.
	ior_ctx *ctx2 = NULL;
	assert_return_code(ior_queue_init(16, &ctx2), 0);
	ior_sqe *c = ior_get_sqe(ctx2);
	assert_non_null(c);
	ior_prep_connect(ctx2, c, client, (const struct sockaddr *) &addr, addrlen);
	ior_sqe_set_data(ctx2, c, TAG_OP);
	assert_true(ior_submit(ctx2) >= 0);
	reap_tags(ctx2, 1, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_OP], 0);
	ior_sqe *a = ior_get_sqe(ctx2);
	assert_non_null(a);
	ior_prep_accept(ctx2, a, listener, NULL, NULL, 0);
	ior_sqe_set_data(ctx2, a, TAG_OP2);
	assert_true(ior_submit(ctx2) >= 0);
	reap_tags(ctx2, 1, res, seen);
	assert_true(res[(uintptr_t) TAG_OP2] >= 0);
	ior_fd_t accepted = (ior_fd_t) (intptr_t) res[(uintptr_t) TAG_OP2];

	ior_sqe *w = ior_get_sqe(ctx2);
	assert_non_null(w);
	ior_prep_send(ctx2, w, client, "x", 1, 0);
	ior_sqe_set_data(ctx2, w, TAG_NEW_SEND);
	ior_sqe *r = ior_get_sqe(ctx2);
	assert_non_null(r);
	memset(nbuf, 0, sizeof(nbuf));
	ior_prep_recv(ctx2, r, accepted, nbuf, sizeof(nbuf), 0);
	ior_sqe_set_data(ctx2, r, TAG_NEW_RECV);
	assert_true(ior_submit(ctx2) >= 0);
	reap_tags(ctx2, 2, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_NEW_SEND], 1);
	assert_int_equal(res[(uintptr_t) TAG_NEW_RECV], 1);
	assert_int_equal(nbuf[0], 'x');

	ior_queue_exit(ctx2);
	test_close_fd(accepted);
	test_close_fd(client);
	test_close_fd(listener);
}

#ifdef _WIN32
// A socket closed with a recv still parked on it fails that recv with what
// AFD reports for a closed object, -ECONNABORTED, not -ECANCELED: nothing
// cancelled it (the case ior_prep_cancel_fd() documents).
static void test_close_under_recv_reports_connaborted(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	char buf[64];
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	submit_recv(s, buf, sizeof(buf), TAG_OP, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20);
	test_close_fd(s->sock[1]);
	s->sock[1] = IOR_TEST_INVALID_FD;
	reap_tags(s->ctx, 1, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECONNABORTED);
}
#endif

// A cancelled recv on a socket that becomes readable later must not consume
// the data: the next recv gets it.
static void test_cancel_recv_data_intact(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	char buf[64];
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];

	submit_recv(s, buf, sizeof(buf), TAG_OP, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	cancel_msleep(20);
	submit_cancel(s, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED);

	const char *msg = "after";
	unsigned len = (unsigned) strlen(msg);
	ior_sqe *w = ior_get_sqe(s->ctx);
	assert_non_null(w);
	ior_prep_send(s->ctx, w, s->sock[0], msg, len, 0);
	ior_sqe_set_data(s->ctx, w, TAG_NEXT);
	memset(buf, 0, sizeof(buf));
	submit_recv(s, buf, sizeof(buf), TAG_OP2, 0);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 2, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_NEXT], (int32_t) len);
	assert_int_equal(res[(uintptr_t) TAG_OP2], (int32_t) len);
	assert_memory_equal(buf, msg, len);
}

/* ===== Work ops ===== */

typedef struct work_gate {
	_Atomic int started;
	_Atomic int release;
	_Atomic int done; /* last access by the callback; see gate_join */
} work_gate;

// Runs until released or cancelled; reports which.
static int32_t gated_work(ior_work_token *token, void *arg)
{
	work_gate *g = arg;
	int32_t res = -ETIMEDOUT;
	atomic_store(&g->started, 1);
	for (int i = 0; i < 3000; i++) {
		if (ior_work_cancelled(token)) {
			res = -ECANCELED;
			break;
		}
		if (atomic_load(&g->release)) {
			res = 42;
			break;
		}
		cancel_msleep(1);
	}
	atomic_store_explicit(&g->done, 1, memory_order_release);
	return res;
}

/*
 * Order the callback's last access to the gate before this thread reuses the
 * stack: the CQE itself synchronizes them, but on io_uring it travels through
 * the kernel (msg_ring), which ThreadSanitizer cannot see.
 */
static void gate_join(work_gate *g)
{
	if (!atomic_load(&g->started)) {
		return;
	}
	while (!atomic_load_explicit(&g->done, memory_order_acquire)) {
		cancel_msleep(1);
	}
}

// A running callback cannot be killed: -EALREADY, but it observes the token.
static void test_cancel_work_running(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];
	work_gate gate;
	atomic_init(&gate.started, 0);
	atomic_init(&gate.release, 0);

	ior_sqe *w = ior_get_sqe(s->ctx);
	assert_non_null(w);
	assert_return_code(ior_prep_work(s->ctx, w, gated_work, &gate), 0);
	ior_sqe_set_data(s->ctx, w, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);
	while (!atomic_load(&gate.started)) {
		cancel_msleep(1);
	}

	submit_cancel(s, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 2, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], -EALREADY);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED); // the callback's choice
	gate_join(&gate);
}

/*
 * A running callback guarded by a link timeout: the cancel must reach the
 * token the callback actually polls (the pair's arbitration token, not the
 * op's own), so the callback can bail out, and the link timeout then resolves
 * as "op finished first".
 */
static void test_cancel_work_guarded_running(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	int32_t res[MAX_TAG];
	char seen[MAX_TAG];
	work_gate gate;
	atomic_init(&gate.started, 0);
	atomic_init(&gate.release, 0);
	atomic_init(&gate.done, 0);

	ior_sqe *w = ior_get_sqe(s->ctx);
	assert_non_null(w);
	assert_return_code(ior_prep_work(s->ctx, w, gated_work, &gate), 0);
	ior_sqe_set_data(s->ctx, w, TAG_OP);
	ior_sqe_set_flags(s->ctx, w, IOR_SQE_IO_LINK);
	ior_sqe *t = ior_get_sqe(s->ctx);
	assert_non_null(t);
	ior_timespec ts = { .tv_sec = 5, .tv_nsec = 0 };
	ior_prep_link_timeout(s->ctx, t, &ts, 0);
	ior_sqe_set_data(s->ctx, t, TAG_TMO);
	assert_true(ior_submit(s->ctx) >= 0);
	while (!atomic_load(&gate.started)) {
		cancel_msleep(1);
	}

	submit_cancel(s, TAG_OP);
	assert_true(ior_submit(s->ctx) >= 0);

	uint64_t start = test_monotonic_now_ns();
	reap_tags(s->ctx, 3, res, seen);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], -EALREADY);
	assert_int_equal(res[(uintptr_t) TAG_OP], -ECANCELED); // the callback's choice
	assert_int_equal(res[(uintptr_t) TAG_TMO], -ECANCELED); // callback finished first
	assert_true(test_monotonic_now_ns() - start < 2000000000ULL);
	gate_join(&gate);
}

/*
 * A callback that has not started never runs when cancelled. The pool is
 * saturated with gated callbacks (more than its 32 worker cap) so the last
 * submitted one is certainly still queued when the cancel arrives.
 */
#define WORK_FLOOD 40

static void test_cancel_work_queued(void **state)
{
	cancel_state *s = (cancel_state *) *state;
	work_gate gates[WORK_FLOOD];

	for (int i = 0; i < WORK_FLOOD; i++) {
		atomic_init(&gates[i].started, 0);
		atomic_init(&gates[i].release, 0);
		ior_sqe *w = ior_get_sqe(s->ctx);
		assert_non_null(w);
		assert_return_code(ior_prep_work(s->ctx, w, gated_work, &gates[i]), 0);
		ior_sqe_set_data(s->ctx, w, (void *) (uintptr_t) (0x100 + i));
	}
	assert_true(ior_submit(s->ctx) >= 0);
	// Wait until the pool is saturated: the last op cannot have started.
	int running = 0;
	for (int spin = 0; spin < 2000 && running < 32; spin++) {
		running = 0;
		for (int i = 0; i < WORK_FLOOD; i++) {
			running += atomic_load(&gates[i].started);
		}
		cancel_msleep(1);
	}
	assert_int_equal(running, 32);
	assert_int_equal(atomic_load(&gates[WORK_FLOOD - 1].started), 0);

	ior_sqe *c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_cancel(s->ctx, c, (void *) (uintptr_t) (0x100 + WORK_FLOOD - 1));
	ior_sqe_set_data(s->ctx, c, TAG_CANCEL);
	assert_true(ior_submit(s->ctx) >= 0);

	// Release the rest and reap everything: the cancelled one never ran.
	for (int i = 0; i < WORK_FLOOD; i++) {
		atomic_store(&gates[i].release, 1);
	}
	int got_cancel = 0, got_target = 0, got_others = 0;
	for (int n = 0; n < WORK_FLOOD + 1; n++) {
		ior_cqe *cqe = NULL;
		int ret;
		while ((ret = ior_wait_cqe(s->ctx, &cqe)) == -EINTR) { }
		assert_return_code(ret, 0);
		uintptr_t tag = (uintptr_t) ior_cqe_get_data(s->ctx, cqe);
		int32_t res = ior_cqe_get_res(s->ctx, cqe);
		if (tag == (uintptr_t) TAG_CANCEL) {
			assert_int_equal(res, 0);
			got_cancel++;
		} else if (tag == 0x100 + WORK_FLOOD - 1) {
			assert_int_equal(res, -ECANCELED);
			got_target++;
		} else {
			assert_int_equal(res, 42);
			got_others++;
		}
		ior_cqe_seen(s->ctx, cqe);
	}
	assert_int_equal(got_cancel, 1);
	assert_int_equal(got_target, 1);
	assert_int_equal(got_others, WORK_FLOOD - 1);
	assert_int_equal(atomic_load(&gates[WORK_FLOOD - 1].started), 0);
	for (int i = 0; i < WORK_FLOOD; i++) {
		gate_join(&gates[i]);
	}
}

/*
 * Stress: many recvs cancelled while readiness races in. Half the targets
 * get data just before the cancel, so the cancel may find them completed
 * (-ENOENT) or beat them (0); either way exactly one CQE per op arrives and
 * a cancelled op never reports data.
 */
#define STRESS_ROUNDS 50
#define STRESS_PAIRS 16

typedef struct stress_state {
	ior_ctx *ctx;
	ior_fd_t sock[STRESS_PAIRS][2];
	char buf[STRESS_PAIRS][8];
} stress_state;

static int setup_stress(void **state)
{
	stress_state *s = calloc(1, sizeof(*s));
	assert_non_null(s);
	assert_return_code(ior_queue_init(2 * STRESS_PAIRS + 64, &s->ctx), 0);
	for (int i = 0; i < STRESS_PAIRS; i++) {
		assert_return_code(test_make_socketpair(s->sock[i]), 0);
	}
	*state = s;
	return 0;
}

static int teardown_stress(void **state)
{
	stress_state *s = (stress_state *) *state;
	if (s) {
		for (int i = 0; i < STRESS_PAIRS; i++) {
			test_close_fd(s->sock[i][0]);
			test_close_fd(s->sock[i][1]);
		}
		ior_queue_exit(s->ctx);
		free(s);
	}
	return 0;
}

static void test_cancel_stress(void **state)
{
	stress_state *s = (stress_state *) *state;
	const char msg[8] = "racing!";

	for (int r = 0; r < STRESS_ROUNDS; r++) {
		for (int i = 0; i < STRESS_PAIRS; i++) {
			ior_sqe *rcv = ior_get_sqe(s->ctx);
			assert_non_null(rcv);
			ior_prep_recv(s->ctx, rcv, s->sock[i][1], s->buf[i], sizeof(s->buf[i]), 0);
			ior_sqe_set_data(s->ctx, rcv, (void *) (uintptr_t) (1 + i));
		}
		assert_true(ior_submit(s->ctx) >= 0);

		// Feed every other target right before cancelling all of them.
		int fed = 0;
		for (int i = 0; i < STRESS_PAIRS; i += 2) {
			ior_sqe *snd = ior_get_sqe(s->ctx);
			assert_non_null(snd);
			ior_prep_send(s->ctx, snd, s->sock[i][0], msg, sizeof(msg), 0);
			ior_sqe_set_data(s->ctx, snd, (void *) (uintptr_t) (100 + i));
			fed++;
		}
		for (int i = 0; i < STRESS_PAIRS; i++) {
			ior_sqe *c = ior_get_sqe(s->ctx);
			assert_non_null(c);
			ior_prep_cancel(s->ctx, c, (void *) (uintptr_t) (1 + i));
			ior_sqe_set_data(s->ctx, c, (void *) (uintptr_t) (200 + i));
		}
		assert_true(ior_submit(s->ctx) >= 0);

		int recv_res[STRESS_PAIRS], cancel_res[STRESS_PAIRS];
		char got_recv[STRESS_PAIRS] = { 0 }, got_cancel[STRESS_PAIRS] = { 0 };
		int drained = 0;
		int expect = 2 * STRESS_PAIRS + fed;
		for (int n = 0; n < expect; n++) {
			ior_cqe *cqe = NULL;
			ior_timespec to = { .tv_sec = 3, .tv_nsec = 0 };
			int ret = ior_wait_cqe_timeout(s->ctx, &cqe, &to);
			if (ret == -EINTR) {
				n--;
				continue;
			}
			if (ret == -ETIME) {
				fail_msg("round %d: missing completion after %d/%d", r, n, expect);
			}
			assert_return_code(ret, 0);
			uintptr_t tag = (uintptr_t) ior_cqe_get_data(s->ctx, cqe);
			int32_t res = ior_cqe_get_res(s->ctx, cqe);
			if (tag >= 200) {
				int i = (int) (tag - 200);
				assert_int_equal(got_cancel[i], 0);
				got_cancel[i] = 1;
				cancel_res[i] = res;
			} else if (tag >= 100) {
				assert_int_equal(res, (int32_t) sizeof(msg));
			} else {
				int i = (int) (tag - 1);
				assert_int_equal(got_recv[i], 0);
				got_recv[i] = 1;
				recv_res[i] = res;
			}
			ior_cqe_seen(s->ctx, cqe);
		}
		for (int i = 0; i < STRESS_PAIRS; i++) {
			assert_int_equal(got_recv[i], 1);
			assert_int_equal(got_cancel[i], 1);
			if (i % 2) {
				// Never fed: must end cancelled. -EALREADY is the cancel
				// catching the op mid-attempt (still finished as cancelled).
				assert_true(cancel_res[i] == 0 || cancel_res[i] == -EALREADY);
				assert_int_equal(recv_res[i], -ECANCELED);
			} else {
				// Fed: either outcome, but consistently paired: a cancel that
				// reports 0 never sees data delivered.
				if (recv_res[i] == -ECANCELED) {
					assert_true(cancel_res[i] == 0 || cancel_res[i] == -EALREADY);
					drained++;
				} else {
					assert_int_equal(recv_res[i], (int32_t) sizeof(msg));
					assert_true(cancel_res[i] == -ENOENT || cancel_res[i] == -EALREADY);
				}
			}
		}
		// Data left behind by cancelled fed recvs: read it out so the next
		// round starts idle.
		for (int i = 0; i < STRESS_PAIRS; i += 2) {
			if (recv_res[i] != -ECANCELED) {
				continue;
			}
			ior_sqe *rcv = ior_get_sqe(s->ctx);
			assert_non_null(rcv);
			ior_prep_recv(s->ctx, rcv, s->sock[i][1], s->buf[i], sizeof(s->buf[i]), 0);
			ior_sqe_set_data(s->ctx, rcv, (void *) (uintptr_t) (1 + i));
		}
		assert_true(ior_submit(s->ctx) >= 0);
		for (int n = 0; n < drained; n++) {
			ior_cqe *cqe = NULL;
			int ret;
			while ((ret = ior_wait_cqe(s->ctx, &cqe)) == -EINTR) { }
			assert_return_code(ret, 0);
			assert_int_equal(ior_cqe_get_res(s->ctx, cqe), (int32_t) sizeof(msg));
			ior_cqe_seen(s->ctx, cqe);
		}
	}
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test_setup_teardown(test_cancel_recv, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_same_batch, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_not_found, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_completed, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_timeout, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_poll, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_guarded, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_chain, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_fd, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(
				test_cancel_two_same_socket_batch, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(
				test_cancel_then_close_and_reopen, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(
				test_foreign_abort_reopen_other_queue, setup_cancel, teardown_cancel),
#ifdef _WIN32
		cmocka_unit_test_setup_teardown(
				test_close_under_recv_reports_connaborted, setup_cancel, teardown_cancel),
#endif
		cmocka_unit_test_setup_teardown(
				test_cancel_recv_data_intact, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_work_running, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(
				test_cancel_work_guarded_running, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_work_queued, setup_cancel, teardown_cancel),
		cmocka_unit_test_setup_teardown(test_cancel_stress, setup_stress, teardown_stress),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
