/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_accept_connect.c - IOR_OP_ACCEPT and IOR_OP_CONNECT on every backend.
 *
 * A listening loopback TCP socket takes connections from sockets connected
 * through ior; the accepted socket must be usable for send/recv (on IOCP
 * that proves the accept context was updated), the peer address must be
 * filled in, a refused connect must fail with -ECONNREFUSED, and a pending
 * accept must be cancellable and bounded by a link timeout.
 */
#include "test_utils.h"
#ifndef _WIN32
#include <netinet/in.h>
#endif

#define TAG_ACCEPT ((void *) 0x1)
#define TAG_CONNECT ((void *) 0x2)
#define TAG_TMO ((void *) 0x3)
#define TAG_CANCEL ((void *) 0x4)
#define TAG_SEND ((void *) 0x5)
#define TAG_RECV ((void *) 0x6)

typedef struct ac_state {
	ior_ctx *ctx;
	ior_fd_t listener;
	struct sockaddr_storage addr;
	socklen_t addrlen;
	ior_fd_t client;
	ior_fd_t accepted;
} ac_state;

static int setup_ac(void **state)
{
	ac_state *s = calloc(1, sizeof(*s));
	assert_non_null(s);
	assert_return_code(ior_queue_init(32, &s->ctx), 0);
	assert_return_code(test_make_listener(&s->listener, &s->addr, &s->addrlen), 0);
	assert_return_code(test_make_tcp_socket(&s->client), 0);
	s->accepted = IOR_TEST_INVALID_FD;
	*state = s;
	return 0;
}

static int teardown_ac(void **state)
{
	ac_state *s = (ac_state *) *state;
	if (s) {
		if (test_fd_is_valid(s->accepted)) {
			test_close_fd(s->accepted);
		}
		test_close_fd(s->client);
		test_close_fd(s->listener);
		ior_queue_exit(s->ctx);
		free(s);
	}
	return 0;
}

// Reap n completions into res[] by tag, failing fast if one never arrives.
#define MAX_TAG 0x10
static void reap_tags(ior_ctx *ctx, int n, int32_t res[MAX_TAG])
{
	char seen[MAX_TAG] = { 0 };
	for (int got = 0; got < n;) {
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = 3, .tv_nsec = 0 };
		int ret = ior_wait_cqe_timeout(ctx, &cqe, &to);
		if (ret == -EAGAIN || ret == -EINTR) {
			continue;
		}
		if (ret == -ETIME) {
			fail_msg("missing completion after %d/%d", got, n);
		}
		assert_return_code(ret, 0);
		uintptr_t tag = (uintptr_t) ior_cqe_get_data(ctx, cqe);
		assert_true(tag < MAX_TAG);
		assert_int_equal(seen[tag], 0);
		seen[tag] = 1;
		res[tag] = ior_cqe_get_res(ctx, cqe);
		ior_cqe_seen(ctx, cqe);
		got++;
	}
}

// The accepted socket, as the backend reports it in res.
static ior_fd_t accepted_fd(int32_t res)
{
	assert_true(res >= 0);
#ifdef _WIN32
	return (ior_fd_t) (intptr_t) res;
#else
	return res;
#endif
}

static void submit_accept(
		ac_state *s, struct sockaddr_storage *peer, socklen_t *peerlen, uint8_t flags)
{
	ior_sqe *a = ior_get_sqe(s->ctx);
	assert_non_null(a);
	ior_prep_accept(s->ctx, a, s->listener, (struct sockaddr *) peer, peerlen, 0);
	ior_sqe_set_data(s->ctx, a, TAG_ACCEPT);
	if (flags) {
		ior_sqe_set_flags(s->ctx, a, flags);
	}
}

static void submit_connect(ac_state *s)
{
	ior_sqe *c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_connect(s->ctx, c, s->client, (const struct sockaddr *) &s->addr, s->addrlen);
	ior_sqe_set_data(s->ctx, c, TAG_CONNECT);
}

// Accept parked first, connect afterwards: both complete, the peer address is
// filled in, and data flows both ways over the new connection.
static void test_accept_then_connect(void **state)
{
	ac_state *s = (ac_state *) *state;
	int32_t res[MAX_TAG];
	struct sockaddr_storage peer;
	socklen_t peerlen = sizeof(peer);
	memset(&peer, 0, sizeof(peer));

	submit_accept(s, &peer, &peerlen, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	// Nothing is connecting yet: the accept must be pending, not failed.
	ior_cqe *cqe = NULL;
	ior_timespec to = { .tv_sec = 0, .tv_nsec = 50000000 };
	assert_int_equal(ior_wait_cqe_timeout(s->ctx, &cqe, &to), -ETIME);

	submit_connect(s);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res);
	assert_int_equal(res[(uintptr_t) TAG_CONNECT], 0);
	s->accepted = accepted_fd(res[(uintptr_t) TAG_ACCEPT]);
	assert_int_equal(peer.ss_family, AF_INET);
	assert_int_equal(peerlen, sizeof(struct sockaddr_in));

	// The accepted socket works for I/O on the backend (IOCP needs its accept
	// context updated for this), in both directions.
	const char *msg = "over accept";
	unsigned len = (unsigned) strlen(msg);
	char buf[32];
	memset(buf, 0, sizeof(buf));
	ior_sqe *snd = ior_get_sqe(s->ctx);
	assert_non_null(snd);
	ior_prep_send(s->ctx, snd, s->client, msg, len, 0);
	ior_sqe_set_data(s->ctx, snd, TAG_SEND);
	ior_sqe *rcv = ior_get_sqe(s->ctx);
	assert_non_null(rcv);
	ior_prep_recv(s->ctx, rcv, s->accepted, buf, sizeof(buf), 0);
	ior_sqe_set_data(s->ctx, rcv, TAG_RECV);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res);
	assert_int_equal(res[(uintptr_t) TAG_SEND], (int32_t) len);
	assert_int_equal(res[(uintptr_t) TAG_RECV], (int32_t) len);
	assert_memory_equal(buf, msg, len);

	memset(buf, 0, sizeof(buf));
	snd = ior_get_sqe(s->ctx);
	assert_non_null(snd);
	ior_prep_send(s->ctx, snd, s->accepted, msg, len, 0);
	ior_sqe_set_data(s->ctx, snd, TAG_SEND);
	rcv = ior_get_sqe(s->ctx);
	assert_non_null(rcv);
	ior_prep_recv(s->ctx, rcv, s->client, buf, sizeof(buf), 0);
	ior_sqe_set_data(s->ctx, rcv, TAG_RECV);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res);
	assert_int_equal(res[(uintptr_t) TAG_SEND], (int32_t) len);
	assert_int_equal(res[(uintptr_t) TAG_RECV], (int32_t) len);
	assert_memory_equal(buf, msg, len);
}

// Connect first (queued in the listen backlog), accept afterwards, without
// address buffers.
static void test_connect_then_accept(void **state)
{
	ac_state *s = (ac_state *) *state;
	int32_t res[MAX_TAG];

	submit_connect(s);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res);
	assert_int_equal(res[(uintptr_t) TAG_CONNECT], 0);

	submit_accept(s, NULL, NULL, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res);
	s->accepted = accepted_fd(res[(uintptr_t) TAG_ACCEPT]);
}

// Nobody listens on a fresh ephemeral port: -ECONNREFUSED.
static void test_connect_refused(void **state)
{
	ac_state *s = (ac_state *) *state;
	int32_t res[MAX_TAG];

	// Pick a port that was just released: close a second listener.
	ior_fd_t dead;
	struct sockaddr_storage dead_addr;
	socklen_t dead_len;
	assert_return_code(test_make_listener(&dead, &dead_addr, &dead_len), 0);
	test_close_fd(dead);

	ior_sqe *c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_connect(s->ctx, c, s->client, (const struct sockaddr *) &dead_addr, dead_len);
	ior_sqe_set_data(s->ctx, c, TAG_CONNECT);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res);
	assert_int_equal(res[(uintptr_t) TAG_CONNECT], -ECONNREFUSED);
}

// A pending accept is cancelled: both CQEs, and the listener stays usable.
static void test_accept_cancel(void **state)
{
	ac_state *s = (ac_state *) *state;
	int32_t res[MAX_TAG];

	submit_accept(s, NULL, NULL, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	ior_cqe *cqe = NULL;
	ior_timespec to = { .tv_sec = 0, .tv_nsec = 20000000 };
	assert_int_equal(ior_wait_cqe_timeout(s->ctx, &cqe, &to), -ETIME);

	ior_sqe *c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_cancel(s->ctx, c, TAG_ACCEPT);
	ior_sqe_set_data(s->ctx, c, TAG_CANCEL);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_int_equal(res[(uintptr_t) TAG_ACCEPT], -ECANCELED);

	// Still a working listener afterwards.
	submit_connect(s);
	submit_accept(s, NULL, NULL, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res);
	assert_int_equal(res[(uintptr_t) TAG_CONNECT], 0);
	s->accepted = accepted_fd(res[(uintptr_t) TAG_ACCEPT]);
}

// A pending accept bounded by a link timeout.
static void test_accept_link_timeout(void **state)
{
	ac_state *s = (ac_state *) *state;
	int32_t res[MAX_TAG];

	submit_accept(s, NULL, NULL, IOR_SQE_IO_LINK);
	ior_sqe *t = ior_get_sqe(s->ctx);
	assert_non_null(t);
	ior_timespec ts = { .tv_sec = 0, .tv_nsec = 50000000 };
	ior_prep_link_timeout(s->ctx, t, &ts, 0);
	ior_sqe_set_data(s->ctx, t, TAG_TMO);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res);
	assert_int_equal(res[(uintptr_t) TAG_ACCEPT], -ECANCELED);
	assert_int_equal(res[(uintptr_t) TAG_TMO], -ETIME);
}

#ifndef _WIN32
// The accepted socket has exactly the mode the flags ask for, whatever the
// backend did to the listener meanwhile.
static void test_accept_flags(void **state)
{
	ac_state *s = (ac_state *) *state;
	int32_t res[MAX_TAG];

	submit_connect(s);
	ior_sqe *a = ior_get_sqe(s->ctx);
	assert_non_null(a);
	ior_prep_accept(s->ctx, a, s->listener, NULL, NULL, IOR_ACCEPT_NONBLOCK | IOR_ACCEPT_CLOEXEC);
	ior_sqe_set_data(s->ctx, a, TAG_ACCEPT);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res);
	assert_int_equal(res[(uintptr_t) TAG_CONNECT], 0);
	s->accepted = accepted_fd(res[(uintptr_t) TAG_ACCEPT]);
	assert_true(fcntl(s->accepted, F_GETFL, 0) & O_NONBLOCK);
	assert_true(fcntl(s->accepted, F_GETFD, 0) & FD_CLOEXEC);
	test_close_fd(s->accepted);
	test_close_fd(s->client);
	assert_return_code(test_make_tcp_socket(&s->client), 0);

	// And with no flags: blocking, whatever the listener's mode is now.
	submit_connect(s);
	submit_accept(s, NULL, NULL, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res);
	assert_int_equal(res[(uintptr_t) TAG_CONNECT], 0);
	s->accepted = accepted_fd(res[(uintptr_t) TAG_ACCEPT]);
	assert_false(fcntl(s->accepted, F_GETFL, 0) & O_NONBLOCK);
}
#endif

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test_setup_teardown(test_accept_then_connect, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_connect_then_accept, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_connect_refused, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_accept_cancel, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_accept_link_timeout, setup_ac, teardown_ac),
#ifndef _WIN32
		cmocka_unit_test_setup_teardown(test_accept_flags, setup_ac, teardown_ac),
#endif
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
