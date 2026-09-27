/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_accept_connect.c - IOR_OP_ACCEPT and IOR_OP_CONNECT on every backend.
 *
 * A listening loopback TCP socket takes connections from sockets connected
 * through ior; the accepted socket must be usable for send/recv (on IOCP
 * that proves the accept context was updated), the peer address must be
 * filled in, a refused connect must fail with -ECONNREFUSED, and a pending
 * accept must be cancellable and bounded by a link timeout. A multishot
 * accept must post one IOR_CQE_F_MORE completion per connection, queued or
 * arriving, apply its flags to each, end without the flag on a cancel, a
 * link timeout or a full completion queue, and lose no connection.
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
#define TAG_ACCEPT2 ((void *) 0x7)
#define TAG_MACCEPT ((void *) 0x8)

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

/*
 * Two accepts parked on one listener, one cancelled: only the target ends
 * -ECANCELED, the other keeps waiting and takes the next connection. On
 * IOCP that exercises the collateral-abort re-issue, since AFD aborts every
 * pending AcceptEx on the listener when one is cancelled.
 */
static void test_accept_two_cancel_one(void **state)
{
	ac_state *s = (ac_state *) *state;
	int32_t res[MAX_TAG];

	submit_accept(s, NULL, NULL, 0);
	ior_sqe *a2 = ior_get_sqe(s->ctx);
	assert_non_null(a2);
	ior_prep_accept(s->ctx, a2, s->listener, NULL, NULL, 0);
	ior_sqe_set_data(s->ctx, a2, TAG_ACCEPT2);
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

	// The other accept is still parked, not failed alongside.
	to.tv_nsec = 50000000;
	assert_int_equal(ior_wait_cqe_timeout(s->ctx, &cqe, &to), -ETIME);

	submit_connect(s);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res);
	assert_int_equal(res[(uintptr_t) TAG_CONNECT], 0);
	s->accepted = accepted_fd(res[(uintptr_t) TAG_ACCEPT2]);
}

// A context torn down with an accept still parked: exit must not hang, and
// the listener must be closable afterwards.
static void test_accept_pending_at_exit(void **state)
{
	ac_state *s = (ac_state *) *state;

	submit_accept(s, NULL, NULL, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	ior_cqe *cqe = NULL;
	ior_timespec to = { .tv_sec = 0, .tv_nsec = 20000000 };
	assert_int_equal(ior_wait_cqe_timeout(s->ctx, &cqe, &to), -ETIME);

	uint64_t start = test_monotonic_now_ns();
	ior_queue_exit(s->ctx);
	s->ctx = NULL;
	assert_true(test_monotonic_now_ns() - start < 2000000000ULL);

	// Teardown closes the listener; the context must not be needed for that.
	assert_return_code(ior_queue_init(32, &s->ctx), 0);
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

/* ================= Multishot accept ================= */

typedef struct cqe_rec {
	void *tag;
	int32_t res;
	uint32_t flags;
} cqe_rec;

// Reap one completion into *r within timeout_ms, or return -ETIME.
static int reap_one(ior_ctx *ctx, cqe_rec *r, int timeout_ms)
{
	for (;;) {
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = timeout_ms / 1000, .tv_nsec = (timeout_ms % 1000) * 1000000L };
		int ret = ior_wait_cqe_timeout(ctx, &cqe, &to);
		if (ret == -EAGAIN || ret == -EINTR) {
			continue;
		}
		if (ret == -ETIME) {
			return -ETIME;
		}
		assert_return_code(ret, 0);
		r->tag = ior_cqe_get_data(ctx, cqe);
		r->res = ior_cqe_get_res(ctx, cqe);
		r->flags = ior_cqe_get_flags(ctx, cqe);
		ior_cqe_seen(ctx, cqe);
		return 0;
	}
}

// A connection of the multishot accept: an edge completion with a socket.
static ior_fd_t reap_maccept(ior_ctx *ctx)
{
	cqe_rec r;
	if (reap_one(ctx, &r, 3000) != 0) {
		fail_msg("no connection reported");
	}
	assert_ptr_equal(r.tag, TAG_MACCEPT);
	assert_true(r.flags & IOR_CQE_F_MORE);
	return accepted_fd(r.res);
}

// The multishot accept's last completion, with res, and nothing else pending.
static void reap_maccept_end(ior_ctx *ctx, int32_t res)
{
	cqe_rec r;
	if (reap_one(ctx, &r, 3000) != 0) {
		fail_msg("no last completion");
	}
	assert_ptr_equal(r.tag, TAG_MACCEPT);
	assert_false(r.flags & IOR_CQE_F_MORE);
	assert_int_equal(r.res, res);
}

static void assert_silent(ior_ctx *ctx, int timeout_ms)
{
	cqe_rec r;
	assert_int_equal(reap_one(ctx, &r, timeout_ms), -ETIME);
}

static void submit_maccept(ac_state *s, unsigned flags, uint8_t sqe_flags)
{
	ior_sqe *a = ior_get_sqe(s->ctx);
	assert_non_null(a);
	ior_prep_accept_multishot(s->ctx, a, s->listener, flags);
	ior_sqe_set_data(s->ctx, a, TAG_MACCEPT);
	if (sqe_flags) {
		ior_sqe_set_flags(s->ctx, a, sqe_flags);
	}
	assert_true(ior_submit(s->ctx) >= 0);
}

// A fresh client connected to the listener (blocking; loopback is at once).
static ior_fd_t connect_client(ac_state *s)
{
	ior_fd_t c;
	assert_return_code(test_make_tcp_socket(&c), 0);
#ifdef _WIN32
	assert_int_equal(connect((SOCKET) c, (const struct sockaddr *) &s->addr, (int) s->addrlen), 0);
#else
	assert_int_equal(connect(c, (const struct sockaddr *) &s->addr, s->addrlen), 0);
#endif
	return c;
}

static void cancel_maccept(ac_state *s, int by_fd)
{
	ior_sqe *c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	if (by_fd) {
		ior_prep_cancel_fd(s->ctx, c, s->listener);
	} else {
		ior_prep_cancel(s->ctx, c, TAG_MACCEPT);
	}
	ior_sqe_set_data(s->ctx, c, TAG_CANCEL);
	assert_true(ior_submit(s->ctx) >= 0);
}

// A cancel's own completion and the accept's last, in whichever order.
static void reap_cancelled(ior_ctx *ctx, int32_t cancel_res)
{
	int got_cancel = 0, got_end = 0;
	while (!got_cancel || !got_end) {
		cqe_rec r;
		if (reap_one(ctx, &r, 3000) != 0) {
			fail_msg("cancel or last completion missing");
		}
		if (r.tag == TAG_CANCEL) {
			assert_int_equal(r.res, cancel_res);
			got_cancel = 1;
		} else {
			assert_ptr_equal(r.tag, TAG_MACCEPT);
			assert_false(r.flags & IOR_CQE_F_MORE);
			assert_int_equal(r.res, -ECANCELED);
			got_end = 1;
		}
	}
}

/*
 * Connections arriving one by one: each is one edge completion, the
 * accepted socket works for I/O both ways, and a cancel ends the operation
 * with -ECANCELED and no IOR_CQE_F_MORE. The listener comes back blocking.
 */
static void test_accept_multishot_stream(void **state)
{
	ac_state *s = (ac_state *) *state;
	int32_t res[MAX_TAG];

	submit_maccept(s, 0, 0);
	assert_silent(s->ctx, 20);

	ior_fd_t clients[3];
	ior_fd_t accepted[3];
	for (int i = 0; i < 3; i++) {
		clients[i] = connect_client(s);
		accepted[i] = reap_maccept(s->ctx);
	}
	assert_silent(s->ctx, 20);

	const char *msg = "over multishot";
	unsigned len = (unsigned) strlen(msg);
	char buf[32];
	for (int dir = 0; dir < 2; dir++) {
		ior_fd_t from = dir ? accepted[2] : clients[2];
		ior_fd_t to = dir ? clients[2] : accepted[2];
		memset(buf, 0, sizeof(buf));
		ior_sqe *snd = ior_get_sqe(s->ctx);
		assert_non_null(snd);
		ior_prep_send(s->ctx, snd, from, msg, len, 0);
		ior_sqe_set_data(s->ctx, snd, TAG_SEND);
		ior_sqe *rcv = ior_get_sqe(s->ctx);
		assert_non_null(rcv);
		ior_prep_recv(s->ctx, rcv, to, buf, sizeof(buf), 0);
		ior_sqe_set_data(s->ctx, rcv, TAG_RECV);
		assert_true(ior_submit(s->ctx) >= 0);
		reap_tags(s->ctx, 2, res);
		assert_int_equal(res[(uintptr_t) TAG_SEND], (int32_t) len);
		assert_int_equal(res[(uintptr_t) TAG_RECV], (int32_t) len);
		assert_memory_equal(buf, msg, len);
	}

	cancel_maccept(s, 0);
	reap_cancelled(s->ctx, 0);
#ifndef _WIN32
	// Restored by the thread backend, never touched elsewhere.
	assert_false(fcntl(s->listener, F_GETFL, 0) & O_NONBLOCK);
#endif
	for (int i = 0; i < 3; i++) {
		test_close_fd(accepted[i]);
		test_close_fd(clients[i]);
	}
}

/*
 * Connections queued before the submit are all reported (one edge, several
 * accepts on the thread backend), a later one too, and cancel by descriptor
 * ends it.
 */
static void test_accept_multishot_backlog(void **state)
{
	ac_state *s = (ac_state *) *state;
	ior_fd_t clients[6];
	ior_fd_t accepted[6];

	for (int i = 0; i < 5; i++) {
		clients[i] = connect_client(s);
	}
	submit_maccept(s, 0, 0);
	for (int i = 0; i < 5; i++) {
		accepted[i] = reap_maccept(s->ctx);
	}
	assert_silent(s->ctx, 50);

	clients[5] = connect_client(s);
	accepted[5] = reap_maccept(s->ctx);

	cancel_maccept(s, 1);
	reap_cancelled(s->ctx, 0);
	for (int i = 0; i < 6; i++) {
		test_close_fd(accepted[i]);
		test_close_fd(clients[i]);
	}
}

#ifndef _WIN32
// Every accepted socket gets exactly the flags asked for.
static void test_accept_multishot_flags(void **state)
{
	ac_state *s = (ac_state *) *state;

	submit_maccept(s, IOR_ACCEPT_NONBLOCK | IOR_ACCEPT_CLOEXEC, 0);
	for (int i = 0; i < 2; i++) {
		ior_fd_t c = connect_client(s);
		ior_fd_t a = reap_maccept(s->ctx);
		assert_true(fcntl(a, F_GETFL, 0) & O_NONBLOCK);
		assert_true(fcntl(a, F_GETFD, 0) & FD_CLOEXEC);
		test_close_fd(a);
		test_close_fd(c);
	}
	cancel_maccept(s, 0);
	reap_cancelled(s->ctx, 0);

	// And none: blocking, whatever the listener's mode is meanwhile.
	submit_maccept(s, 0, 0);
	ior_fd_t c = connect_client(s);
	ior_fd_t a = reap_maccept(s->ctx);
	assert_false(fcntl(a, F_GETFL, 0) & O_NONBLOCK);
	assert_false(fcntl(a, F_GETFD, 0) & FD_CLOEXEC);
	test_close_fd(a);
	test_close_fd(c);
	cancel_maccept(s, 0);
	reap_cancelled(s->ctx, 0);
}
#endif

/*
 * A link timeout bounds the whole operation: a connection before the
 * deadline is reported, then the accept ends -ECANCELED and the timeout
 * -ETIME.
 */
static void test_accept_multishot_link_timeout(void **state)
{
	ac_state *s = (ac_state *) *state;

	ior_fd_t c = connect_client(s);
	ior_sqe *a = ior_get_sqe(s->ctx);
	assert_non_null(a);
	ior_prep_accept_multishot(s->ctx, a, s->listener, 0);
	ior_sqe_set_data(s->ctx, a, TAG_MACCEPT);
	ior_sqe_set_flags(s->ctx, a, IOR_SQE_IO_LINK);
	ior_sqe *t = ior_get_sqe(s->ctx);
	assert_non_null(t);
	ior_timespec ts = { .tv_sec = 0, .tv_nsec = 100000000 };
	ior_prep_link_timeout(s->ctx, t, &ts, 0);
	ior_sqe_set_data(s->ctx, t, TAG_TMO);
	assert_true(ior_submit(s->ctx) >= 0);

	ior_fd_t accepted = reap_maccept(s->ctx);
	int got_end = 0, got_tmo = 0;
	while (!got_end || !got_tmo) {
		cqe_rec r;
		if (reap_one(s->ctx, &r, 3000) != 0) {
			fail_msg("timeout or last completion missing");
		}
		if (r.tag == TAG_TMO) {
			assert_int_equal(r.res, -ETIME);
			got_tmo = 1;
		} else {
			assert_ptr_equal(r.tag, TAG_MACCEPT);
			assert_false(r.flags & IOR_CQE_F_MORE);
			assert_int_equal(r.res, -ECANCELED);
			got_end = 1;
		}
	}
	test_close_fd(accepted);
	test_close_fd(c);
}

/*
 * A link timeout firing while connections stream in: whenever the deadline
 * lands relative to an accept being reported and the next one being armed,
 * the operation must still end (-ECANCELED, no IOR_CQE_F_MORE) once the
 * timeout has fired, without waiting for yet another connection. Connections
 * are made one at a time, each reported before the next, so that at the
 * deadline the backlog is usually empty: an accept armed after the timeout
 * fired, and missed by it, would then hang the operation. What a round
 * leaves in the backlog (the connection made right before the timeout was
 * reaped, at least) is drained before the next round: left there, it would
 * be reported by the next round's accept ahead of that round's own
 * connections (Linux and macOS hand a connection over even once its peer
 * has reset it), which then delays their report by one, and so leaves one
 * more behind every round.
 */
// Close abortively (RST, no TIME_WAIT): the test makes thousands of
// connections, more than the ephemeral port range holds in TIME_WAIT. On
// Windows only closesocket() honours the linger option (CloseHandle() goes
// to the driver directly and closes gracefully).
static void close_abortive(ior_fd_t fd)
{
	struct linger lg = { .l_onoff = 1, .l_linger = 0 };
#ifdef _WIN32
	setsockopt((SOCKET) fd, SOL_SOCKET, SO_LINGER, (const char *) &lg, sizeof(lg));
	closesocket((SOCKET) fd);
#else
	setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
	test_close_fd(fd);
#endif
}

// Accept and close whatever is queued on the listener right now.
static void drain_listener(ac_state *s)
{
	while (test_wait_readable(s->listener, 0) == 1) {
#ifdef _WIN32
		SOCKET a = accept((SOCKET) s->listener, NULL, NULL);
		if (a == INVALID_SOCKET) {
			break;
		}
		test_close_fd((ior_fd_t) a);
#else
		int a = accept(s->listener, NULL, NULL);
		if (a < 0) {
			break;
		}
		test_close_fd(a);
#endif
	}
}

static void test_accept_multishot_link_timeout_under_load(void **state)
{
	ac_state *s = (ac_state *) *state;
	enum { ROUNDS = 200, MAX_CLIENTS = 32 };
	ior_fd_t fds[2 * MAX_CLIENTS + 8];

	for (int round = 0; round < ROUNDS; round++) {
		int nfds = 0;
		int clients = 0;
		ior_sqe *a = ior_get_sqe(s->ctx);
		assert_non_null(a);
		ior_prep_accept_multishot(s->ctx, a, s->listener, 0);
		ior_sqe_set_data(s->ctx, a, TAG_MACCEPT);
		ior_sqe_set_flags(s->ctx, a, IOR_SQE_IO_LINK);
		ior_sqe *t = ior_get_sqe(s->ctx);
		assert_non_null(t);
		ior_timespec ts = { .tv_sec = 0, .tv_nsec = (1 + round % 3) * 500000L };
		ior_prep_link_timeout(s->ctx, t, &ts, 0);
		ior_sqe_set_data(s->ctx, t, TAG_TMO);
		assert_true(ior_submit(s->ctx) >= 0);

		int got_end = 0, got_tmo = 0;
		while (!got_end || !got_tmo) {
			if (!got_tmo && !got_end && clients < MAX_CLIENTS) {
				fds[nfds++] = connect_client(s);
				clients++;
			}
			cqe_rec r;
			if (reap_one(s->ctx, &r, 3000) != 0) {
				fail_msg("round %d: no completion after %d connections (timeout %d, end %d)",
						round, clients, got_tmo, got_end);
			}
			if (r.tag == TAG_TMO) {
				assert_int_equal(r.res, -ETIME);
				got_tmo = 1;
			} else {
				assert_ptr_equal(r.tag, TAG_MACCEPT);
				if (r.flags & IOR_CQE_F_MORE) {
					assert_false(got_end);
					assert_true(nfds < (int) (sizeof(fds) / sizeof(fds[0])));
					fds[nfds++] = accepted_fd(r.res);
				} else {
					assert_int_equal(r.res, -ECANCELED);
					got_end = 1;
				}
			}
		}
		drain_listener(s);
		for (int i = 0; i < nfds; i++) {
			close_abortive(fds[i]);
		}
	}
}

/*
 * A cancel right behind the submit, with connections queued: whatever the
 * accept reported before its -ECANCELED is the caller's, and the rest is
 * still in the backlog for a one-shot accept. No connection is lost.
 */
static void test_accept_multishot_cancel_keeps_connections(void **state)
{
	ac_state *s = (ac_state *) *state;
	int32_t res[MAX_TAG];
	enum { N = 3 };
	ior_fd_t clients[N];
	ior_fd_t accepted[N];

	for (int i = 0; i < N; i++) {
		clients[i] = connect_client(s);
	}
	submit_maccept(s, 0, 0);
	cancel_maccept(s, 0);

	int n = 0;
	int got_cancel = 0, got_end = 0;
	while (!got_cancel || !got_end) {
		cqe_rec r;
		if (reap_one(s->ctx, &r, 3000) != 0) {
			fail_msg("cancel or last completion missing");
		}
		if (r.tag == TAG_CANCEL) {
			// -ENOENT only if the accept ended first, which it cannot here.
			assert_true(r.res == 0 || r.res == -EALREADY);
			got_cancel = 1;
		} else {
			assert_ptr_equal(r.tag, TAG_MACCEPT);
			if (r.flags & IOR_CQE_F_MORE) {
				assert_false(got_end);
				assert_true(n < N);
				accepted[n++] = accepted_fd(r.res);
			} else {
				assert_int_equal(r.res, -ECANCELED);
				got_end = 1;
			}
		}
	}
	assert_silent(s->ctx, 20);

	for (; n < N; n++) {
		submit_accept(s, NULL, NULL, 0);
		assert_true(ior_submit(s->ctx) >= 0);
		reap_tags(s->ctx, 1, res);
		accepted[n] = accepted_fd(res[(uintptr_t) TAG_ACCEPT]);
	}
	for (int i = 0; i < N; i++) {
		test_close_fd(accepted[i]);
		test_close_fd(clients[i]);
	}
}

/*
 * A socket that is not listening fails the operation at once with -EINVAL,
 * as accept(2) does, and without IOR_CQE_F_MORE: the thread backend must not
 * park it on its poller, where nothing would ever wake it.
 */
static void test_accept_multishot_not_listening(void **state)
{
	ac_state *s = (ac_state *) *state;
	ior_fd_t bound;
	assert_return_code(test_make_tcp_socket(&bound), 0);
	struct sockaddr_in a;
	memset(&a, 0, sizeof(a));
	a.sin_family = AF_INET;
	a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
#ifdef _WIN32
	assert_int_equal(bind((SOCKET) bound, (struct sockaddr *) &a, sizeof(a)), 0);
#else
	assert_int_equal(bind(bound, (struct sockaddr *) &a, sizeof(a)), 0);
#endif

	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	ior_prep_accept_multishot(s->ctx, sqe, bound, 0);
	ior_sqe_set_data(s->ctx, sqe, TAG_MACCEPT);
	assert_true(ior_submit(s->ctx) >= 0);

	cqe_rec r;
	if (reap_one(s->ctx, &r, 3000) != 0) {
		fail_msg("no completion for a socket that is not listening");
	}
	assert_ptr_equal(r.tag, TAG_MACCEPT);
	assert_false(r.flags & IOR_CQE_F_MORE);
	assert_int_equal(r.res, -EINVAL);
	assert_silent(s->ctx, 20);
	test_close_fd(bound);
}

// Torn down with a multishot accept armed: exit must not hang.
static void test_accept_multishot_pending_at_exit(void **state)
{
	ac_state *s = (ac_state *) *state;

	submit_maccept(s, 0, 0);
	assert_silent(s->ctx, 20);

	uint64_t start = test_monotonic_now_ns();
	ior_queue_exit(s->ctx);
	s->ctx = NULL;
	assert_true(test_monotonic_now_ns() - start < 2000000000ULL);
	assert_return_code(ior_queue_init(32, &s->ctx), 0);
}

/*
 * A connection that finds the completion queue full ends the operation on
 * io_uring and the thread backend: it is the last completion, with the
 * socket and no IOR_CQE_F_MORE, and what is still queued waits for the next
 * accept. Where exactly it ends is the kernel's on io_uring: one that caches
 * completions before posting them (6.8) notices the full queue one
 * connection later than the thread backend, which ends at cq + 1. IOCP
 * keeps a few accepts out and has no such end.
 */
static void test_accept_multishot_cq_full(void **state)
{
	ac_state *s = (ac_state *) *state;
	int32_t res[MAX_TAG];

	if (ior_get_backend_type(s->ctx) == IOR_BACKEND_IOCP) {
		skip();
	}
	unsigned cq = ior_cq_entries(s->ctx);
	assert_true(cq >= 8 && cq <= 512);
	unsigned n = cq + 4;
	ior_fd_t *clients = calloc(n, sizeof(*clients));
	ior_fd_t *accepted = calloc(n, sizeof(*accepted));
	assert_non_null(clients);
	assert_non_null(accepted);

	submit_maccept(s, 0, 0);
	for (unsigned i = 0; i < n; i++) {
		clients[i] = connect_client(s);
	}

	// cq edges fill the queue; the next connection ends the operation.
	unsigned got = 0;
	for (;;) {
		cqe_rec r;
		if (reap_one(s->ctx, &r, 3000) != 0) {
			fail_msg("last completion missing after %u connections", got);
		}
		assert_ptr_equal(r.tag, TAG_MACCEPT);
		assert_true(got < n);
		accepted[got++] = accepted_fd(r.res);
		if (!(r.flags & IOR_CQE_F_MORE)) {
			break;
		}
	}
	assert_true(got >= cq + 1 && got <= cq + 3);
	assert_silent(s->ctx, 20);

	// The remaining connections are still there for one-shot accepts.
	while (got < n) {
		submit_accept(s, NULL, NULL, 0);
		assert_true(ior_submit(s->ctx) >= 0);
		reap_tags(s->ctx, 1, res);
		accepted[got++] = accepted_fd(res[(uintptr_t) TAG_ACCEPT]);
	}

	for (unsigned i = 0; i < n; i++) {
		test_close_fd(accepted[i]);
		test_close_fd(clients[i]);
	}
	free(clients);
	free(accepted);
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test_setup_teardown(test_accept_then_connect, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_connect_then_accept, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_connect_refused, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_accept_cancel, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_accept_two_cancel_one, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_accept_pending_at_exit, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_accept_link_timeout, setup_ac, teardown_ac),
#ifndef _WIN32
		cmocka_unit_test_setup_teardown(test_accept_flags, setup_ac, teardown_ac),
#endif
		cmocka_unit_test_setup_teardown(test_accept_multishot_stream, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_accept_multishot_backlog, setup_ac, teardown_ac),
#ifndef _WIN32
		cmocka_unit_test_setup_teardown(test_accept_multishot_flags, setup_ac, teardown_ac),
#endif
		cmocka_unit_test_setup_teardown(test_accept_multishot_link_timeout, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(
				test_accept_multishot_link_timeout_under_load, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(
				test_accept_multishot_cancel_keeps_connections, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_accept_multishot_not_listening, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(
				test_accept_multishot_pending_at_exit, setup_ac, teardown_ac),
		cmocka_unit_test_setup_teardown(test_accept_multishot_cq_full, setup_ac, teardown_ac),
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
