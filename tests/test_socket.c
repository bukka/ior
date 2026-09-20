/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_socket.c - Read/write and send/recv coverage over connected stream
 * sockets.
 *
 * These tests use a connected stream socket pair (test_make_socketpair):
 * AF_UNIX socketpair on POSIX, a loopback AF_INET TCP pair on Windows. They
 * exercise plain ior_prep_read / ior_prep_write on socket fds as well as the
 * dedicated ior_prep_send / ior_prep_recv socket operations.
 *
 * NOTE ON IOCP: read/write are routed through ReadFile/WriteFile while
 * send/recv use WSASend/WSARecv. On io_uring the kernel treats socket fds
 * uniformly. The whole suite uses offset 0 because sockets are not seekable and
 * the backend must not treat the offset as a file position for a socket fd.
 */
#include "test_utils.h"
#ifndef _WIN32
#include <sys/socket.h>
#endif

/* Fixture: a ctx plus a connected stream socket pair. sock[0] and sock[1]
 * are the two ends; bytes written to one are readable from the other. */
typedef struct sock_state {
	ior_ctx *ctx;
	ior_fd_t sock[2];
} sock_state;

static int setup_socketpair(void **state)
{
	sock_state *s = calloc(1, sizeof(*s));
	assert_non_null(s);

	int ret = ior_queue_init(32, &s->ctx);
	assert_return_code(ret, 0);
	assert_non_null(s->ctx);

	ret = test_make_socketpair(s->sock);
	assert_return_code(ret, 0);
	assert_true(test_fd_is_valid(s->sock[0]));
	assert_true(test_fd_is_valid(s->sock[1]));

	*state = s;
	return 0;
}

static int teardown_socketpair(void **state)
{
	sock_state *s = (sock_state *) *state;
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

/* Small helper: submit a single op and reap its one completion, returning
 * the res. Asserts the submit and the wait both succeed. */
static int32_t submit_one_and_get_res(ior_ctx *ctx, ior_sqe *sqe, void *tag)
{
	ior_sqe_set_data(ctx, sqe, tag);

	int ret = ior_submit_and_wait(ctx, 1);
	assert_true(ret >= 0);

	ior_cqe *cqe = NULL;
	ret = ior_wait_cqe(ctx, &cqe);
	assert_return_code(ret, 0);
	assert_int_equal((uintptr_t) ior_cqe_get_data(ctx, cqe), (uintptr_t) tag);

	int32_t res = ior_cqe_get_res(ctx, cqe);
	ior_cqe_seen(ctx, cqe);
	return res;
}

/* ===================================================================== */
/* Basic round-trip                                                      */
/* ===================================================================== */

/* Write a payload into sock[0], read it back from sock[1], verify bytes. */
static void test_socket_write_then_read(void **state)
{
	sock_state *s = (sock_state *) *state;

	const char *msg = "socket roundtrip";
	unsigned len = (unsigned) strlen(msg);

	ior_sqe *w = ior_get_sqe(s->ctx);
	assert_non_null(w);
	ior_prep_write(s->ctx, w, s->sock[0], msg, len, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, w, (void *) 0x1), (int32_t) len);

	char buf[64];
	memset(buf, 0, sizeof(buf));

	ior_sqe *r = ior_get_sqe(s->ctx);
	assert_non_null(r);
	ior_prep_read(s->ctx, r, s->sock[1], buf, len, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, r, (void *) 0x2), (int32_t) len);

	assert_memory_equal(buf, msg, len);
}

/* Same in the reverse direction, to confirm the pair is bidirectional and
 * neither end is special-cased. */
static void test_socket_read_other_direction(void **state)
{
	sock_state *s = (sock_state *) *state;

	const char *msg = "reverse path";
	unsigned len = (unsigned) strlen(msg);

	ior_sqe *w = ior_get_sqe(s->ctx);
	assert_non_null(w);
	ior_prep_write(s->ctx, w, s->sock[1], msg, len, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, w, (void *) 0x3), (int32_t) len);

	char buf[64];
	memset(buf, 0, sizeof(buf));

	ior_sqe *r = ior_get_sqe(s->ctx);
	assert_non_null(r);
	ior_prep_read(s->ctx, r, s->sock[0], buf, len, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, r, (void *) 0x4), (int32_t) len);

	assert_memory_equal(buf, msg, len);
}

/* ===================================================================== */
/* Partial read: read buffer smaller than what was written               */
/* ===================================================================== */

/*
 * Write N bytes, then read with a buffer that only holds part of them. A
 * stream read must return only as many bytes as the buffer allows (a "short
 * read"), and a second read must return the remainder. This pins down that
 * the backend honours the requested length rather than over-reading.
 */
static void test_socket_partial_read(void **state)
{
	sock_state *s = (sock_state *) *state;

	const char *msg = "0123456789ABCDEF"; /* 16 bytes */
	unsigned len = (unsigned) strlen(msg);

	ior_sqe *w = ior_get_sqe(s->ctx);
	assert_non_null(w);
	ior_prep_write(s->ctx, w, s->sock[0], msg, len, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, w, (void *) 0x10), (int32_t) len);

	/* First read: only ask for 6 bytes. */
	char buf[32];
	memset(buf, 0, sizeof(buf));

	ior_sqe *r1 = ior_get_sqe(s->ctx);
	assert_non_null(r1);
	ior_prep_read(s->ctx, r1, s->sock[1], buf, 6, 0);
	int32_t got1 = submit_one_and_get_res(s->ctx, r1, (void *) 0x11);
	assert_int_equal(got1, 6);
	assert_memory_equal(buf, "012345", 6);

	/* Second read: the remaining 10 bytes. */
	memset(buf, 0, sizeof(buf));
	ior_sqe *r2 = ior_get_sqe(s->ctx);
	assert_non_null(r2);
	ior_prep_read(s->ctx, r2, s->sock[1], buf, len - 6, 0);
	int32_t got2 = submit_one_and_get_res(s->ctx, r2, (void *) 0x12);
	assert_int_equal(got2, (int32_t) (len - 6));
	assert_memory_equal(buf, "6789ABCDEF", len - 6);
}

/* ===================================================================== */
/* Read after the peer closes -> EOF (res == 0)                          */
/* ===================================================================== */

/*
 * Close one end, then read from the other. On a stream socket this is the
 * orderly-shutdown case: the read must complete with res == 0 (EOF), not an
 * error and not a hang. This is the socket analogue of reading past EOF on a
 * file and is a common real-world path (peer hung up).
 */
static void test_socket_read_after_peer_close(void **state)
{
	sock_state *s = (sock_state *) *state;

	/* Close the write end. */
	test_close_fd(s->sock[0]);
	s->sock[0] = IOR_TEST_INVALID_FD;

	char buf[32];
	memset(buf, 0, sizeof(buf));

	ior_sqe *r = ior_get_sqe(s->ctx);
	assert_non_null(r);
	ior_prep_read(s->ctx, r, s->sock[1], buf, sizeof(buf), 0);
	int32_t res = submit_one_and_get_res(s->ctx, r, (void *) 0x20);

	/* Orderly peer shutdown surfaces as a 0-byte read (EOF). */
	assert_int_equal(res, 0);
}

/* ===================================================================== */
/* send/recv round-trip                                                  */
/* ===================================================================== */

/* Send a payload into sock[0] with ior_prep_send, receive it from sock[1]
 * with ior_prep_recv, and verify the bytes match. */
static void test_socket_send_then_recv(void **state)
{
	sock_state *s = (sock_state *) *state;

	const char *msg = "send/recv roundtrip";
	unsigned len = (unsigned) strlen(msg);

	ior_sqe *snd = ior_get_sqe(s->ctx);
	assert_non_null(snd);
	ior_prep_send(s->ctx, snd, s->sock[0], msg, len, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, snd, (void *) 0x30), (int32_t) len);

	char buf[64];
	memset(buf, 0, sizeof(buf));

	ior_sqe *rcv = ior_get_sqe(s->ctx);
	assert_non_null(rcv);
	ior_prep_recv(s->ctx, rcv, s->sock[1], buf, len, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, rcv, (void *) 0x31), (int32_t) len);

	assert_memory_equal(buf, msg, len);
}

/* Cross-check that send/recv interoperate with write/read: data written with
 * ior_prep_write is readable via ior_prep_recv and vice versa, since they are
 * just different ways to move bytes over the same stream socket. */
static void test_socket_send_recv_interop_with_rw(void **state)
{
	sock_state *s = (sock_state *) *state;

	/* write -> recv */
	const char *msg1 = "written, received";
	unsigned len1 = (unsigned) strlen(msg1);

	ior_sqe *w = ior_get_sqe(s->ctx);
	assert_non_null(w);
	ior_prep_write(s->ctx, w, s->sock[0], msg1, len1, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, w, (void *) 0x40), (int32_t) len1);

	char buf[64];
	memset(buf, 0, sizeof(buf));
	ior_sqe *rcv = ior_get_sqe(s->ctx);
	assert_non_null(rcv);
	ior_prep_recv(s->ctx, rcv, s->sock[1], buf, len1, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, rcv, (void *) 0x41), (int32_t) len1);
	assert_memory_equal(buf, msg1, len1);

	/* send -> read */
	const char *msg2 = "sent, then read";
	unsigned len2 = (unsigned) strlen(msg2);

	ior_sqe *snd = ior_get_sqe(s->ctx);
	assert_non_null(snd);
	ior_prep_send(s->ctx, snd, s->sock[1], msg2, len2, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, snd, (void *) 0x42), (int32_t) len2);

	memset(buf, 0, sizeof(buf));
	ior_sqe *r = ior_get_sqe(s->ctx);
	assert_non_null(r);
	ior_prep_read(s->ctx, r, s->sock[0], buf, len2, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, r, (void *) 0x43), (int32_t) len2);
	assert_memory_equal(buf, msg2, len2);
}

/* recv with a buffer smaller than the sent payload returns a short read; a
 * second recv drains the remainder. Mirrors test_socket_partial_read. */
static void test_socket_partial_recv(void **state)
{
	sock_state *s = (sock_state *) *state;

	const char *msg = "0123456789ABCDEF"; /* 16 bytes */
	unsigned len = (unsigned) strlen(msg);

	ior_sqe *snd = ior_get_sqe(s->ctx);
	assert_non_null(snd);
	ior_prep_send(s->ctx, snd, s->sock[0], msg, len, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, snd, (void *) 0x50), (int32_t) len);

	char buf[32];
	memset(buf, 0, sizeof(buf));

	ior_sqe *r1 = ior_get_sqe(s->ctx);
	assert_non_null(r1);
	ior_prep_recv(s->ctx, r1, s->sock[1], buf, 6, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, r1, (void *) 0x51), 6);
	assert_memory_equal(buf, "012345", 6);

	memset(buf, 0, sizeof(buf));
	ior_sqe *r2 = ior_get_sqe(s->ctx);
	assert_non_null(r2);
	ior_prep_recv(s->ctx, r2, s->sock[1], buf, len - 6, 0);
	assert_int_equal(submit_one_and_get_res(s->ctx, r2, (void *) 0x52), (int32_t) (len - 6));
	assert_memory_equal(buf, "6789ABCDEF", len - 6);
}

/* recv after the peer closes its end completes with res == 0 (EOF), mirroring
 * test_socket_read_after_peer_close but on the send/recv path. */
static void test_socket_recv_after_peer_close(void **state)
{
	sock_state *s = (sock_state *) *state;

	test_close_fd(s->sock[0]);
	s->sock[0] = IOR_TEST_INVALID_FD;

	char buf[32];
	memset(buf, 0, sizeof(buf));

	ior_sqe *rcv = ior_get_sqe(s->ctx);
	assert_non_null(rcv);
	ior_prep_recv(s->ctx, rcv, s->sock[1], buf, sizeof(buf), 0);
	int32_t res = submit_one_and_get_res(s->ctx, rcv, (void *) 0x60);

	assert_int_equal(res, 0);
}

/*
 * A recv on a non-blocking socket with nothing to read must wait for data
 * rather than fail with -EAGAIN: the backend gates the syscall on readiness
 * (io_uring does this natively; the threads backend parks the op on its
 * poller). The peer sends only after the recv has been submitted.
 */
static void test_socket_nonblocking_recv_waits(void **state)
{
	sock_state *s = (sock_state *) *state;
	assert_return_code(test_set_nonblocking(s->sock[1]), 0);

	char buf[32];
	memset(buf, 0, sizeof(buf));

	ior_sqe *rcv = ior_get_sqe(s->ctx);
	assert_non_null(rcv);
	ior_prep_recv(s->ctx, rcv, s->sock[1], buf, sizeof(buf), 0);
	ior_sqe_set_data(s->ctx, rcv, (void *) 0x70);
	assert_true(ior_submit(s->ctx) >= 0);

	// Nothing to read yet: the recv must still be pending.
	ior_timespec to = { .tv_sec = 0, .tv_nsec = 50000000 }; // 50ms
	ior_cqe *cqe = NULL;
	assert_int_equal(ior_wait_cqe_timeout(s->ctx, &cqe, &to), -ETIME);

	const char *msg = "late";
	unsigned len = (unsigned) strlen(msg);
	ior_sqe *snd = ior_get_sqe(s->ctx);
	assert_non_null(snd);
	ior_prep_send(s->ctx, snd, s->sock[0], msg, len, 0);
	ior_sqe_set_data(s->ctx, snd, (void *) 0x71);
	assert_true(ior_submit(s->ctx) >= 0);

	int got_recv = 0, got_send = 0;
	while (!got_recv || !got_send) {
		int ret = ior_wait_cqe(s->ctx, &cqe);
		if (ret == -EINTR) {
			continue;
		}
		assert_return_code(ret, 0);
		uintptr_t tag = (uintptr_t) ior_cqe_get_data(s->ctx, cqe);
		int32_t res = ior_cqe_get_res(s->ctx, cqe);
		if (tag == 0x70) {
			assert_int_equal(res, (int32_t) len);
			assert_memory_equal(buf, msg, len);
			got_recv = 1;
		} else {
			assert_int_equal(tag, 0x71);
			assert_int_equal(res, (int32_t) len);
			got_send = 1;
		}
		ior_cqe_seen(s->ctx, cqe);
	}
}

#ifdef MSG_DONTWAIT
/* MSG_DONTWAIT is honoured as with io_uring: no data means -EAGAIN at once. */
static void test_socket_recv_dontwait(void **state)
{
	sock_state *s = (sock_state *) *state;

	char buf[32];
	ior_sqe *rcv = ior_get_sqe(s->ctx);
	assert_non_null(rcv);
	ior_prep_recv(s->ctx, rcv, s->sock[1], buf, sizeof(buf), MSG_DONTWAIT);
	assert_int_equal(submit_one_and_get_res(s->ctx, rcv, (void *) 0x72), -EAGAIN);
}

/*
 * The same on the send side, which must not park or block a worker once the
 * send buffer is full. macOS reaches -EAGAIN through the readiness probe
 * rather than the flag, which its send(2) ignores.
 */
static void test_socket_send_dontwait(void **state)
{
	sock_state *s = (sock_state *) *state;

	char buf[4096];
	memset(buf, 0x5a, sizeof(buf));

	/* Fill the pipe: nobody reads sock[1], so this ends in -EAGAIN. */
	int32_t res;
	size_t guard = 0;
	do {
		ior_sqe *snd = ior_get_sqe(s->ctx);
		assert_non_null(snd);
		ior_prep_send(s->ctx, snd, s->sock[0], buf, sizeof(buf), MSG_DONTWAIT);
		res = submit_one_and_get_res(s->ctx, snd, (void *) 0x73);
		assert_true(res > 0 || res == -EAGAIN);
		assert_true(++guard < 65536); /* never fills: fail rather than spin */
	} while (res > 0);

	assert_int_equal(res, -EAGAIN);
}
#endif

/*
 * A send larger than the socket buffer completes short rather than occupying
 * its worker until the peer drains. Readiness alone cannot give this: poll()
 * promises only SO_SNDLOWAT bytes of room, while a blocking send does not
 * return until all of len is queued.
 */
static void test_socket_send_larger_than_buffer(void **state)
{
	sock_state *s = (sock_state *) *state;

	int sndbuf = 8192;
	(void) setsockopt(s->sock[0], SOL_SOCKET, SO_SNDBUF, (void *) &sndbuf, sizeof(sndbuf));
	(void) setsockopt(s->sock[1], SOL_SOCKET, SO_RCVBUF, (void *) &sndbuf, sizeof(sndbuf));

	/* Far more than the buffers can hold, and nobody reads sock[1]. */
	size_t len = 4u * 1024 * 1024;
	char *buf = calloc(1, len);
	assert_non_null(buf);

	ior_sqe *snd = ior_get_sqe(s->ctx);
	assert_non_null(snd);
	ior_prep_send(s->ctx, snd, s->sock[0], buf, (unsigned) len, 0);
	int32_t res = submit_one_and_get_res(s->ctx, snd, (void *) 0x74);

	assert_true(res > 0);
	assert_true((size_t) res < len);
	free(buf);
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test_setup_teardown(
				test_socket_write_then_read, setup_socketpair, teardown_socketpair),
		cmocka_unit_test_setup_teardown(
				test_socket_read_other_direction, setup_socketpair, teardown_socketpair),
		cmocka_unit_test_setup_teardown(
				test_socket_partial_read, setup_socketpair, teardown_socketpair),
		cmocka_unit_test_setup_teardown(
				test_socket_read_after_peer_close, setup_socketpair, teardown_socketpair),
		cmocka_unit_test_setup_teardown(
				test_socket_send_then_recv, setup_socketpair, teardown_socketpair),
		cmocka_unit_test_setup_teardown(
				test_socket_send_recv_interop_with_rw, setup_socketpair, teardown_socketpair),
		cmocka_unit_test_setup_teardown(
				test_socket_partial_recv, setup_socketpair, teardown_socketpair),
		cmocka_unit_test_setup_teardown(
				test_socket_recv_after_peer_close, setup_socketpair, teardown_socketpair),
		cmocka_unit_test_setup_teardown(
				test_socket_nonblocking_recv_waits, setup_socketpair, teardown_socketpair),
#ifdef MSG_DONTWAIT
		cmocka_unit_test_setup_teardown(
				test_socket_recv_dontwait, setup_socketpair, teardown_socketpair),
		cmocka_unit_test_setup_teardown(
				test_socket_send_dontwait, setup_socketpair, teardown_socketpair),
#endif
		cmocka_unit_test_setup_teardown(
				test_socket_send_larger_than_buffer, setup_socketpair, teardown_socketpair),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
