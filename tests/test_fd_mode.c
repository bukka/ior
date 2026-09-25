/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_fd_mode.c - descriptor blocking mode across an operation.
 *
 * A descriptor handed to ior comes back in the mode it went in: an op that
 * has to take a blocking descriptor's mode over (the thread backend, for
 * accept, connect and anything RWF_NOWAIT does not cover) restores it before
 * its completion is posted, ops sharing a descriptor share one switch, a
 * cancelled op restores too, and a descriptor the caller keeps non-blocking
 * is never touched. io_uring never changes descriptor state, so every check
 * here holds on it as well. POSIX only: the checks read O_NONBLOCK.
 */
#define _GNU_SOURCE
#include "test_utils.h"
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <poll.h>

#define TAG_A ((void *) 0x1)
#define TAG_B ((void *) 0x2)
#define TAG_CANCEL ((void *) 0x3)

static int is_nonblocking(int fd)
{
	int fl = fcntl(fd, F_GETFL, 0);
	assert_true(fl >= 0);
	return (fl & O_NONBLOCK) != 0;
}

static void set_blocking(int fd, int on)
{
	int fl = fcntl(fd, F_GETFL, 0);
	assert_true(fl >= 0);
	fl = on ? (fl & ~O_NONBLOCK) : (fl | O_NONBLOCK);
	assert_return_code(fcntl(fd, F_SETFL, fl), 0);
}

// The thread backend is the one that switches modes; the others never do.
static int switches_modes(ior_ctx *ctx)
{
	return strcmp(ior_get_backend_name(ctx), "threads") == 0;
}

// Nothing completes within a short while: the op is parked.
static void assert_pending(ior_ctx *ctx)
{
	ior_cqe *cqe = NULL;
	ior_timespec to = { .tv_sec = 0, .tv_nsec = 100 * 1000 * 1000 };
	int ret = ior_wait_cqe_timeout(ctx, &cqe, &to);
	assert_true(ret == -ETIME || ret == -EAGAIN);
}

// Reap one completion, checking its tag.
static int32_t reap(ior_ctx *ctx, void *tag)
{
	for (;;) {
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = 3, .tv_nsec = 0 };
		int ret = ior_wait_cqe_timeout(ctx, &cqe, &to);
		if (ret == -EAGAIN || ret == -EINTR) {
			continue;
		}
		assert_return_code(ret, 0);
		assert_ptr_equal(ior_cqe_get_data(ctx, cqe), tag);
		int32_t res = ior_cqe_get_res(ctx, cqe);
		ior_cqe_seen(ctx, cqe);
		return res;
	}
}

// Reap one completion with either tag; returns the tag, res through *res.
static void *reap_either(ior_ctx *ctx, int32_t *res)
{
	for (;;) {
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = 3, .tv_nsec = 0 };
		int ret = ior_wait_cqe_timeout(ctx, &cqe, &to);
		if (ret == -EAGAIN || ret == -EINTR) {
			continue;
		}
		assert_return_code(ret, 0);
		void *tag = ior_cqe_get_data(ctx, cqe);
		*res = ior_cqe_get_res(ctx, cqe);
		ior_cqe_seen(ctx, cqe);
		return tag;
	}
}

static ior_ctx *make_ctx(uint32_t flags)
{
	ior_params params;
	memset(&params, 0, sizeof(params));
	params.flags = flags;
	ior_ctx *ctx = NULL;
	assert_return_code(ior_queue_init_params(32, &ctx, &params), 0);
	assert_non_null(ctx);
	return ctx;
}

static void submit_read(ior_ctx *ctx, int fd, void *buf, unsigned len, uint64_t off, void *tag)
{
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_read(ctx, sqe, fd, buf, len, off);
	ior_sqe_set_data(ctx, sqe, tag);
	assert_true(ior_submit(ctx) >= 0);
}

static void submit_write(
		ior_ctx *ctx, int fd, const void *buf, unsigned len, uint64_t off, void *tag)
{
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_write(ctx, sqe, fd, buf, len, off);
	ior_sqe_set_data(ctx, sqe, tag);
	assert_true(ior_submit(ctx) >= 0);
}

static void submit_accept(ior_ctx *ctx, int listener, void *tag)
{
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	ior_prep_accept(ctx, sqe, listener, NULL, NULL, 0);
	ior_sqe_set_data(ctx, sqe, tag);
	assert_true(ior_submit(ctx) >= 0);
}

// Fill a pipe's write end until it would block, leaving it blocking.
static void fill_pipe(int wfd)
{
	char chunk[4096];
	memset(chunk, 'x', sizeof(chunk));
	set_blocking(wfd, 0);
	for (;;) {
		ssize_t n = write(wfd, chunk, sizeof(chunk));
		if (n < 0) {
			assert_true(errno == EAGAIN || errno == EWOULDBLOCK);
			break;
		}
	}
	set_blocking(wfd, 1);
}

// Drain a pipe's read end, leaving it blocking.
static void drain_pipe(int rfd)
{
	char chunk[4096];
	set_blocking(rfd, 0);
	for (;;) {
		ssize_t n = read(rfd, chunk, sizeof(chunk));
		if (n < 0) {
			assert_true(errno == EAGAIN || errno == EWOULDBLOCK);
			break;
		}
		assert_true(n > 0);
	}
	set_blocking(rfd, 1);
}

/* A read parked on an empty blocking pipe completes once data arrives, and
 * both ends are still blocking afterwards. */
static void test_read_pipe_restored(void **state)
{
	(void) state;
	ior_ctx *ctx = make_ctx(0);
	int p[2];
	assert_return_code(pipe(p), 0);

	char buf[16];
	submit_read(ctx, p[0], buf, sizeof(buf), IOR_OFF_NONE, TAG_A);
	assert_pending(ctx);
	assert_int_equal(write(p[1], "hello", 5), 5);
	assert_int_equal(reap(ctx, TAG_A), 5);
	assert_memory_equal(buf, "hello", 5);

	assert_false(is_nonblocking(p[0]));
	assert_false(is_nonblocking(p[1]));
	close(p[0]);
	close(p[1]);
	ior_queue_exit(ctx);
}

/* A write parked on a full blocking pipe completes once it is drained, and
 * the write end is still blocking afterwards. */
static void test_write_pipe_restored(void **state)
{
	(void) state;
	ior_ctx *ctx = make_ctx(0);
	int p[2];
	assert_return_code(pipe(p), 0);
	fill_pipe(p[1]);

	submit_write(ctx, p[1], "y", 1, IOR_OFF_NONE, TAG_A);
	assert_pending(ctx);
	drain_pipe(p[0]);
	assert_int_equal(reap(ctx, TAG_A), 1);

	assert_false(is_nonblocking(p[1]));
	assert_false(is_nonblocking(p[0]));
	close(p[0]);
	close(p[1]);
	ior_queue_exit(ctx);
}

/* A read given an offset on a pipe is treated as positionless, as on
 * io_uring, and parks rather than failing with -ESPIPE. */
static void test_positioned_read_on_pipe(void **state)
{
	(void) state;
	ior_ctx *ctx = make_ctx(0);
	int p[2];
	assert_return_code(pipe(p), 0);

	char buf[16];
	submit_read(ctx, p[0], buf, sizeof(buf), 0, TAG_A);
	assert_pending(ctx);
	assert_int_equal(write(p[1], "abc", 3), 3);
	assert_int_equal(reap(ctx, TAG_A), 3);

	assert_false(is_nonblocking(p[0]));
	close(p[0]);
	close(p[1]);
	ior_queue_exit(ctx);
}

/* A pty refuses RWF_NOWAIT, so a read on it is the case where the thread
 * backend must take the mode over: the master is non-blocking while the
 * read is parked and blocking again once it has completed. */
static void test_pty_read_restored(void **state)
{
	(void) state;
	int master = posix_openpt(O_RDWR | O_NOCTTY);
	if (master < 0) {
		skip(); // no pty on this system
	}
	assert_return_code(grantpt(master), 0);
	assert_return_code(unlockpt(master), 0);
	int slave = open(ptsname(master), O_RDWR | O_NOCTTY);
	assert_true(slave >= 0);

	ior_ctx *ctx = make_ctx(0);
	char buf[64];
	submit_read(ctx, master, buf, sizeof(buf), IOR_OFF_NONE, TAG_A);
	assert_pending(ctx);
	if (switches_modes(ctx)) {
		assert_true(is_nonblocking(master));
	}
	assert_int_equal(write(slave, "hi\n", 3), 3);
	assert_true(reap(ctx, TAG_A) > 0);

	assert_false(is_nonblocking(master));
	assert_false(is_nonblocking(slave));
	ior_queue_exit(ctx);
	close(slave);
	close(master);
}

/* Positionless reads and writes on a regular file run to completion and
 * leave its mode alone. */
static void test_regular_file_positionless(void **state)
{
	(void) state;
	char *path = create_temp_file("0123456789", 10);
	assert_non_null(path);
	int fd = open(path, O_RDWR);
	assert_true(fd >= 0);
	ior_ctx *ctx = make_ctx(0);

	char buf[16];
	submit_read(ctx, fd, buf, 4, IOR_OFF_NONE, TAG_A);
	assert_int_equal(reap(ctx, TAG_A), 4);
	assert_memory_equal(buf, "0123", 4);

	submit_write(ctx, fd, "AB", 2, IOR_OFF_NONE, TAG_B);
	assert_int_equal(reap(ctx, TAG_B), 2);
	assert_int_equal(pread(fd, buf, 10, 0), 10);
	assert_memory_equal(buf, "0123AB6789", 10);

	assert_false(is_nonblocking(fd));
	ior_queue_exit(ctx);
	close(fd);
	remove_temp_file(path);
	free(path);
}

/* Accept on a blocking listener and connect from a blocking client: the
 * thread backend switches both while the ops wait, and both are blocking
 * again by the time their completions are reaped; so is the accepted socket. */
static void test_accept_connect_restored(void **state)
{
	(void) state;
	ior_ctx *ctx = make_ctx(0);
	int listener, client;
	struct sockaddr_storage addr;
	socklen_t addrlen;
	assert_return_code(test_make_listener(&listener, &addr, &addrlen), 0);
	assert_return_code(test_make_tcp_socket(&client), 0);

	submit_accept(ctx, listener, TAG_A);
	assert_pending(ctx);
	if (switches_modes(ctx)) {
		assert_true(is_nonblocking(listener));
	}

	ior_sqe *c = ior_get_sqe(ctx);
	assert_non_null(c);
	ior_prep_connect(ctx, c, client, (const struct sockaddr *) &addr, addrlen);
	ior_sqe_set_data(ctx, c, TAG_B);
	assert_true(ior_submit(ctx) >= 0);

	int32_t res;
	int accepted = -1;
	for (int i = 0; i < 2; i++) {
		void *tag = reap_either(ctx, &res);
		if (tag == TAG_A) {
			assert_true(res >= 0);
			accepted = res;
		} else {
			assert_ptr_equal(tag, TAG_B);
			assert_int_equal(res, 0);
		}
	}

	assert_false(is_nonblocking(listener));
	assert_false(is_nonblocking(client));
	assert_false(is_nonblocking(accepted));
	close(accepted);
	close(client);
	close(listener);
	ior_queue_exit(ctx);
}

/* Two accepts parked on one blocking listener share its switch: the first
 * completion leaves the listener non-blocking for the second, and only the
 * last restores it. */
static void test_shared_descriptor(void **state)
{
	(void) state;
	ior_ctx *ctx = make_ctx(0);
	int listener;
	struct sockaddr_storage addr;
	socklen_t addrlen;
	assert_return_code(test_make_listener(&listener, &addr, &addrlen), 0);

	submit_accept(ctx, listener, TAG_A);
	submit_accept(ctx, listener, TAG_B);
	assert_pending(ctx);

	int client1, client2;
	assert_return_code(test_make_tcp_socket(&client1), 0);
	assert_return_code(connect(client1, (const struct sockaddr *) &addr, addrlen), 0);
	int32_t res;
	void *first = reap_either(ctx, &res);
	assert_true(res >= 0);
	close(res);
	if (switches_modes(ctx)) {
		assert_true(is_nonblocking(listener)); // the other accept still holds it
	}

	assert_return_code(test_make_tcp_socket(&client2), 0);
	assert_return_code(connect(client2, (const struct sockaddr *) &addr, addrlen), 0);
	void *second = reap_either(ctx, &res);
	assert_true(res >= 0);
	assert_ptr_not_equal(first, second);
	close(res);

	assert_false(is_nonblocking(listener));
	close(client1);
	close(client2);
	close(listener);
	ior_queue_exit(ctx);
}

/* A cancelled accept restores the listener like a completed one. */
static void test_cancel_restores(void **state)
{
	(void) state;
	ior_ctx *ctx = make_ctx(0);
	int listener;
	struct sockaddr_storage addr;
	socklen_t addrlen;
	assert_return_code(test_make_listener(&listener, &addr, &addrlen), 0);

	submit_accept(ctx, listener, TAG_A);
	assert_pending(ctx);

	ior_sqe *c = ior_get_sqe(ctx);
	assert_non_null(c);
	ior_prep_cancel(ctx, c, TAG_A);
	ior_sqe_set_data(ctx, c, TAG_CANCEL);
	assert_true(ior_submit(ctx) >= 0);

	int32_t res;
	for (int i = 0; i < 2; i++) {
		void *tag = reap_either(ctx, &res);
		if (tag == TAG_A) {
			assert_int_equal(res, -ECANCELED);
		} else {
			assert_ptr_equal(tag, TAG_CANCEL);
			assert_int_equal(res, 0);
		}
	}

	assert_false(is_nonblocking(listener));
	close(listener);
	ior_queue_exit(ctx);
}

/* A listener the caller keeps non-blocking is left that way: ior restores
 * only what it switched. */
static void test_caller_nonblocking_kept(void **state)
{
	(void) state;
	ior_ctx *ctx = make_ctx(0);
	int listener;
	struct sockaddr_storage addr;
	socklen_t addrlen;
	assert_return_code(test_make_listener(&listener, &addr, &addrlen), 0);
	set_blocking(listener, 0);

	submit_accept(ctx, listener, TAG_A);
	assert_pending(ctx);
	int client;
	assert_return_code(test_make_tcp_socket(&client), 0);
	assert_return_code(connect(client, (const struct sockaddr *) &addr, addrlen), 0);
	int32_t res = reap(ctx, TAG_A);
	assert_true(res >= 0);
	close(res);

	assert_true(is_nonblocking(listener));
	close(client);
	close(listener);
	ior_queue_exit(ctx);
}

/* Under IOR_SETUP_FD_NONBLOCK the backend takes the caller's word and never
 * looks at the mode: a non-blocking pipe read parks and completes as usual. */
static void test_setup_flag(void **state)
{
	(void) state;
	ior_ctx *ctx = make_ctx(IOR_SETUP_FD_NONBLOCK);
	int p[2];
	assert_return_code(pipe(p), 0);
	set_blocking(p[0], 0);

	char buf[16];
	submit_read(ctx, p[0], buf, sizeof(buf), IOR_OFF_NONE, TAG_A);
	assert_pending(ctx);
	assert_int_equal(write(p[1], "hello", 5), 5);
	assert_int_equal(reap(ctx, TAG_A), 5);

	assert_true(is_nonblocking(p[0]));
	close(p[0]);
	close(p[1]);
	ior_queue_exit(ctx);
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test_read_pipe_restored),
		cmocka_unit_test(test_write_pipe_restored),
		cmocka_unit_test(test_positioned_read_on_pipe),
		cmocka_unit_test(test_pty_read_restored),
		cmocka_unit_test(test_regular_file_positionless),
		cmocka_unit_test(test_accept_connect_restored),
		cmocka_unit_test(test_shared_descriptor),
		cmocka_unit_test(test_cancel_restores),
		cmocka_unit_test(test_caller_nonblocking_kept),
		cmocka_unit_test(test_setup_flag),
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
