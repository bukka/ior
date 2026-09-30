/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_fork.c - ior_queue_forget() in a forked child.
 *
 * The parent's context holds every kind of descriptor a backend opens: the
 * ring or completion event, the notification descriptor, the poster ring of
 * work ops, the poller with a multishot poll's dup, a pending signal wait's
 * signalfd and a pending process wait's pidfd. The child forgets the context
 * and must be left with exactly the descriptors it had before the context
 * existed; the parent's pending ops must still complete afterwards. Forked
 * while the poller is busy, forget must not block, and may leave only the
 * multishot polls' dups open.
 */
#include "test_utils.h"
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAX_FDS 1024
#define TAG_WORK ((void *) 0x1)
#define TAG_POLL ((void *) 0x2)
#define TAG_SIG ((void *) 0x3)
#define TAG_WAIT ((void *) 0x4)
#define TAG_ACCEPT ((void *) 0x5)

static void open_fds(char *out)
{
	for (int fd = 0; fd < MAX_FDS; fd++) {
		out[fd] = fcntl(fd, F_GETFD) != -1;
	}
}

static int32_t work_fn(ior_work_token *token, void *arg)
{
	(void) token;
	(void) arg;
	return 1;
}

static void submit(ior_ctx *ctx, ior_sqe *sqe, void *tag)
{
	ior_sqe_set_data(ctx, sqe, tag);
	assert_true(ior_submit(ctx) >= 0);
}

// Reap one completion, which must carry tag.
static int32_t reap(ior_ctx *ctx, void *tag)
{
	ior_cqe *cqe = NULL;
	ior_timespec to = { .tv_sec = 5, .tv_nsec = 0 };
	int ret;
	while ((ret = ior_wait_cqe_timeout(ctx, &cqe, &to)) == -EINTR) { }
	assert_return_code(ret, 0);
	assert_ptr_equal(ior_cqe_get_data(ctx, cqe), tag);
	int32_t res = ior_cqe_get_res(ctx, cqe);
	ior_cqe_seen(ctx, cqe);
	return res;
}

// Forget in a child: 0 when it is left with exactly the descriptors of base.
static int forget_in_child(ior_ctx *ctx, const char *base)
{
	pid_t pid = fork();
	assert_true(pid >= 0);
	if (pid == 0) {
		char before[MAX_FDS];
		char after[MAX_FDS];
		open_fds(before);
		int extra = 0;
		for (int fd = 0; fd < MAX_FDS; fd++) {
			extra += before[fd] && !base[fd];
		}
		if (extra == 0) {
			_exit(2); // nothing inherited to forget: the test is not testing
		}
		if (ior_queue_forget(ctx) != 0) {
			_exit(3);
		}
		open_fds(after);
		for (int fd = 0; fd < MAX_FDS; fd++) {
			if (after[fd] != base[fd]) {
				fprintf(stderr, "fd %d %s after forget\n", fd, after[fd] ? "left open" : "closed");
				_exit(4);
			}
		}
		_exit(0);
	}
	int status;
	assert_int_equal(waitpid(pid, &status, 0), pid);
	assert_true(WIFEXITED(status));
	return WEXITSTATUS(status);
}

static void test_forget_after_fork(void **state)
{
	(void) state;
	int p[2];
	assert_return_code(pipe(p), 0);
	sigset_t set;
	sigset_t saved;
	sigemptyset(&set);
	sigaddset(&set, SIGUSR1);
	assert_return_code(pthread_sigmask(SIG_BLOCK, &set, &saved), 0);
	pid_t kid = fork();
	assert_true(kid >= 0);
	if (kid == 0) {
		usleep(400000);
		_exit(5);
	}

	ior_fd_t listener;
	struct sockaddr_storage laddr;
	socklen_t laddrlen;
	assert_return_code(test_make_listener(&listener, &laddr, &laddrlen), 0);

	char base[MAX_FDS];
	open_fds(base);

	ior_ctx *ctx = NULL;
	assert_return_code(ior_queue_init(32, &ctx), 0);
	assert_true(ior_notify_fd(ctx) != IOR_INVALID_FD);

	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_return_code(ior_prep_work(ctx, sqe, work_fn, NULL), 0);
	submit(ctx, sqe, TAG_WORK);
	assert_int_equal(reap(ctx, TAG_WORK), 1);

	sqe = ior_get_sqe(ctx);
	ior_prep_poll_multishot(ctx, sqe, p[0], IOR_POLL_IN);
	submit(ctx, sqe, TAG_POLL);

	ior_sigset_t sigs;
	ior_siginfo_t info;
	assert_return_code(ior_sigemptyset(&sigs), 0);
	assert_return_code(ior_sigaddset(&sigs, SIGUSR1), 0);
	sqe = ior_get_sqe(ctx);
	assert_return_code(ior_prep_sigwait(ctx, sqe, &sigs, &info), 0);
	submit(ctx, sqe, TAG_SIG);

	int status = -1;
	sqe = ior_get_sqe(ctx);
	assert_return_code(ior_prep_waitpid(ctx, sqe, kid, &status, 0), 0);
	submit(ctx, sqe, TAG_WAIT);

	// A multishot accept: the thread backend's poller watches a dup of the
	// listener for it, as for the poll.
	sqe = ior_get_sqe(ctx);
	ior_prep_accept_multishot(ctx, sqe, listener, IOR_ACCEPT_CLOEXEC);
	submit(ctx, sqe, TAG_ACCEPT);

	// Let the thread backend park the waits and its poller take the poll.
	usleep(100000);

	assert_int_equal(forget_in_child(ctx, base), 0);

	// The parent's ring is untouched: every pending op still completes, in
	// whatever order (a poll(2) poller reports the poll again and again).
	assert_int_equal(write(p[1], "x", 1), 1);
	assert_return_code(kill(getpid(), SIGUSR1), 0);
	ior_fd_t client;
	assert_return_code(test_make_tcp_socket(&client), 0);
	assert_int_equal(connect(client, (const struct sockaddr *) &laddr, laddrlen), 0);
	int got_poll = 0, got_sig = 0, got_wait = 0, got_accept = 0;
	while (!got_poll || !got_sig || !got_wait || !got_accept) {
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = 5, .tv_nsec = 0 };
		int ret;
		while ((ret = ior_wait_cqe_timeout(ctx, &cqe, &to)) == -EINTR) { }
		assert_return_code(ret, 0);
		void *tag = ior_cqe_get_data(ctx, cqe);
		int32_t res = ior_cqe_get_res(ctx, cqe);
		ior_cqe_seen(ctx, cqe);
		if (tag == TAG_POLL) {
			assert_true(res & IOR_POLL_IN);
			got_poll = 1;
		} else if (tag == TAG_ACCEPT) {
			assert_true(res >= 0);
			close(res);
			got_accept = 1;
		} else if (tag == TAG_SIG) {
			assert_int_equal(res, SIGUSR1);
			got_sig = 1;
		} else {
			assert_ptr_equal(tag, TAG_WAIT);
			assert_int_equal(res, (int32_t) kid);
			got_wait = 1;
		}
	}
	assert_true(WIFEXITED(status));
	assert_int_equal(WEXITSTATUS(status), 5);
	// Reported, not collected: the child is still ours to collect.
	assert_int_equal(waitpid(kid, &status, WNOHANG), kid);
	assert_int_equal(WEXITSTATUS(status), 5);

	ior_queue_exit(ctx);
	pthread_sigmask(SIG_SETMASK, &saved, NULL);
	close(client);
	close(listener);
	close(p[0]);
	close(p[1]);
}

#define BUSY_PIPES 8
#define BUSY_FORKS 100

typedef struct busy_state {
	int p[BUSY_PIPES][2];
	_Atomic int stop;
} busy_state;

// Keeps the multishot polls reporting, so the poller thread is often busy
// (and holding its lock) when the test forks. Each pipe is drained and then
// written, so it stays readable until the next round: a byte read back at
// once could be gone before the poller looks, and an edge-triggered watch
// then reports nothing.
static void *busy_writer(void *arg)
{
	busy_state *b = arg;
	char buf[16];
	while (!atomic_load(&b->stop)) {
		for (int i = 0; i < BUSY_PIPES; i++) {
			ssize_t n = read(b->p[i][0], buf, sizeof(buf));
			(void) n;
			n = write(b->p[i][1], "x", 1);
			(void) n;
		}
		usleep(1000);
	}
	return NULL;
}

// Reap a child, which must exit within 10 s: one still running then is
// killed and reported, so a hang fails the test instead of timing it out.
static int reap_child(pid_t pid)
{
	int status;
	for (int i = 0; i < 1000; i++) {
		pid_t r = waitpid(pid, &status, WNOHANG);
		assert_true(r >= 0);
		if (r == pid) {
			return status;
		}
		usleep(10000);
	}
	char path[64];
	char line[256];
	snprintf(path, sizeof(path), "/proc/%d/wchan", (int) pid);
	FILE *f = fopen(path, "r");
	if (f) {
		if (fgets(line, sizeof(line), f)) {
			fprintf(stderr, "stuck child %d wchan: %s\n", (int) pid, line);
		}
		fclose(f);
	}
	snprintf(path, sizeof(path), "/proc/%d/status", (int) pid);
	f = fopen(path, "r");
	if (f) {
		while (fgets(line, sizeof(line), f)) {
			if (strncmp(line, "State:", 6) == 0 || strncmp(line, "SigBlk:", 7) == 0
					|| strncmp(line, "SigIgn:", 7) == 0) {
				fprintf(stderr, "stuck child %d %s", (int) pid, line);
			}
		}
		fclose(f);
	}
	kill(pid, SIGKILL);
	waitpid(pid, &status, 0);
	fail_msg("child %d did not exit within 10 s", (int) pid);
	return -1;
}

// Whether fd is a descriptor of the read end of one of the watched pipes.
static int is_busy_pipe(const busy_state *b, int fd)
{
	struct stat st;
	if (fstat(fd, &st) != 0) {
		return 0;
	}
	for (int i = 0; i < BUSY_PIPES; i++) {
		struct stat pst;
		if (fstat(b->p[i][0], &pst) == 0 && pst.st_dev == st.st_dev && pst.st_ino == st.st_ino) {
			return 1;
		}
	}
	return 0;
}

/*
 * A fork can land while the poller thread holds its lock, which it does
 * whenever it reports an edge: forget must not block on it. It then leaves
 * the multishot polls' dups open, but only those: everything else the
 * context opened is closed either way.
 */
static void test_forget_busy_poller(void **state)
{
	(void) state;
	busy_state b;
	atomic_init(&b.stop, 0);

	char base[MAX_FDS];
	for (int i = 0; i < BUSY_PIPES; i++) {
		assert_return_code(pipe(b.p[i]), 0);
		assert_return_code(fcntl(b.p[i][0], F_SETFL, O_NONBLOCK), 0);
	}
	open_fds(base);

	ior_ctx *ctx = NULL;
	assert_return_code(ior_queue_init(256, &ctx), 0);
	for (int i = 0; i < BUSY_PIPES; i++) {
		ior_sqe *sqe = ior_get_sqe(ctx);
		assert_non_null(sqe);
		ior_prep_poll_multishot(ctx, sqe, b.p[i][0], IOR_POLL_IN);
		ior_sqe_set_data(ctx, sqe, (void *) (uintptr_t) (0x10 + i));
	}
	assert_true(ior_submit(ctx) >= 0);

	pthread_t writer;
	assert_int_equal(pthread_create(&writer, NULL, busy_writer, &b), 0);

	// A worker creates the poller when it takes the first poll: forked
	// before it is published, the child cannot know its descriptors. Every
	// poll having reported an edge, it is up.
	int seen[BUSY_PIPES] = { 0 };
	uint64_t start = test_monotonic_now_ns();
	for (int left = BUSY_PIPES; left > 0;) {
		if (test_monotonic_now_ns() - start > 10000000000ULL) {
			for (int i = 0; i < BUSY_PIPES; i++) {
				if (!seen[i]) {
					fprintf(stderr, "pipe %d never reported\n", i);
				}
			}
			fail_msg("%d polls did not report within 10 s", left);
		}
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = 5, .tv_nsec = 0 };
		int ret;
		while ((ret = ior_wait_cqe_timeout(ctx, &cqe, &to)) == -EINTR) { }
		assert_return_code(ret, 0);
		int i = (int) ((uintptr_t) ior_cqe_get_data(ctx, cqe) - 0x10);
		assert_true(i >= 0 && i < BUSY_PIPES);
		if (!seen[i]) {
			seen[i] = 1;
			left--;
		}
		ior_cqe_seen(ctx, cqe);
	}

	for (int round = 0; round < BUSY_FORKS; round++) {
		// Reap the edges, so the polls rarely end on a full completion queue.
		ior_cqe *cqe = NULL;
		while (ior_peek_cqe(ctx, &cqe) == 0) {
			ior_cqe_seen(ctx, cqe);
		}

		pid_t pid = fork();
		assert_true(pid >= 0);
		if (pid == 0) {
			alarm(5); // a forget that blocks ends the child with SIGALRM
			if (ior_queue_forget(ctx) != 0) {
				_exit(3);
			}
			char after[MAX_FDS];
			open_fds(after);
			for (int fd = 0; fd < MAX_FDS; fd++) {
				if (after[fd] && !base[fd] && !is_busy_pipe(&b, fd)) {
					fprintf(stderr, "fd %d left open after forget\n", fd);
					_exit(4);
				}
			}
			_exit(0);
		}
		int status = reap_child(pid);
		assert_true(WIFEXITED(status));
		assert_int_equal(WEXITSTATUS(status), 0);
	}
	atomic_store(&b.stop, 1);
	assert_int_equal(pthread_join(writer, NULL), 0);

	ior_queue_exit(ctx);
	for (int i = 0; i < BUSY_PIPES; i++) {
		close(b.p[i][0]);
		close(b.p[i][1]);
	}
}

static void test_forget_null(void **state)
{
	(void) state;
	assert_int_equal(ior_queue_forget(NULL), -EINVAL);
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test_forget_after_fork),
		cmocka_unit_test(test_forget_busy_poller),
		cmocka_unit_test(test_forget_null),
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
