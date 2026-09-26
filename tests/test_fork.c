/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_fork.c - ior_queue_forget() in a forked child.
 *
 * The parent's context holds every kind of descriptor a backend opens: the
 * ring or completion event, the notification descriptor, the poster ring of
 * work ops, the poller with a multishot poll's dup, a pending signal wait's
 * signalfd and a pending process wait's pidfd. The child forgets the context
 * and must be left with exactly the descriptors it had before the context
 * existed; the parent's pending ops must still complete afterwards.
 */
#include "test_utils.h"
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAX_FDS 1024
#define TAG_WORK ((void *) 0x1)
#define TAG_POLL ((void *) 0x2)
#define TAG_SIG ((void *) 0x3)
#define TAG_WAIT ((void *) 0x4)

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

	// Let the thread backend park the waits and its poller take the poll.
	usleep(100000);

	assert_int_equal(forget_in_child(ctx, base), 0);

	// The parent's ring is untouched: every pending op still completes, in
	// whatever order (a poll(2) poller reports the poll again and again).
	assert_int_equal(write(p[1], "x", 1), 1);
	assert_return_code(kill(getpid(), SIGUSR1), 0);
	int got_poll = 0, got_sig = 0, got_wait = 0;
	while (!got_poll || !got_sig || !got_wait) {
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

	ior_queue_exit(ctx);
	pthread_sigmask(SIG_SETMASK, &saved, NULL);
	close(p[0]);
	close(p[1]);
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
		cmocka_unit_test(test_forget_null),
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
