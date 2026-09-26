/* SPDX-License-Identifier: BSD-3-Clause */
#include "test_utils.h"

/*
 * With both Linux backends built, IOR_BACKEND_AUTO falls back to the thread
 * backend when io_uring is unusable. A seccomp filter in a forked child makes
 * io_uring_setup fail, as a container's default profile or the
 * io_uring_disabled sysctl would.
 */
#if defined(IOR_HAVE_URING) && defined(IOR_HAVE_THREADS)

#include <linux/filter.h>
#include <linux/seccomp.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/wait.h>

static int block_io_uring_setup(int err)
{
	struct sock_filter filter[] = {
		BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
		BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_io_uring_setup, 0, 1),
		BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (err & SECCOMP_RET_DATA)),
		BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
	};
	struct sock_fprog prog = {
		.len = sizeof(filter) / sizeof(filter[0]),
		.filter = filter,
	};
	if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) < 0) {
		return -1;
	}
	return prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog);
}

/*
 * Init a context in a child with io_uring_setup failing with err (0: not
 * blocked). Returns the backend it got, or 100 + errno of a failed init
 * (200+ when the child itself fails).
 */
static int init_blocked(int err, const char *env, ior_backend_type backend)
{
	pid_t pid = fork();
	assert_true(pid >= 0);
	if (pid == 0) {
		if (err && block_io_uring_setup(err) < 0) {
			_exit(200);
		}
		if (env) {
			setenv("IOR_BACKEND", env, 1);
		} else {
			unsetenv("IOR_BACKEND");
		}
		ior_ctx *ctx;
		ior_params params = { .backend = backend };
		int ret = ior_queue_init_params(8, &ctx, &params);
		if (ret < 0) {
			_exit(100 + (-ret < 99 ? -ret : 99));
		}
		/* The context must work, not just exist. */
		ior_sqe *sqe = ior_get_sqe(ctx);
		ior_cqe *cqe;
		if (!sqe) {
			_exit(201);
		}
		ior_prep_nop(ctx, sqe);
		if (ior_submit_and_wait(ctx, 1) < 0 || ior_wait_cqe(ctx, &cqe) < 0) {
			_exit(202);
		}
		ior_cqe_seen(ctx, cqe);
		int type = ior_get_backend_type(ctx);
		ior_queue_exit(ctx);
		_exit(type);
	}
	int status;
	assert_int_equal(waitpid(pid, &status, 0), pid);
	assert_true(WIFEXITED(status));
	return WEXITSTATUS(status);
}

static void test_auto_falls_back_on_eperm(void **state)
{
	(void) state;
	assert_int_equal(init_blocked(EPERM, NULL, IOR_BACKEND_AUTO), IOR_BACKEND_THREADS);
}

static void test_auto_falls_back_on_enosys(void **state)
{
	(void) state;
	assert_int_equal(init_blocked(ENOSYS, NULL, IOR_BACKEND_AUTO), IOR_BACKEND_THREADS);
}

// Other errors are not "io_uring unusable" and surface.
static void test_auto_keeps_other_errors(void **state)
{
	(void) state;
	assert_int_equal(init_blocked(EINVAL, NULL, IOR_BACKEND_AUTO), 100 + EINVAL);
}

// An explicit choice, by parameter or environment, never falls back.
static void test_explicit_uring_is_strict(void **state)
{
	(void) state;
	assert_int_equal(init_blocked(EPERM, NULL, IOR_BACKEND_IOURING), 100 + EPERM);
	assert_int_equal(init_blocked(EPERM, "io_uring", IOR_BACKEND_AUTO), 100 + EPERM);
}

// Unblocked, AUTO still picks io_uring.
static void test_auto_prefers_uring(void **state)
{
	(void) state;
	assert_int_equal(init_blocked(0, NULL, IOR_BACKEND_AUTO), IOR_BACKEND_IOURING);
}

#endif

int main(void)
{
#if defined(IOR_HAVE_URING) && defined(IOR_HAVE_THREADS)
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test_auto_falls_back_on_eperm),
		cmocka_unit_test(test_auto_falls_back_on_enosys),
		cmocka_unit_test(test_auto_keeps_other_errors),
		cmocka_unit_test(test_explicit_uring_is_strict),
		cmocka_unit_test(test_auto_prefers_uring),
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
#else
	printf("backend fallback tests require both io_uring and thread backends\n");
	return 0;
#endif
}
