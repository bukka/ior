/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_waitpid.c - IOR_OP_WAITPID on every backend.
 *
 * Children are spawned by the test (fork on POSIX, the test binary re-run
 * with --child on Windows) and waited for through ior: a child that exited
 * before the op was submitted, one that exits while the op is pending, a
 * batch of them, cancellation, a link timeout, teardown with a wait still
 * pending, and on POSIX the waitpid extras: WNOHANG, a killed child, any
 * child (-1) on a worker, and -ECHILD for a process that is not a child.
 * ior reports a child and never collects it, so on POSIX every report is
 * followed by the test's own waitpid(WNOHANG), which must find the child
 * with the status ior gave.
 */
#include "test_utils.h"
#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#include <sys/wait.h>
#endif

#define TAG_WAIT ((void *) 0x1)
#define TAG_TMO ((void *) 0x2)
#define TAG_CANCEL ((void *) 0x3)
#define TAG_WAIT2 ((void *) 0x4)
#define TAG_BATCH ((void *) 0x8) /* + index, up to MAX_KIDS */
#define MAX_KIDS 8
#define MAX_TAG (0x8 + MAX_KIDS)

typedef struct kid {
	ior_pid_t pid;
	int code;
	int reaped; /* collected by the test (Windows: reported) */
#ifdef _WIN32
	HANDLE handle; /* keeps the pid from being recycled under the test */
#endif
} kid;

typedef struct wp_state {
	ior_ctx *ctx;
	int nkids;
	kid kids[MAX_KIDS];
} wp_state;

/* Start a child that sleeps sleep_ms and exits with code. */
static kid *spawn_child(wp_state *s, int code, int sleep_ms)
{
	assert_true(s->nkids < MAX_KIDS);
	kid *k = &s->kids[s->nkids++];
	k->code = code;
	k->reaped = 0;
#ifdef _WIN32
	char path[MAX_PATH];
	assert_true(GetModuleFileNameA(NULL, path, sizeof(path)) > 0);
	char cmd[MAX_PATH + 64];
	snprintf(cmd, sizeof(cmd), "\"%s\" --child %d %d", path, code, sleep_ms);
	STARTUPINFOA si;
	PROCESS_INFORMATION pi;
	memset(&si, 0, sizeof(si));
	si.cb = sizeof(si);
	assert_true(
			CreateProcessA(path, cmd, NULL, NULL, FALSE, CREATE_NO_WINDOW, NULL, NULL, &si, &pi));
	CloseHandle(pi.hThread);
	k->handle = pi.hProcess;
	k->pid = pi.dwProcessId;
#else
	pid_t pid = fork();
	assert_true(pid >= 0);
	if (pid == 0) {
		if (sleep_ms > 0) {
			usleep((useconds_t) sleep_ms * 1000);
		}
		_exit(code);
	}
	k->pid = pid;
#endif
	return k;
}

static kid *find_kid(wp_state *s, int32_t res)
{
	for (int i = 0; i < s->nkids; i++) {
		if ((int32_t) s->kids[i].pid == res) {
			return &s->kids[i];
		}
	}
	return NULL;
}

static void kill_child(kid *k)
{
#ifdef _WIN32
	TerminateProcess(k->handle, 9);
#else
	kill(k->pid, SIGKILL);
#endif
}

/* The child exited normally with its code. */
static void assert_exited(kid *k, int status)
{
#ifdef _WIN32
	assert_int_equal(status, k->code);
#else
	assert_true(WIFEXITED(status));
	assert_int_equal(WEXITSTATUS(status), k->code);
#endif
}

static int setup_wp(void **state)
{
	wp_state *s = calloc(1, sizeof(*s));
	assert_non_null(s);
	assert_return_code(ior_queue_init(32, &s->ctx), 0);
	*state = s;
	return 0;
}

static int teardown_wp(void **state)
{
	wp_state *s = (wp_state *) *state;
	if (!s) {
		return 0;
	}
	if (s->ctx) {
		ior_queue_exit(s->ctx);
	}
	// Whatever the test did not collect is killed and collected here.
	for (int i = 0; i < s->nkids; i++) {
		kid *k = &s->kids[i];
#ifdef _WIN32
		if (!k->reaped) {
			kill_child(k);
			WaitForSingleObject(k->handle, 5000);
		}
		CloseHandle(k->handle);
#else
		if (!k->reaped) {
			kill_child(k);
			int status;
			(void) waitpid(k->pid, &status, 0);
		}
#endif
	}
	free(s);
	return 0;
}

// Reap n completions into res[] by tag, failing fast if one never arrives.
static void reap_tags(ior_ctx *ctx, int n, int32_t res[MAX_TAG], int timeout_s)
{
	char seen[MAX_TAG] = { 0 };
	for (int got = 0; got < n;) {
		ior_cqe *cqe = NULL;
		ior_timespec to = { .tv_sec = timeout_s, .tv_nsec = 0 };
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

static void submit_wait(
		wp_state *s, ior_pid_t pid, int *status, int options, void *tag, uint8_t flags)
{
	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	assert_return_code(ior_prep_waitpid(s->ctx, sqe, pid, status, options), 0);
	ior_sqe_set_data(s->ctx, sqe, tag);
	if (flags) {
		ior_sqe_set_flags(s->ctx, sqe, flags);
	}
}

#ifndef _WIN32
/*
 * Collect a child ior reported: it must still be waitable, and its state
 * must be what the report said. Nothing to collect on Windows.
 */
static void collect(kid *k, int status, int options)
{
	int st = -1;
	assert_int_equal(k->reaped, 0);
	assert_int_equal(waitpid(k->pid, &st, options | WNOHANG), k->pid);
	assert_int_equal(st, status);
	if (!(options & (WUNTRACED | WCONTINUED))) {
		k->reaped = 1;
	}
}
#else
static void collect(kid *k, int status, int options)
{
	(void) status;
	(void) options;
	assert_int_equal(k->reaped, 0);
	k->reaped = 1;
}
#endif

/*
 * A reported pid must be one of ours, with the status it exited with, and
 * still ours to collect.
 */
static kid *check_reported(wp_state *s, int32_t res, int status)
{
	kid *k = find_kid(s, res);
	assert_non_null(k);
	assert_exited(k, status);
	collect(k, status, 0);
	return k;
}

static void sleep_ms(int ms)
{
#ifdef _WIN32
	Sleep((DWORD) ms);
#else
	usleep((useconds_t) ms * 1000);
#endif
}

// Already exited when the op is submitted: reported without waiting.
static void test_waitpid_exited(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status = -1;

	kid *k = spawn_child(s, 7, 0);
	sleep_ms(100);
	submit_wait(s, k->pid, &status, 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	check_reported(s, res[(uintptr_t) TAG_WAIT], status);
}

// Exits while the op is pending: nothing completes before, the pid after.
static void test_waitpid_pending(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status = -1;

	kid *k = spawn_child(s, 3, 300);
	submit_wait(s, k->pid, &status, 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	ior_cqe *cqe = NULL;
	ior_timespec to = { .tv_sec = 0, .tv_nsec = 50000000 };
	assert_int_equal(ior_wait_cqe_timeout(s->ctx, &cqe, &to), -ETIME);

	reap_tags(s->ctx, 1, res, 3);
	check_reported(s, res[(uintptr_t) TAG_WAIT], status);
}

// A NULL status is allowed.
static void test_waitpid_null_status(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];

	kid *k = spawn_child(s, 0, 50);
	submit_wait(s, k->pid, NULL, 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], (int32_t) k->pid);
#ifndef _WIN32
	int status = -1;
	assert_int_equal(waitpid(k->pid, &status, WNOHANG), k->pid);
	assert_exited(k, status);
#endif
	k->reaped = 1;
}

// Several children at once, exiting in no particular order, each op gets
// its own child's pid and status.
static void test_waitpid_many(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status[MAX_KIDS];
	const int n = 6;

	for (int i = 0; i < n; i++) {
		spawn_child(s, i + 1, (i * 37) % 200);
	}
	for (int i = 0; i < n; i++) {
		status[i] = -1;
		submit_wait(s, s->kids[i].pid, &status[i], 0, (char *) TAG_BATCH + i, 0);
	}
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, n, res, 5);
	for (int i = 0; i < n; i++) {
		int32_t r = res[(uintptr_t) TAG_BATCH + i];
		assert_int_equal(r, (int32_t) s->kids[i].pid);
		check_reported(s, r, status[i]);
	}
}

// A pending wait is cancellable where the child is watched; where a worker
// blocks in waitpid() the cancel reports -EALREADY and the op completes
// once the child is gone.
static void test_waitpid_cancel(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status = -1;

	kid *k = spawn_child(s, 0, 1500);
	submit_wait(s, k->pid, &status, 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	sleep_ms(50);

	ior_sqe *c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_cancel(s->ctx, c, TAG_WAIT);
	ior_sqe_set_data(s->ctx, c, TAG_CANCEL);
	assert_true(ior_submit(s->ctx) >= 0);

	// Either CQE may land first: IOCP posts the wait's abort before the
	// cancel's own result. Only a cancel that reports -EALREADY is known
	// to arrive alone.
	res[(uintptr_t) TAG_CANCEL] = INT32_MIN;
	reap_tags(s->ctx, 1, res, 3);
	if (res[(uintptr_t) TAG_CANCEL] == -EALREADY) {
		// A worker holds it in waitpid(), or the cancel caught the probe
		// (the op then ends -ECANCELED although the cancel said running).
		kill_child(k);
		reap_tags(s->ctx, 1, res, 3);
		if (res[(uintptr_t) TAG_WAIT] == -ECANCELED) {
			return;
		}
		assert_int_equal(res[(uintptr_t) TAG_WAIT], (int32_t) k->pid);
		collect(k, status, 0);
		return;
	}
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], -ECANCELED);
	assert_int_equal(status, -1);
}

// A link timeout bounds a watched wait: -ECANCELED / -ETIME at the deadline.
static void test_waitpid_link_timeout(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status = -1;

	kid *k = spawn_child(s, 0, 1500);
	submit_wait(s, k->pid, &status, 0, TAG_WAIT, IOR_SQE_IO_LINK);
	ior_sqe *lt = ior_get_sqe(s->ctx);
	assert_non_null(lt);
	ior_timespec ts = { .tv_sec = 0, .tv_nsec = 50000000 };
	ior_prep_link_timeout(s->ctx, lt, &ts, 0);
	ior_sqe_set_data(s->ctx, lt, TAG_TMO);
	assert_true(ior_submit(s->ctx) >= 0);

	uint64_t start = test_monotonic_now_ns();
	reap_tags(s->ctx, 2, res, 5);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], -ECANCELED);
	assert_int_equal(res[(uintptr_t) TAG_TMO], -ETIME);
	assert_true(test_monotonic_now_ns() - start < 1000000000ULL);
}

#ifndef _WIN32
/*
 * A wait nothing can wake (WUNTRACED is never watched) holds no worker, so
 * its link timeout cancels it at the deadline, and the child it was waiting
 * for is left to be collected.
 */
static void test_waitpid_link_timeout_unwatched(void **state)
{
	wp_state *s = (wp_state *) *state;
	int status = -1;

	kid *k = spawn_child(s, 3, 600);
	submit_wait(s, k->pid, &status, WUNTRACED, TAG_WAIT, IOR_SQE_IO_LINK);
	ior_sqe *lt = ior_get_sqe(s->ctx);
	assert_non_null(lt);
	ior_timespec ts = { .tv_sec = 0, .tv_nsec = 50000000 };
	ior_prep_link_timeout(s->ctx, lt, &ts, 0);
	ior_sqe_set_data(s->ctx, lt, TAG_TMO);
	uint64_t start = test_monotonic_now_ns();
	assert_true(ior_submit(s->ctx) >= 0);

	int32_t res[MAX_TAG];
	reap_tags(s->ctx, 2, res, 5);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], -ECANCELED);
	assert_int_equal(res[(uintptr_t) TAG_TMO], -ETIME);
	assert_true(test_monotonic_now_ns() - start < 400000000ULL);
	assert_int_equal(status, -1);

	assert_int_equal(waitpid(k->pid, &status, 0), k->pid);
	k->reaped = 1;
	assert_exited(k, status);
}
#endif

// Teardown with a wait still pending must not hang on a watched wait.
static void test_waitpid_pending_at_exit(void **state)
{
	wp_state *s = (wp_state *) *state;

	kid *k = spawn_child(s, 0, 1500);
	submit_wait(s, k->pid, NULL, 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	sleep_ms(50);

	uint64_t start = test_monotonic_now_ns();
	ior_queue_exit(s->ctx);
	s->ctx = NULL;
	assert_true(test_monotonic_now_ns() - start < 1000000000ULL);
}

// A process that is not a child of ours: -ECHILD at once, however long it
// lives.
static void test_waitpid_echild(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status = -1;

#ifdef _WIN32
	ior_pid_t pid = 0x7ffffff0; // no such process
#else
	ior_pid_t pid = 1; // init: alive, nobody's child
#endif
	submit_wait(s, pid, &status, 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], -ECHILD);
}

#ifndef _WIN32
// WNOHANG: 0 while the child runs, the pid once it has exited.
static void test_waitpid_wnohang(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status = -1;

	kid *k = spawn_child(s, 5, 300);
	submit_wait(s, k->pid, &status, WNOHANG, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], 0);

	sleep_ms(600);
	submit_wait(s, k->pid, &status, WNOHANG, TAG_WAIT2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	check_reported(s, res[(uintptr_t) TAG_WAIT2], status);
}

// A killed child reports the signal.
static void test_waitpid_signaled(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status = -1;

	kid *k = spawn_child(s, 0, 3000);
	submit_wait(s, k->pid, &status, 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	sleep_ms(50);
	kill_child(k);

	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], (int32_t) k->pid);
	assert_true(WIFSIGNALED(status));
	assert_int_equal(WTERMSIG(status), SIGKILL);
	collect(k, status, 0);
}

/*
 * -1 reports any child. Two children, two waits in turn: the first reports
 * one of them, which the test collects, and the second the other, since an
 * exited child is reported until collected.
 */
static void test_waitpid_any(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status[2] = { -1, -1 };

	spawn_child(s, 1, 0);
	spawn_child(s, 2, 100);
	submit_wait(s, -1, &status[0], 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	kid *a = check_reported(s, res[(uintptr_t) TAG_WAIT], status[0]);

	submit_wait(s, -1, &status[1], 0, TAG_WAIT2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	kid *b = check_reported(s, res[(uintptr_t) TAG_WAIT2], status[1]);
	assert_ptr_not_equal(a, b);
}

/*
 * A report is not a collection: an exited child is reported by every wait
 * that asks about it, whether at once (WNOHANG) or pending, with the same
 * status each time, until the caller collects it, after which a wait for
 * it is -ECHILD.
 */
static void test_waitpid_reported_again(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status[3] = { -1, -1, -1 };

	kid *k = spawn_child(s, 9, 0);
	sleep_ms(100);
	submit_wait(s, k->pid, &status[0], WNOHANG, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], (int32_t) k->pid);
	assert_exited(k, status[0]);

	submit_wait(s, k->pid, &status[1], 0, TAG_WAIT2, 0);
	submit_wait(s, -1, &status[2], 0, TAG_TMO, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT2], (int32_t) k->pid);
	assert_int_equal(res[(uintptr_t) TAG_TMO], (int32_t) k->pid);
	assert_int_equal(status[1], status[0]);
	assert_int_equal(status[2], status[0]);
	collect(k, status[0], 0);

	submit_wait(s, k->pid, &status[1], 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], -ECHILD);
}

/*
 * Two waits pending on the same live child both complete when it exits,
 * whether the child is watched (one pid, no options) or probed, and the
 * child is still there to collect once.
 */
static void test_waitpid_two_waiters(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status[2] = { -1, -1 };

	kid *k = spawn_child(s, 4, 200);
	assert_return_code(setpgid(k->pid, k->pid), 0);
	submit_wait(s, k->pid, &status[0], 0, TAG_WAIT, 0);
	submit_wait(s, -k->pid, &status[1], 0, TAG_WAIT2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], (int32_t) k->pid);
	assert_int_equal(res[(uintptr_t) TAG_WAIT2], (int32_t) k->pid);
	assert_exited(k, status[0]);
	assert_int_equal(status[1], status[0]);
	collect(k, status[0], 0);
}

/*
 * A cancelled wait for any child takes nothing: it holds no worker, so the
 * cancel claims it (0), and the child it was waiting for is left to be
 * collected. Where the wait is probed from a timer, a cancel that lands
 * while a probe runs finds it running (-EALREADY), as on io_uring; the
 * probe then sees the cancel and ends it the same way.
 */
static void test_waitpid_any_cancel(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status = -1;

	kid *k = spawn_child(s, 4, 300);
	submit_wait(s, -1, &status, 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	sleep_ms(50);

	ior_sqe *c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_cancel(s->ctx, c, TAG_WAIT);
	ior_sqe_set_data(s->ctx, c, TAG_CANCEL);
	assert_true(ior_submit(s->ctx) >= 0);

	reap_tags(s->ctx, 2, res, 3);
	int32_t cancel_res = res[(uintptr_t) TAG_CANCEL];
	assert_true(cancel_res == 0 || cancel_res == -EALREADY);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], -ECANCELED);
	assert_int_equal(status, -1);

	assert_int_equal(waitpid(k->pid, &status, 0), k->pid);
	k->reaped = 1;
	assert_exited(k, status);
}

/*
 * A wait for stop and continue reports sees the child stop, then resume.
 * Each report stays pending until a wait with that option collects it: the
 * stop is reported again by a second wait before the test collects it.
 */
static void test_waitpid_stopped_continued(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status = -1;

	kid *k = spawn_child(s, 0, 5000);
	submit_wait(s, -1, &status, WUNTRACED, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	sleep_ms(50);
	kill(k->pid, SIGSTOP);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], (int32_t) k->pid);
	assert_true(WIFSTOPPED(status));
	assert_int_equal(WSTOPSIG(status), SIGSTOP);

	int again = -1;
	submit_wait(s, k->pid, &again, WUNTRACED, TAG_WAIT2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT2], (int32_t) k->pid);
	assert_int_equal(again, status);
	collect(k, status, WUNTRACED);

	status = -1;
	submit_wait(s, k->pid, &status, WCONTINUED, TAG_WAIT2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	sleep_ms(50);
	kill(k->pid, SIGCONT);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT2], (int32_t) k->pid);
	assert_true(WIFCONTINUED(status));
	collect(k, status, WCONTINUED);
}

/*
 * A stopped child is nothing to a wait that did not ask (no WUNTRACED): a
 * wait for it stays pending, and a wait for any child or for its group
 * reports an exited sibling instead, even the older one behind it on the
 * kernel's list. macOS's waitid reports a stopped child whatever is asked,
 * and would hand it out in place of the sibling; ior must not.
 */
static void test_waitpid_any_stopped(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status[3] = { -1, -1, -1 };
	memset(res, 0, sizeof(res));

	kid *e = spawn_child(s, 6, 300);
	kid *k = spawn_child(s, 0, 5000); // newer: ahead of e on the kernel's list
	assert_return_code(setpgid(k->pid, k->pid), 0);
	assert_return_code(setpgid(e->pid, k->pid), 0);
	kill(k->pid, SIGSTOP);
	sleep_ms(400); // e has exited, k is stopped with its stop uncollected

	submit_wait(s, k->pid, &status[0], 0, TAG_WAIT, 0);
	submit_wait(s, -1, &status[1], 0, TAG_WAIT2, 0);
	submit_wait(s, -k->pid, &status[2], 0, TAG_TMO, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 2, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT2], (int32_t) e->pid);
	assert_int_equal(res[(uintptr_t) TAG_TMO], (int32_t) e->pid);
	assert_int_equal(status[2], status[1]);
	check_reported(s, res[(uintptr_t) TAG_WAIT2], status[1]);

	// The wait on the stopped child itself has nothing to report (its
	// status is not read here: the op is in flight and may write it)...
	ior_cqe *cqe = NULL;
	ior_timespec to = { .tv_sec = 0, .tv_nsec = 200 * 1000000L };
	assert_int_equal(ior_wait_cqe_timeout(s->ctx, &cqe, &to), -ETIME);

	// ...until the child is gone.
	kill(k->pid, SIGKILL);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_WAIT], (int32_t) k->pid);
	assert_true(WIFSIGNALED(status[0]));
	assert_int_equal(WTERMSIG(status[0]), SIGKILL);
	collect(k, status[0], 0);
}

/* A wait for a process group reports the child that leads it. */
static void test_waitpid_group(void **state)
{
	wp_state *s = (wp_state *) *state;
	int32_t res[MAX_TAG];
	int status = -1;

	kid *k = spawn_child(s, 6, 100);
	assert_return_code(setpgid(k->pid, k->pid), 0);
	submit_wait(s, -k->pid, &status, 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	check_reported(s, res[(uintptr_t) TAG_WAIT], status);
}

/*
 * Teardown with a wait for any child pending does not wait for the child,
 * and does not collect it.
 */
static void test_waitpid_any_at_exit(void **state)
{
	wp_state *s = (wp_state *) *state;

	kid *k = spawn_child(s, 2, 1500);
	submit_wait(s, -1, NULL, 0, TAG_WAIT, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	sleep_ms(50);

	uint64_t start = test_monotonic_now_ns();
	ior_queue_exit(s->ctx);
	s->ctx = NULL;
	assert_true(test_monotonic_now_ns() - start < 500000000ULL);

	int status;
	assert_int_equal(waitpid(k->pid, &status, 0), k->pid);
	k->reaped = 1;
	assert_exited(k, status);
}
#endif

int main(int argc, char **argv)
{
#ifdef _WIN32
	// The child side of spawn_child: sleep, then exit with the code.
	if (argc == 4 && strcmp(argv[1], "--child") == 0) {
		Sleep((DWORD) atoi(argv[3]));
		return atoi(argv[2]);
	}
#else
	(void) argc;
	(void) argv;
#endif
	const struct CMUnitTest tests[] = {
		cmocka_unit_test_setup_teardown(test_waitpid_exited, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_pending, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_null_status, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_many, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_cancel, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_link_timeout, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_pending_at_exit, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_echild, setup_wp, teardown_wp),
#ifndef _WIN32
		cmocka_unit_test_setup_teardown(test_waitpid_wnohang, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_signaled, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_any, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_reported_again, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_two_waiters, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_any_cancel, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_link_timeout_unwatched, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_stopped_continued, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_any_stopped, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_group, setup_wp, teardown_wp),
		cmocka_unit_test_setup_teardown(test_waitpid_any_at_exit, setup_wp, teardown_wp),
#endif
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
