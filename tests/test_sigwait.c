/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_sigwait.c - IOR_OP_SIGWAIT on every backend.
 *
 * Signals are sent to the test's own process, with the set blocked on the
 * caller's thread as sigwaitinfo() needs: one arriving while the op is
 * pending, one pending before it is submitted, a queued value, a set of
 * several signals, a signal outside the set left untouched, several ops on
 * a queued signal, two ops on one signal, cancellation, a link timeout,
 * teardown with a wait still pending, and bad arguments. On Windows the
 * signals are the console control events, raised on a console of the
 * test's own so nothing else on the machine sees them.
 */
#include "test_utils.h"
#ifdef _WIN32
#include <windows.h>
#include <signal.h>
#else
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#endif

#define TAG_SIG ((void *) 0x1)
#define TAG_SIG2 ((void *) 0x2)
#define TAG_TMO ((void *) 0x3)
#define TAG_CANCEL ((void *) 0x4)
#define TAG_BATCH ((void *) 0x8) /* + index */
#define MAX_BATCH 4
#define MAX_TAG (0x8 + MAX_BATCH)

/*
 * Whether a pending wait can be taken back: an io_uring poll cancels
 * natively, a thread backend worker looks at its token between sigtimedwait
 * slices, IOCP unlists the op. A worker blocked in sigwait() (no
 * sigtimedwait) ends only when a signal of the set arrives.
 */
#if defined(_WIN32) || defined(IOR_HAVE_URING) || defined(IOR_HAVE_SIGTIMEDWAIT)
#define SIGWAIT_INTERRUPTIBLE 1
#else
#define SIGWAIT_INTERRUPTIBLE 0
#endif

/* Whether more than the signal number is reported (sigwait() knows no more). */
#if defined(IOR_HAVE_URING) || defined(IOR_HAVE_SIGTIMEDWAIT)
#define SIGWAIT_HAS_INFO 1
#else
#define SIGWAIT_HAS_INFO 0
#endif

#ifdef _WIN32
#define SIG_A SIGINT
#define SIG_B SIGBREAK
#else
#define SIG_A SIGUSR1
#define SIG_B SIGUSR2
#ifdef SIGRTMIN
#define SIG_RT (SIGRTMIN + 3)
#endif
#endif

typedef struct sw_state {
	ior_ctx *ctx;
#ifndef _WIN32
	sigset_t saved_mask;
#endif
} sw_state;

/* Every signal the tests send. */
static void test_signals(ior_sigset_t *set)
{
	assert_return_code(ior_sigemptyset(set), 0);
	assert_return_code(ior_sigaddset(set, SIG_A), 0);
	assert_return_code(ior_sigaddset(set, SIG_B), 0);
#ifdef SIG_RT
	assert_return_code(ior_sigaddset(set, SIG_RT), 0);
#endif
}

static void sleep_ms(int ms)
{
#ifdef _WIN32
	Sleep((DWORD) ms);
#else
	usleep((useconds_t) ms * 1000);
#endif
}

/* Raise sig for this process (a console control event on Windows). */
static void send_signal(int sig)
{
#ifdef _WIN32
	DWORD type = sig == SIGINT ? CTRL_C_EVENT : CTRL_BREAK_EVENT;
	assert_true(GenerateConsoleCtrlEvent(type, 0));
#else
	assert_return_code(kill(getpid(), sig), 0);
#endif
}

#ifndef _WIN32
/* Consume whatever test signal is still pending, so no test inherits one. */
static void drain_pending(void)
{
	sigset_t set;
	test_signals(&set);
#ifdef IOR_HAVE_SIGTIMEDWAIT
	struct timespec zero = { 0, 0 };
	siginfo_t info;
	while (sigtimedwait(&set, &info, &zero) > 0) { }
#else
	for (;;) {
		sigset_t pending;
		assert_return_code(sigpending(&pending), 0);
		int any = 0;
		for (int signo = 1; signo < NSIG; signo++) {
			if (sigismember(&set, signo) == 1 && sigismember(&pending, signo) == 1) {
				any = 1;
			}
		}
		if (!any) {
			break;
		}
		int sig;
		assert_return_code(sigwait(&set, &sig), 0);
	}
#endif
}
#endif

static int setup_sw(void **state)
{
	sw_state *s = calloc(1, sizeof(*s));
	assert_non_null(s);
#ifndef _WIN32
	// Blocked here, and in every thread ior creates, the signals stay
	// pending for the ops instead of running their default action.
	sigset_t set;
	test_signals(&set);
	assert_return_code(pthread_sigmask(SIG_BLOCK, &set, &s->saved_mask), 0);
#endif
	assert_return_code(ior_queue_init(32, &s->ctx), 0);
	*state = s;
	return 0;
}

static int teardown_sw(void **state)
{
	sw_state *s = (sw_state *) *state;
	if (!s) {
		return 0;
	}
	if (s->ctx) {
		ior_queue_exit(s->ctx);
	}
#ifndef _WIN32
	drain_pending();
	pthread_sigmask(SIG_SETMASK, &s->saved_mask, NULL);
#endif
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

// Nothing completes within ms.
static void assert_quiet(ior_ctx *ctx, int ms)
{
	ior_cqe *cqe = NULL;
	ior_timespec to = { .tv_sec = 0, .tv_nsec = (long long) ms * 1000000 };
	int ret;
	while ((ret = ior_wait_cqe_timeout(ctx, &cqe, &to)) == -EINTR) { }
	assert_int_equal(ret, -ETIME);
}

static void submit_sigwait(
		sw_state *s, const ior_sigset_t *set, ior_siginfo_t *info, void *tag, uint8_t flags)
{
	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	assert_return_code(ior_prep_sigwait(s->ctx, sqe, set, info), 0);
	ior_sqe_set_data(s->ctx, sqe, tag);
	if (flags) {
		ior_sqe_set_flags(s->ctx, sqe, flags);
	}
}

static void one_signal_set(ior_sigset_t *set, int sig)
{
	assert_return_code(ior_sigemptyset(set), 0);
	assert_return_code(ior_sigaddset(set, sig), 0);
}

/* The op took sig, and says so in info. */
static void assert_took(int32_t res, const ior_siginfo_t *info, int sig)
{
	assert_int_equal(res, sig);
	assert_int_equal(info->si_signo, sig);
#ifdef _WIN32
	assert_int_equal(info->si_code, sig == SIGINT ? (int) CTRL_C_EVENT : (int) CTRL_BREAK_EVENT);
#elif SIGWAIT_HAS_INFO
	assert_int_equal(info->si_code, SI_USER);
	assert_int_equal(info->si_pid, getpid());
#endif
}

// Arrives while the op is pending: nothing completes before, the signal after.
static void test_sigwait_pending(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info;

	one_signal_set(&set, SIG_A);
	memset(&info, 0, sizeof(info));
	submit_sigwait(s, &set, &info, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	assert_quiet(s->ctx, 50);

	send_signal(SIG_A);
	reap_tags(s->ctx, 1, res, 3);
	assert_took(res[(uintptr_t) TAG_SIG], &info, SIG_A);
}

#ifndef _WIN32
// Pending before the op is submitted: taken at once.
static void test_sigwait_pending_at_submit(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info;

	send_signal(SIG_A);
	one_signal_set(&set, SIG_A);
	submit_sigwait(s, &set, &info, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_took(res[(uintptr_t) TAG_SIG], &info, SIG_A);
}
#endif

// A NULL info is allowed.
static void test_sigwait_null_info(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;

	one_signal_set(&set, SIG_B);
	submit_sigwait(s, &set, NULL, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	send_signal(SIG_B);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_SIG], SIG_B);
}

// A set of two: whichever arrives is reported.
static void test_sigwait_set_of_two(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info;

	one_signal_set(&set, SIG_A);
	assert_return_code(ior_sigaddset(&set, SIG_B), 0);
	submit_sigwait(s, &set, &info, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	send_signal(SIG_B);
	reap_tags(s->ctx, 1, res, 3);
	assert_took(res[(uintptr_t) TAG_SIG], &info, SIG_B);
}

#ifndef _WIN32
// A signal outside the set is left where it is: pending, for someone else.
static void test_sigwait_other_signal_untouched(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info;

	one_signal_set(&set, SIG_A);
	submit_sigwait(s, &set, &info, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);

	send_signal(SIG_B);
	assert_quiet(s->ctx, 100);
	sigset_t pending;
	assert_return_code(sigpending(&pending), 0);
	assert_int_equal(sigismember(&pending, SIG_B), 1);

	send_signal(SIG_A);
	reap_tags(s->ctx, 1, res, 3);
	assert_took(res[(uintptr_t) TAG_SIG], &info, SIG_A);
	assert_return_code(sigpending(&pending), 0);
	assert_int_equal(sigismember(&pending, SIG_B), 1);
}
#endif

#ifdef SIG_RT
// sigqueue(): the value and the sender come with the signal.
static void test_sigwait_queued_value(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info;

	one_signal_set(&set, SIG_RT);
	submit_sigwait(s, &set, &info, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);

	union sigval v;
	v.sival_int = 42;
	assert_return_code(sigqueue(getpid(), SIG_RT, v), 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_SIG], SIG_RT);
	assert_int_equal(info.si_signo, SIG_RT);
#if SIGWAIT_HAS_INFO
	assert_int_equal(info.si_code, SI_QUEUE);
	assert_int_equal(info.si_value.sival_int, 42);
	assert_int_equal(info.si_pid, getpid());
#endif
}

// Several ops on a queued signal, several instances of it: each op takes
// one, and every value sent is reported once.
static void test_sigwait_many(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info[MAX_BATCH];
	const int n = 3;

	one_signal_set(&set, SIG_RT);
	for (int i = 0; i < n; i++) {
		submit_sigwait(s, &set, &info[i], (char *) TAG_BATCH + i, 0);
	}
	assert_true(ior_submit(s->ctx) >= 0);
	for (int i = 0; i < n; i++) {
		union sigval v;
		v.sival_int = i + 1;
		assert_return_code(sigqueue(getpid(), SIG_RT, v), 0);
	}

	reap_tags(s->ctx, n, res, 5);
	int seen = 0;
	for (int i = 0; i < n; i++) {
		assert_int_equal(res[(uintptr_t) TAG_BATCH + i], SIG_RT);
		assert_int_equal(info[i].si_signo, SIG_RT);
#if SIGWAIT_HAS_INFO
		int value = info[i].si_value.sival_int;
		assert_true(value >= 1 && value <= n);
		assert_int_equal(seen & (1 << value), 0);
		seen |= 1 << value;
#endif
	}
	(void) seen;
}
#endif

/*
 * Bring a still-pending op with tag to an end: cancel it, and where a
 * worker blocks in sigwait() release it with sig. Reaps the cancel and the
 * op, whose result is returned.
 */
static int32_t end_pending(sw_state *s, void *tag, int sig)
{
	int32_t res[MAX_TAG];
	ior_sqe *c = ior_get_sqe(s->ctx);
	assert_non_null(c);
	ior_prep_cancel(s->ctx, c, tag);
	ior_sqe_set_data(s->ctx, c, TAG_CANCEL);
	assert_true(ior_submit(s->ctx) >= 0);

	// Either CQE may land first: only a cancel that reports -EALREADY is
	// known to arrive alone (a thread backend worker holds the op).
	res[(uintptr_t) TAG_CANCEL] = INT32_MIN;
	reap_tags(s->ctx, 1, res, 3);
	if (res[(uintptr_t) TAG_CANCEL] == -EALREADY) {
		if (!SIGWAIT_INTERRUPTIBLE) {
			send_signal(sig);
		}
		reap_tags(s->ctx, 1, res, 3);
		return res[(uintptr_t) tag];
	}
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_CANCEL], 0);
	return res[(uintptr_t) tag];
}

// Two ops on one signal, one arrival: one op takes it. The other keeps
// waiting, except on io_uring, where the arrival woke both polls and the
// second finds the signal gone: -EAGAIN, to be submitted again.
static void test_sigwait_two_on_one(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info[2];

	one_signal_set(&set, SIG_A);
	submit_sigwait(s, &set, &info[0], TAG_SIG, 0);
	submit_sigwait(s, &set, &info[1], TAG_SIG2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	send_signal(SIG_A);

	reap_tags(s->ctx, 1, res, 3);
	void *taker = res[(uintptr_t) TAG_SIG] == SIG_A ? TAG_SIG : TAG_SIG2;
	void *other = taker == TAG_SIG ? TAG_SIG2 : TAG_SIG;
	assert_int_equal(res[(uintptr_t) taker], SIG_A);

	ior_cqe *cqe = NULL;
	ior_timespec to = { .tv_sec = 0, .tv_nsec = 200000000 };
	int ret;
	while ((ret = ior_wait_cqe_timeout(s->ctx, &cqe, &to)) == -EINTR) { }
	if (ret == 0) {
		assert_ptr_equal(ior_cqe_get_data(s->ctx, cqe), other);
		assert_int_equal(ior_cqe_get_res(s->ctx, cqe), -EAGAIN);
		ior_cqe_seen(s->ctx, cqe);
		return;
	}
	if (ior_get_backend_type(s->ctx) == IOR_BACKEND_IOURING) {
		fail_msg("the second poll did not report the taken signal");
	}
	assert_int_equal(ret, -ETIME);
	int32_t r = end_pending(s, other, SIG_A);
	assert_true(r == -ECANCELED || r == SIG_A);
}

// A pending wait is cancellable where the worker can be reached; elsewhere
// the cancel reports -EALREADY and the op completes with its signal.
static void test_sigwait_cancel(void **state)
{
	sw_state *s = (sw_state *) *state;
	ior_sigset_t set;
	ior_siginfo_t info;

	one_signal_set(&set, SIG_A);
	submit_sigwait(s, &set, &info, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	sleep_ms(50);

	int32_t r = end_pending(s, TAG_SIG, SIG_A);
	if (SIGWAIT_INTERRUPTIBLE) {
		assert_int_equal(r, -ECANCELED);
	} else {
		assert_int_equal(r, SIG_A);
	}
}

// A link timeout bounds the wait: -ECANCELED / -ETIME at the deadline.
static void test_sigwait_link_timeout(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info;

	one_signal_set(&set, SIG_A);
	submit_sigwait(s, &set, &info, TAG_SIG, IOR_SQE_IO_LINK);
	ior_sqe *lt = ior_get_sqe(s->ctx);
	assert_non_null(lt);
	ior_timespec ts = { .tv_sec = 0, .tv_nsec = 50000000 };
	ior_prep_link_timeout(s->ctx, lt, &ts, 0);
	ior_sqe_set_data(s->ctx, lt, TAG_TMO);
	assert_true(ior_submit(s->ctx) >= 0);

	uint64_t start = test_monotonic_now_ns();
	if (SIGWAIT_INTERRUPTIBLE) {
		reap_tags(s->ctx, 2, res, 5);
		assert_int_equal(res[(uintptr_t) TAG_SIG], -ECANCELED);
		assert_int_equal(res[(uintptr_t) TAG_TMO], -ETIME);
		assert_true(test_monotonic_now_ns() - start < 1000000000ULL);
	} else {
		// The worker blocked in sigwait() cannot be stopped: the timeout
		// completes at the deadline, the wait once a signal releases it.
		reap_tags(s->ctx, 1, res, 5);
		assert_int_equal(res[(uintptr_t) TAG_TMO], -EALREADY);
		assert_true(test_monotonic_now_ns() - start < 1000000000ULL);
		send_signal(SIG_A);
		reap_tags(s->ctx, 1, res, 5);
		assert_int_equal(res[(uintptr_t) TAG_SIG], SIG_A);
	}
}

#if !SIGWAIT_INTERRUPTIBLE
static void *release_later(void *arg)
{
	(void) arg;
	sleep_ms(100);
	kill(getpid(), SIG_A);
	return NULL;
}
#endif

// Teardown with a wait still pending must not hang.
static void test_sigwait_pending_at_exit(void **state)
{
	sw_state *s = (sw_state *) *state;
	ior_sigset_t set;

	one_signal_set(&set, SIG_A);
	submit_sigwait(s, &set, NULL, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	sleep_ms(50);

#if !SIGWAIT_INTERRUPTIBLE
	// The worker only returns for a signal: send one from the side.
	pthread_t t;
	assert_return_code(pthread_create(&t, NULL, release_later, NULL), 0);
#endif
	uint64_t start = test_monotonic_now_ns();
	ior_queue_exit(s->ctx);
	s->ctx = NULL;
	assert_true(test_monotonic_now_ns() - start < 1000000000ULL);
#if !SIGWAIT_INTERRUPTIBLE
	pthread_join(t, NULL);
#endif
}

// A NULL or empty set is refused at prep.
static void test_sigwait_bad_args(void **state)
{
	sw_state *s = (sw_state *) *state;
	ior_sigset_t set;

	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	assert_int_equal(ior_prep_sigwait(s->ctx, sqe, NULL, NULL), -EINVAL);
	assert_return_code(ior_sigemptyset(&set), 0);
	assert_int_equal(ior_prep_sigwait(s->ctx, sqe, &set, NULL), -EINVAL);
	assert_int_equal(ior_sigaddset(&set, 0), -EINVAL);
	assert_int_equal(ior_sigismember(&set, SIG_A), 0);
	assert_return_code(ior_sigaddset(&set, SIG_A), 0);
	assert_int_equal(ior_sigismember(&set, SIG_A), 1);
	ior_prep_nop(s->ctx, sqe);
}

/*
 * A signal an op took and the caller put back is taken by the next wait,
 * with the same details: the second op is pending when it goes back, which
 * on Windows is what receives it (with no taker it would be raised).
 */
static void test_sigrequeue(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info;
	ior_siginfo_t info2;

	one_signal_set(&set, SIG_A);
	submit_sigwait(s, &set, &info, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	send_signal(SIG_A);
	reap_tags(s->ctx, 1, res, 3);
	assert_took(res[(uintptr_t) TAG_SIG], &info, SIG_A);

	submit_sigwait(s, &set, &info2, TAG_SIG2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	sleep_ms(50);
	assert_return_code(ior_sigrequeue(&info), 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_took(res[(uintptr_t) TAG_SIG2], &info2, SIG_A);
}

#ifdef SIG_RT
/* A queued signal goes back with its value. */
static void test_sigrequeue_value(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info;
	ior_siginfo_t info2;

	one_signal_set(&set, SIG_RT);
	submit_sigwait(s, &set, &info, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	union sigval v;
	v.sival_int = 42;
	assert_return_code(sigqueue(getpid(), SIG_RT, v), 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_SIG], SIG_RT);

	assert_return_code(ior_sigrequeue(&info), 0);
	submit_sigwait(s, &set, &info2, TAG_SIG2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_SIG2], SIG_RT);
	assert_int_equal(info2.si_signo, SIG_RT);
#if SIGWAIT_HAS_INFO
	assert_int_equal(info2.si_code, SI_QUEUE);
	assert_int_equal(info2.si_value.sival_int, 42);
	assert_int_equal(info2.si_pid, getpid());
#endif
}
#endif

#ifdef __linux__
static void *requeue_thread(void *arg)
{
	return (void *) (intptr_t) ior_sigrequeue((const ior_siginfo_t *) arg);
}

/*
 * Put back from a thread other than the main one, kill()'s si_code goes back
 * as SI_QUEUE (the kernel's rule), keeping who sent it.
 */
static void test_sigrequeue_other_thread(void **state)
{
	sw_state *s = (sw_state *) *state;
	int32_t res[MAX_TAG];
	ior_sigset_t set;
	ior_siginfo_t info;
	ior_siginfo_t info2;

	one_signal_set(&set, SIG_A);
	submit_sigwait(s, &set, &info, TAG_SIG, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	send_signal(SIG_A);
	reap_tags(s->ctx, 1, res, 3);
	assert_took(res[(uintptr_t) TAG_SIG], &info, SIG_A);

	pthread_t t;
	void *ret;
	assert_int_equal(pthread_create(&t, NULL, requeue_thread, &info), 0);
	assert_int_equal(pthread_join(t, &ret), 0);
	assert_int_equal((int) (intptr_t) ret, 0);

	submit_sigwait(s, &set, &info2, TAG_SIG2, 0);
	assert_true(ior_submit(s->ctx) >= 0);
	reap_tags(s->ctx, 1, res, 3);
	assert_int_equal(res[(uintptr_t) TAG_SIG2], SIG_A);
	assert_int_equal(info2.si_code, SI_QUEUE);
	assert_int_equal(info2.si_pid, getpid());
}
#endif

static void test_sigrequeue_bad_args(void **state)
{
	(void) state;
	ior_siginfo_t info;
	memset(&info, 0, sizeof(info));
	assert_int_equal(ior_sigrequeue(NULL), -EINVAL);
	assert_int_equal(ior_sigrequeue(&info), -EINVAL);
#ifdef _WIN32
	info.si_signo = SIGTERM;
	assert_int_equal(ior_sigrequeue(&info), -EINVAL);
#endif
}

#ifdef _WIN32
// Only the console control signals can be waited for.
static void test_sigwait_unsupported(void **state)
{
	sw_state *s = (sw_state *) *state;
	ior_sigset_t set;

	one_signal_set(&set, SIGTERM);
	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	assert_int_equal(ior_prep_sigwait(s->ctx, sqe, &set, NULL), -ENOTSUP);
	ior_prep_nop(s->ctx, sqe);
}

/*
 * A console of the test's own, so the control events it raises reach no
 * other process, with the standard handles (ctest's pipes) kept, and Ctrl+C
 * enabled whatever the test was started with.
 */
static int private_console(void)
{
	HANDLE in = GetStdHandle(STD_INPUT_HANDLE);
	HANDLE out = GetStdHandle(STD_OUTPUT_HANDLE);
	HANDLE err = GetStdHandle(STD_ERROR_HANDLE);
	FreeConsole();
	if (!AllocConsole()) {
		return -1;
	}
	SetStdHandle(STD_INPUT_HANDLE, in);
	SetStdHandle(STD_OUTPUT_HANDLE, out);
	SetStdHandle(STD_ERROR_HANDLE, err);
	SetConsoleCtrlHandler(NULL, FALSE);
	return 0;
}
#endif

int main(void)
{
#ifdef _WIN32
	if (private_console() < 0) {
		fprintf(stderr, "cannot allocate a console\n");
		return 1;
	}
#endif
	const struct CMUnitTest tests[] = {
		cmocka_unit_test_setup_teardown(test_sigwait_pending, setup_sw, teardown_sw),
#ifndef _WIN32
		cmocka_unit_test_setup_teardown(test_sigwait_pending_at_submit, setup_sw, teardown_sw),
#endif
		cmocka_unit_test_setup_teardown(test_sigwait_null_info, setup_sw, teardown_sw),
		cmocka_unit_test_setup_teardown(test_sigwait_set_of_two, setup_sw, teardown_sw),
#ifndef _WIN32
		cmocka_unit_test_setup_teardown(test_sigwait_other_signal_untouched, setup_sw, teardown_sw),
#endif
#ifdef SIG_RT
		cmocka_unit_test_setup_teardown(test_sigwait_queued_value, setup_sw, teardown_sw),
		cmocka_unit_test_setup_teardown(test_sigwait_many, setup_sw, teardown_sw),
#endif
		cmocka_unit_test_setup_teardown(test_sigwait_two_on_one, setup_sw, teardown_sw),
		cmocka_unit_test_setup_teardown(test_sigwait_cancel, setup_sw, teardown_sw),
		cmocka_unit_test_setup_teardown(test_sigwait_link_timeout, setup_sw, teardown_sw),
		cmocka_unit_test_setup_teardown(test_sigwait_pending_at_exit, setup_sw, teardown_sw),
		cmocka_unit_test_setup_teardown(test_sigwait_bad_args, setup_sw, teardown_sw),
		cmocka_unit_test_setup_teardown(test_sigrequeue, setup_sw, teardown_sw),
#ifdef SIG_RT
		cmocka_unit_test_setup_teardown(test_sigrequeue_value, setup_sw, teardown_sw),
#endif
#ifdef __linux__
		cmocka_unit_test_setup_teardown(test_sigrequeue_other_thread, setup_sw, teardown_sw),
#endif
		cmocka_unit_test_setup_teardown(test_sigrequeue_bad_args, setup_sw, teardown_sw),
#ifdef _WIN32
		cmocka_unit_test_setup_teardown(test_sigwait_unsupported, setup_sw, teardown_sw),
#endif
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
