/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * test_signals.c - signals never reach ior's threads.
 *
 * Every thread ior creates blocks all signals, so a work callback runs with a
 * full mask and a process-directed signal, sent while the pool is busy, is
 * handled on the caller's thread rather than on a worker.
 */
#include "test_utils.h"
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <unistd.h>

#define NWORK 4

typedef struct sig_state {
	ior_ctx *ctx;
} sig_state;

static int setup_sig(void **state)
{
	sig_state *s = calloc(1, sizeof(*s));
	assert_non_null(s);
	assert_return_code(ior_queue_init(32, &s->ctx), 0);
	*state = s;
	return 0;
}

static int teardown_sig(void **state)
{
	sig_state *s = (sig_state *) *state;
	if (s) {
		ior_queue_exit(s->ctx);
		free(s);
	}
	return 0;
}

static void msleep(unsigned ms)
{
	struct timespec ts = { .tv_sec = ms / 1000, .tv_nsec = (long) (ms % 1000) * 1000000L };
	nanosleep(&ts, NULL);
}

// Reports whether the calling (worker) thread has these signals blocked.
static int32_t mask_probe(ior_work_token *token, void *arg)
{
	(void) token;
	(void) arg;
	sigset_t set;
	if (pthread_sigmask(SIG_BLOCK, NULL, &set) != 0) {
		return -1;
	}
	int ok = sigismember(&set, SIGUSR1) && sigismember(&set, SIGUSR2) && sigismember(&set, SIGTERM)
			&& sigismember(&set, SIGINT) && sigismember(&set, SIGALRM) && sigismember(&set, SIGHUP);
	return ok ? 1 : 0;
}

// A work callback starts with every signal blocked.
static void test_worker_mask(void **state)
{
	sig_state *s = (sig_state *) *state;

	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	assert_return_code(ior_prep_work(s->ctx, sqe, mask_probe, NULL), 0);
	ior_sqe_set_data(s->ctx, sqe, (void *) 0x1);
	assert_true(ior_submit_and_wait(s->ctx, 1) >= 0);

	ior_cqe *cqe = NULL;
	assert_return_code(ior_wait_cqe(s->ctx, &cqe), 0);
	assert_int_equal(ior_cqe_get_res(s->ctx, cqe), 1);
	ior_cqe_seen(s->ctx, cqe);
}

/* Handler bookkeeping: async-signal-safe stores only. */
static volatile sig_atomic_t g_handled;
static pthread_t g_handler_thread;

static void on_usr1(int signo)
{
	(void) signo;
	g_handler_thread = pthread_self();
	g_handled = 1;
}

typedef struct busy_gate {
	_Atomic int started;
	_Atomic int release;
} busy_gate;

static int32_t busy_work(ior_work_token *token, void *arg)
{
	(void) token;
	busy_gate *g = arg;
	atomic_fetch_add(&g->started, 1);
	while (!atomic_load(&g->release)) {
		msleep(1);
	}
	return 0;
}

/*
 * A process-directed signal sent while several workers are busy is handled
 * on this thread: the workers are not candidates for delivery.
 */
static void test_signal_lands_on_caller(void **state)
{
	sig_state *s = (sig_state *) *state;
	busy_gate gate;
	atomic_init(&gate.started, 0);
	atomic_init(&gate.release, 0);

	struct sigaction sa, old;
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_usr1;
	sigemptyset(&sa.sa_mask);
	assert_return_code(sigaction(SIGUSR1, &sa, &old), 0);

	for (int i = 0; i < NWORK; i++) {
		ior_sqe *sqe = ior_get_sqe(s->ctx);
		assert_non_null(sqe);
		assert_return_code(ior_prep_work(s->ctx, sqe, busy_work, &gate), 0);
		ior_sqe_set_data(s->ctx, sqe, (void *) (uintptr_t) (0x10 + i));
	}
	assert_true(ior_submit(s->ctx) >= 0);
	while (atomic_load(&gate.started) < NWORK) {
		msleep(1);
	}

	g_handled = 0;
	assert_return_code(kill(getpid(), SIGUSR1), 0);
	for (int spin = 0; spin < 2000 && !g_handled; spin++) {
		msleep(1);
	}
	assert_int_equal(g_handled, 1);
	assert_true(pthread_equal(g_handler_thread, pthread_self()));

	atomic_store(&gate.release, 1);
	for (int i = 0; i < NWORK; i++) {
		ior_cqe *cqe = NULL;
		int ret;
		while ((ret = ior_wait_cqe(s->ctx, &cqe)) == -EINTR) { }
		assert_return_code(ret, 0);
		ior_cqe_seen(s->ctx, cqe);
	}
	sigaction(SIGUSR1, &old, NULL);
}

/* A callback that sleeps and reports whether the sleep ran to completion. */
typedef struct sleep_probe {
	_Atomic int started;
	pthread_t tid;
} sleep_probe;

static int32_t sleeping_work(ior_work_token *token, void *arg)
{
	(void) token;
	sleep_probe *p = arg;
	p->tid = pthread_self();
	atomic_store_explicit(&p->started, 1, memory_order_release);
	struct timespec ts = { .tv_sec = 0, .tv_nsec = 100000000L };
	int ret = nanosleep(&ts, NULL);
	return ret == 0 ? 1 : -errno; // -EINTR if the signal got through
}

/*
 * A signal directed at a worker thread while its callback is in a syscall:
 * with the mask it stays pending and the syscall completes untouched; no
 * handler runs on the worker, and EINTR is never seen there.
 */
static void test_worker_syscall_not_interrupted(void **state)
{
	sig_state *s = (sig_state *) *state;
	sleep_probe probe;
	atomic_init(&probe.started, 0);

	struct sigaction sa, old;
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_usr1;
	sigemptyset(&sa.sa_mask);
	assert_return_code(sigaction(SIGUSR1, &sa, &old), 0);
	g_handled = 0;

	ior_sqe *sqe = ior_get_sqe(s->ctx);
	assert_non_null(sqe);
	assert_return_code(ior_prep_work(s->ctx, sqe, sleeping_work, &probe), 0);
	ior_sqe_set_data(s->ctx, sqe, (void *) 0x2);
	assert_true(ior_submit(s->ctx) >= 0);
	while (!atomic_load_explicit(&probe.started, memory_order_acquire)) {
		msleep(1);
	}
	msleep(10); // well inside the sleep
	assert_return_code(pthread_kill(probe.tid, SIGUSR1), 0);

	ior_cqe *cqe = NULL;
	int ret;
	while ((ret = ior_wait_cqe(s->ctx, &cqe)) == -EINTR) { }
	assert_return_code(ret, 0);
	assert_int_equal(ior_cqe_get_res(s->ctx, cqe), 1);
	ior_cqe_seen(s->ctx, cqe);
	assert_int_equal(g_handled, 0);

	sigaction(SIGUSR1, &old, NULL);
}

typedef struct kicker {
	pthread_t target;
} kicker;

static void *kick_main(void *arg)
{
	kicker *k = arg;
	msleep(30);
	pthread_kill(k->target, SIGUSR1);
	return NULL;
}

/*
 * The other side of the contract: the caller's blocking wait is what a
 * signal interrupts. With nothing in flight, ior_wait_cqe() returns -EINTR
 * once the signal lands, and the handler ran on this thread.
 */
static void test_caller_wait_interrupted(void **state)
{
	sig_state *s = (sig_state *) *state;

	struct sigaction sa, old;
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_usr1;
	sigemptyset(&sa.sa_mask); // no SA_RESTART: the wait must report it
	assert_return_code(sigaction(SIGUSR1, &sa, &old), 0);
	g_handled = 0;

	kicker k = { .target = pthread_self() };
	pthread_t t;
	assert_return_code(pthread_create(&t, NULL, kick_main, &k), 0);

	ior_cqe *cqe = NULL;
	int ret = ior_wait_cqe(s->ctx, &cqe);
	assert_int_equal(ret, -EINTR);
	// ThreadSanitizer defers handlers to its next safe point, so the wait can
	// report -EINTR a moment before the handler has run.
	for (int spin = 0; spin < 2000 && !g_handled; spin++) {
		msleep(1);
	}
	assert_int_equal(g_handled, 1);
	assert_true(pthread_equal(g_handler_thread, pthread_self()));

	pthread_join(t, NULL);
	sigaction(SIGUSR1, &old, NULL);
}

// The caller's own mask is left alone by context creation and teardown.
static void test_caller_mask_untouched(void **state)
{
	(void) state;
	sigset_t before, after;
	assert_return_code(pthread_sigmask(SIG_BLOCK, NULL, &before), 0);

	ior_ctx *ctx;
	assert_return_code(ior_queue_init(32, &ctx), 0);
	ior_sqe *sqe = ior_get_sqe(ctx);
	assert_non_null(sqe);
	assert_return_code(ior_prep_work(ctx, sqe, mask_probe, NULL), 0);
	assert_true(ior_submit_and_wait(ctx, 1) >= 0);
	ior_cqe *cqe = NULL;
	assert_return_code(ior_wait_cqe(ctx, &cqe), 0);
	ior_cqe_seen(ctx, cqe);
	ior_queue_exit(ctx);

	assert_return_code(pthread_sigmask(SIG_BLOCK, NULL, &after), 0);
	// Compare signal by signal: sigset_t carries unused bytes.
	for (int signo = 1; signo < NSIG; signo++) {
		assert_int_equal(sigismember(&before, signo), sigismember(&after, signo));
	}
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test_setup_teardown(test_worker_mask, setup_sig, teardown_sig),
		cmocka_unit_test_setup_teardown(test_signal_lands_on_caller, setup_sig, teardown_sig),
		cmocka_unit_test_setup_teardown(
				test_worker_syscall_not_interrupted, setup_sig, teardown_sig),
		cmocka_unit_test_setup_teardown(test_caller_wait_interrupted, setup_sig, teardown_sig),
		cmocka_unit_test(test_caller_mask_untouched),
	};
	return cmocka_run_group_tests(tests, NULL, NULL);
}
