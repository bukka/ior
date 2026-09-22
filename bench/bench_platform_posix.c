/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * bench_platform_posix.c - POSIX implementation of the benchmark OS primitives.
 * See bench_platform.h for the contract.
 */
#include "bench_platform.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

int bench_platform_init(void)
{
	return 0;
}

void bench_platform_shutdown(void)
{
}

uint64_t bench_now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
}

int bench_spawn_sleeper(bench_sleeper *sl)
{
	pid_t pid = fork();
	if (pid < 0) {
		return -errno;
	}
	if (pid == 0) {
		bench_sleep_forever();
	}
	sl->pid = pid;
	sl->handle = 0;
	return 0;
}

void bench_kill_sleeper(const bench_sleeper *sl)
{
	kill(sl->pid, SIGKILL);
}

void bench_close_sleeper(bench_sleeper *sl)
{
	(void) sl;
}

void bench_sleep_forever(void)
{
	for (;;) {
		pause();
	}
}

int bench_sig_number(int which)
{
	return SIGRTMIN + 2 + which;
}

int bench_sig_block(int signo)
{
	sigset_t set;
	sigemptyset(&set);
	sigaddset(&set, signo);
	int err = pthread_sigmask(SIG_BLOCK, &set, NULL);
	return err ? -err : 0;
}

int bench_sig_queue(int signo, int value)
{
	union sigval v;
	v.sival_int = value;
	return sigqueue(getpid(), signo, v) < 0 ? -errno : 0;
}

int bench_sig_value(const ior_siginfo_t *info)
{
	return info->si_value.sival_int;
}

int bench_sig_drain(int signo)
{
	sigset_t set;
	sigemptyset(&set);
	sigaddset(&set, signo);
	int n = 0;
	for (;;) {
		sigset_t pending;
		if (sigpending(&pending) < 0 || !sigismember(&pending, signo)) {
			return n;
		}
		int sig;
		if (sigwait(&set, &sig) != 0) {
			return n;
		}
		n++;
	}
}

typedef struct posix_thread_start {
	void (*fn)(void *);
	void *arg;
} posix_thread_start;

static void *posix_thread_main(void *arg)
{
	posix_thread_start st = *(posix_thread_start *) arg;
	free(arg);
	st.fn(st.arg);
	return NULL;
}

int bench_thread_start(bench_thread *t, void (*fn)(void *), void *arg)
{
	posix_thread_start *st = malloc(sizeof(*st));
	if (!st) {
		return -ENOMEM;
	}
	st->fn = fn;
	st->arg = arg;
	pthread_t tid;
	int err = pthread_create(&tid, NULL, posix_thread_main, st);
	if (err) {
		free(st);
		return -err;
	}
	t->handle = (uintptr_t) tid;
	return 0;
}

void bench_thread_join(bench_thread *t)
{
	pthread_join((pthread_t) t->handle, NULL);
}

typedef struct posix_gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	uint32_t credits;
	int closed;
} posix_gate;

int bench_gate_init(bench_gate *g)
{
	posix_gate *pg = calloc(1, sizeof(*pg));
	if (!pg) {
		return -ENOMEM;
	}
	pthread_mutex_init(&pg->lock, NULL);
	pthread_cond_init(&pg->cond, NULL);
	g->impl = pg;
	return 0;
}

void bench_gate_destroy(bench_gate *g)
{
	posix_gate *pg = g->impl;
	if (!pg) {
		return;
	}
	pthread_cond_destroy(&pg->cond);
	pthread_mutex_destroy(&pg->lock);
	free(pg);
	g->impl = NULL;
}

void bench_gate_post(bench_gate *g, uint32_t n)
{
	posix_gate *pg = g->impl;
	pthread_mutex_lock(&pg->lock);
	pg->credits += n;
	pthread_cond_signal(&pg->cond);
	pthread_mutex_unlock(&pg->lock);
}

uint32_t bench_gate_take(bench_gate *g)
{
	posix_gate *pg = g->impl;
	pthread_mutex_lock(&pg->lock);
	while (pg->credits == 0 && !pg->closed) {
		pthread_cond_wait(&pg->cond, &pg->lock);
	}
	uint32_t n = pg->credits;
	pg->credits = 0;
	pthread_mutex_unlock(&pg->lock);
	return n;
}

void bench_gate_close(bench_gate *g)
{
	posix_gate *pg = g->impl;
	pthread_mutex_lock(&pg->lock);
	pg->closed = 1;
	pthread_cond_broadcast(&pg->cond);
	pthread_mutex_unlock(&pg->lock);
}

int bench_fd_is_valid(ior_fd_t fd)
{
	return fd >= 0;
}

int bench_wait_readable(ior_fd_t fd, int timeout_ms)
{
	struct pollfd pfd = { .fd = fd, .events = POLLIN };
	int ret;
	do {
		ret = poll(&pfd, 1, timeout_ms);
	} while (ret < 0 && errno == EINTR);
	if (ret < 0) {
		return -errno;
	}
	return ret > 0 ? 1 : 0;
}

void bench_close_fd(ior_fd_t fd)
{
	if (bench_fd_is_valid(fd)) {
		close(fd);
	}
}

void bench_close_fd_abort(ior_fd_t fd)
{
	if (bench_fd_is_valid(fd)) {
		struct linger lg = { .l_onoff = 1, .l_linger = 0 };
		(void) setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
		close(fd);
	}
}

ior_fd_t bench_fd_from_res(int32_t res)
{
	return res;
}

int bench_make_listener(ior_fd_t *fd, struct sockaddr_storage *addr, socklen_t *addrlen)
{
	int l = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (l < 0) {
		return -errno;
	}
	struct sockaddr_in a;
	memset(&a, 0, sizeof(a));
	a.sin_family = AF_INET;
	a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	socklen_t alen = sizeof(a);
	if (bind(l, (struct sockaddr *) &a, sizeof(a)) < 0 || listen(l, SOMAXCONN) < 0
			|| getsockname(l, (struct sockaddr *) &a, &alen) < 0) {
		int err = errno;
		close(l);
		return -err;
	}
	memcpy(addr, &a, sizeof(a));
	*addrlen = sizeof(a);
	*fd = l;
	return 0;
}

int bench_make_tcp_socket(ior_fd_t *fd)
{
	int s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (s < 0) {
		return -errno;
	}
	*fd = s;
	return 0;
}

const char *bench_default_workspace(void)
{
	return "/tmp/ior";
}

int bench_ensure_dir(const char *path)
{
	if (mkdir(path, 0700) == 0 || errno == EEXIST) {
		return 0;
	}
	return -errno;
}

int bench_make_tcp_pair(ior_fd_t fds[2])
{
	int listener = -1, client = -1, accepted = -1;
	struct sockaddr_in addr;
	socklen_t addr_len = sizeof(addr);
	int one = 1;

	listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (listener < 0) {
		goto fail;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0; /* ephemeral */

	if (bind(listener, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
		goto fail;
	}
	if (listen(listener, 1) < 0) {
		goto fail;
	}
	if (getsockname(listener, (struct sockaddr *) &addr, &addr_len) < 0) {
		goto fail;
	}

	client = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (client < 0) {
		goto fail;
	}
	if (connect(client, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
		goto fail;
	}

	accepted = accept(listener, NULL, NULL);
	if (accepted < 0) {
		goto fail;
	}

	close(listener);
	listener = -1;

	/* Disable Nagle so ping-pong latency reflects the I/O path, not coalescing. */
	setsockopt(accepted, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	setsockopt(client, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

	fds[0] = accepted;
	fds[1] = client;
	return 0;

fail:
	if (accepted >= 0) {
		close(accepted);
	}
	if (client >= 0) {
		close(client);
	}
	if (listener >= 0) {
		close(listener);
	}
	return -EIO;
}

ior_fd_t bench_open_tmpfile(const char *dir, uint64_t size)
{
	char template[4096];
	int n = snprintf(template, sizeof(template), "%s/ior_bench_XXXXXX", dir);
	if (n < 0 || (size_t) n >= sizeof(template)) {
		return IOR_INVALID_FD;
	}

	int fd = mkstemp(template);
	if (fd < 0) {
		return IOR_INVALID_FD;
	}
	/* Unlink now: the file stays alive while the fd is open and disappears on
	 * close, so callers never need to track a path and the workspace dir stays
	 * clean. */
	unlink(template);

	/* Fill with real data (not a sparse hole) so reads do actual work. */
	if (size > 0) {
		static char buf[65536];
		memset(buf, 0xab, sizeof(buf));
		uint64_t remaining = size;
		while (remaining > 0) {
			size_t chunk = remaining < sizeof(buf) ? (size_t) remaining : sizeof(buf);
			ssize_t w = write(fd, buf, chunk);
			if (w <= 0) {
				close(fd);
				return IOR_INVALID_FD;
			}
			remaining -= (uint64_t) w;
		}
	}

	return fd;
}
