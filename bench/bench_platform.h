/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * bench_platform.h - OS resource primitives for the ior benchmark.
 *
 * The ior API abstracts the I/O *operations* themselves, so the benchmark
 * driver, scenarios and metrics are fully portable and free of #ifdefs. The
 * only platform-divergent surface is the setup/teardown of the OS resources a
 * scenario runs against: real loopback TCP sockets, temp files, and a
 * high-resolution monotonic clock. That surface lives behind this header and is
 * implemented once per platform (bench_platform_posix.c / bench_platform_win.c);
 * CMake compiles only the matching one.
 */
#ifndef BENCH_PLATFORM_H
#define BENCH_PLATFORM_H

#include <stdint.h>
#include "../src/ior.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Process-wide init/shutdown for the platform layer (e.g. WSAStartup on
 * Windows). Call once at program start / end. Returns 0 on success, negative on
 * failure.
 */
int bench_platform_init(void);
void bench_platform_shutdown(void);

/*
 * Current value of the monotonic clock, in nanoseconds: CLOCK_MONOTONIC on
 * POSIX, QueryPerformanceCounter on Windows. Used for all benchmark timing.
 */
uint64_t bench_now_ns(void);

/*
 * Create a connected pair of real loopback TCP sockets (AF_INET, 127.0.0.1).
 *
 * Unlike a Unix-domain socketpair, this goes through the real kernel TCP/IP
 * stack - which is what the PHP socket-offload workload this benchmark models
 * actually exercises - and it is the only portable option on Windows anyway.
 *
 * fds[0] and fds[1] are the two connected ends; bytes sent on one are received
 * on the other. TCP_NODELAY is set on both ends so request/response latency is
 * not distorted by Nagle. On Windows both ends are created WSA_FLAG_OVERLAPPED
 * so the IOCP backend can issue overlapped I/O on them.
 *
 * Returns 0 on success; on success the caller closes both ends with
 * bench_close_fd(). Returns a negative errno-style code on failure.
 */
int bench_make_tcp_pair(ior_fd_t fds[2]);

/*
 * Default workspace directory for benchmark temp files when the user does not
 * override it on the command line: "/tmp/ior" on POSIX, "%TEMP%\ior" on Windows.
 * Returns a pointer to a static, null-terminated string.
 */
const char *bench_default_workspace(void);

/*
 * Ensure the workspace directory `path` exists (creating it if needed). Only the
 * final component is created, so the parent must already exist (true for the
 * defaults above). Returns 0 on success or if it already exists, negative on
 * failure.
 */
int bench_ensure_dir(const char *path);

/*
 * Create a temp file of exactly `size` bytes inside workspace directory `dir`,
 * filled with data (not sparse), opened for read+write and usable by the active
 * ior backend. On Windows the handle is opened FILE_FLAG_OVERLAPPED (required by
 * IOCP) and FILE_FLAG_DELETE_ON_CLOSE; on POSIX the file is unlinked
 * immediately. In both cases the file lives only while the fd is open and
 * closing it with bench_close_fd() removes it - the workspace dir itself is left
 * in place.
 *
 * Returns IOR_INVALID_FD on failure.
 */
ior_fd_t bench_open_tmpfile(const char *dir, uint64_t size);

/*
 * Create a listening loopback TCP socket on an ephemeral port (backlog
 * SOMAXCONN). Fills *addr and *addrlen with the address to connect to.
 * Returns 0 or a negative errno-style code; close it with bench_close_fd().
 */
int bench_make_listener(ior_fd_t *fd, struct sockaddr_storage *addr, socklen_t *addrlen);

/*
 * Create an unconnected TCP socket for ior_prep_connect() (WSA_FLAG_OVERLAPPED
 * on Windows). Returns 0 or a negative errno-style code.
 */
int bench_make_tcp_socket(ior_fd_t *fd);

/*
 * Close a connected socket with a reset (SO_LINGER, zero timeout) so it leaves
 * no TIME_WAIT behind: connection churn would otherwise exhaust the ephemeral
 * port range within seconds.
 */
void bench_close_fd_abort(ior_fd_t fd);

/* The accepted socket an IOR_OP_ACCEPT completion carries in res (>= 0). */
ior_fd_t bench_fd_from_res(int32_t res);

/* Close a descriptor returned by bench_make_tcp_pair() or bench_open_tmpfile(). */
void bench_close_fd(ior_fd_t fd);

/* True if fd is a valid descriptor (handles the HANDLE-vs-int difference). */
int bench_fd_is_valid(ior_fd_t fd);

/*
 * A child process that sleeps until killed, for --waits: a forked child in
 * pause() on POSIX, this program re-run with --sleeper on Windows (where the
 * process handle also keeps the pid from being recycled). The kill leaves
 * reaping to the IOR_OP_WAITPID op waiting on it; close releases the handle
 * once that completed.
 */
typedef struct bench_sleeper {
	ior_pid_t pid;
	uintptr_t handle;
} bench_sleeper;

int bench_spawn_sleeper(bench_sleeper *sl);
void bench_kill_sleeper(const bench_sleeper *sl);
void bench_close_sleeper(bench_sleeper *sl);

/* The --sleeper side: never returns. */
void bench_sleep_forever(void);

/*
 * Signals for the sigwait scenario and --sigwaits: a real-time signal on
 * POSIX (SIGRTMIN + which), queued with a value so every instance is
 * delivered and can be told apart. Windows has none of this: -1, and the
 * scenario reports -ENOTSUP.
 */
int bench_sig_number(int which);

/* Block signo in the calling thread (and in every thread it creates after),
 * so it stays pending for the ops instead of running its default action. */
int bench_sig_block(int signo);

/* Queue signo to this process carrying value. Returns 0, -EAGAIN when the
 * queue is full (retry after a moment), else a negative errno. */
int bench_sig_queue(int signo, int value);

/* The value bench_sig_queue() sent, out of what an IOR_OP_SIGWAIT stored. */
int bench_sig_value(const ior_siginfo_t *info);

/* Take every pending signo without waiting; returns how many there were. */
int bench_sig_drain(int signo);

/* A thread, for the sigwait scenario's sender. */
typedef struct bench_thread {
	uintptr_t handle;
} bench_thread;

int bench_thread_start(bench_thread *t, void (*fn)(void *), void *arg);
void bench_thread_join(bench_thread *t);

/*
 * A counting gate between two threads: post() adds credits and wakes the
 * taker, take() blocks until there are credits and takes them all, or
 * returns 0 once the gate is closed and empty.
 */
typedef struct bench_gate {
	void *impl;
} bench_gate;

int bench_gate_init(bench_gate *g);
void bench_gate_destroy(bench_gate *g);
void bench_gate_post(bench_gate *g, uint32_t n);
uint32_t bench_gate_take(bench_gate *g);
void bench_gate_close(bench_gate *g);

/*
 * Wait up to timeout_ms for a descriptor from ior_notify_fd() to become
 * readable (poll() on POSIX, WSAPoll() on Windows). Returns 1 if readable, 0
 * on timeout, negative errno on error.
 */
int bench_wait_readable(ior_fd_t fd, int timeout_ms);

#ifdef __cplusplus
}
#endif

#endif /* BENCH_PLATFORM_H */
