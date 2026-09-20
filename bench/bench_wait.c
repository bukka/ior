/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * bench_wait.c - how a scenario blocks for completions.
 *
 * By default a scenario blocks in ior_wait_cqe() (under BENCH_TRACE through
 * the stall watchdog). With --notify it instead does what a loop embedding a
 * context does: waits for ior_notify_fd() to become readable, clears it, and
 * lets its next peek reap whatever landed. Clearing before the peek is safe
 * under the descriptor's contract: a completion posted after the clear signals
 * again, and one posted before it is already visible to the peek.
 */
#include "bench_platform.h"
#include "bench_scenario.h"
#include "bench_trace.h"

#include <errno.h>

int bench_wait_completion(ior_ctx *ctx, const bench_options *opts, ior_cqe **cqe, uint64_t progress)
{
	if (!opts->notify) {
		return BENCH_WAIT_CQE(ctx, cqe, progress);
	}

	*cqe = NULL;
	ior_fd_t nfd = ior_notify_fd(ctx);
	if (!bench_fd_is_valid(nfd)) {
		return -EIO;
	}

	int ret = bench_wait_readable(nfd, 1000);
	if (ret < 0) {
		return ret;
	}
	if (ret == 0) {
		return -ETIME;
	}
	ret = ior_notify_clear(ctx);
	return ret < 0 ? ret : -EAGAIN;
}
