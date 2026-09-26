/* SPDX-License-Identifier: BSD-3-Clause */
/*
 * iocp_fault.c - the IOCP backend with PostQueuedCompletionStatus refusing
 * completion packets, as it does when nonpaged pool runs out.
 *
 * Compiled in place of ior_iocp.c into a library that the existing test
 * programs are linked against again (see CMakeLists.txt), so every synthetic
 * completion - NOPs, timers, link timeouts, work, polls, cancels - takes the
 * backlog path at the rate IOR_FAULT_PQCS_RATE sets (a percentage, default
 * 100). Wake packets (NULL OVERLAPPED) are let through: a blocked consumer
 * has no other way to hear of a kept completion.
 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <windows.h>
#include <stdlib.h>

static BOOL fault_pqcs(HANDLE port, DWORD bytes, ULONG_PTR key, LPOVERLAPPED ov);
#define PostQueuedCompletionStatus fault_pqcs
#include "ior_iocp.c"
#undef PostQueuedCompletionStatus

static volatile LONG fault_rate = -1;
static volatile LONG fault_seq;

static BOOL fault_pqcs(HANDLE port, DWORD bytes, ULONG_PTR key, LPOVERLAPPED ov)
{
	if (fault_rate < 0) {
		const char *env = getenv("IOR_FAULT_PQCS_RATE");
		InterlockedExchange(&fault_rate, env ? atoi(env) : 100);
	}
	if (ov) {
		// A cheap LCG over a shared counter: spread, not random.
		uint32_t x = (uint32_t) InterlockedIncrement(&fault_seq) * 1103515245u + 12345u;
		if ((LONG) ((x >> 8) % 100) < fault_rate) {
			SetLastError(ERROR_NO_SYSTEM_RESOURCES);
			return FALSE;
		}
	}
	return PostQueuedCompletionStatus(port, bytes, key, ov);
}
