/* SPDX-License-Identifier: BSD-3-Clause */
#include "config.h"

#ifdef IOR_HAVE_THREADS

#include "ior.h"
#include "ior_threads_poller.h"
#include "ior_threads_event.h"
#include "ior_worker_pool.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/event.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define IOR_POLLER_MAX_EVENTS 64
#define IOR_POLLER_NO_SLOT ((size_t) -1)

typedef struct ior_poller_fd_node ior_poller_fd_node;

typedef struct ior_poller_req {
	int fd;
	uint32_t mask;
	uint64_t deadline_ns; /* absolute monotonic, 0 = none */
	void *req;
	int multi; /* persistent: completes at every edge until dropped */
	int cancelled; /* set by cancel(); completes with -ECANCELED */
	int ended; /* an edge was declined: retires with res as its last result */
	int res; /* staged result while the fd bookkeeping completes */
	int retired; /* staged with its last result: unlinked, freed after the callback */
	ior_poller_fd_node *node; /* the node it waits in, NULL while incoming or retired */
	size_t heap_slot; /* in the deadline heap, IOR_POLLER_NO_SLOT if not */
	struct ior_poller_req *hnext; /* cancel lookup chain, while it can be cancelled */
	struct ior_poller_req *next; /* incoming queue, then the node's list */
	struct ior_poller_req *done_next; /* staged completions */
} ior_poller_req;

/*
 * kqueue registers per (fd, filter) pair, so requests are grouped in per-fd
 * nodes, found through a table indexed by the descriptor, and the node tracks
 * which filters are currently registered (`reg`, as IOR_POLL_IN/OUT bits, or
 * IOR_THREADS_POLLER_PROC for a process watch). A process watch's node is
 * keyed by pid, which is no small index, so those nodes are a list of their
 * own, searched for a pid: there are few. A multishot request needs EV_CLEAR
 * on its filters, which is per registration, so it watches a dup(2) of its
 * descriptor in a node of its own (`multi`, with `fd` the dup), which the
 * table does not name. A node's requests are a plain list: there are few per
 * descriptor.
 */
struct ior_poller_fd_node {
	int fd;
	int proc; /* fd is a pid watched with EVFILT_PROC */
	int multi;
	int dirty; /* on the dirty list: a request in it was cancelled or ended */
	uint32_t reg;
	ior_poller_req *reqs;
	ior_poller_fd_node *prev; /* fds or procs, for shutdown and fork */
	ior_poller_fd_node *next;
	ior_poller_fd_node *dirty_prev;
	ior_poller_fd_node *dirty_next;
};

/*
 * `lock` guards the incoming queue, the nodes and everything that finds them
 * so that cancel() can find a request from any thread. The poller thread
 * holds it for all this work and drops it only around kevent() and around
 * completion callbacks, which run with no poller lock held (owners take
 * their own locks in them).
 *
 * Nothing a pass does is linear in the number of registered descriptors: a
 * one-shot request finds its node through `by_fd`, a cancel its request
 * through `map` (keyed by the owner's request), a pass sweeps only the nodes
 * on the dirty list, and deadlines are a min-heap. A thousand idle
 * connections cost nothing while they stay idle.
 */
struct ior_threads_poller {
	pthread_t thread;
	int kq;
	ior_threads_event event; /* wakeup for add()/cancel()/destroy() */
	pthread_mutex_t lock;
	ior_poller_req *incoming_head;
	ior_poller_req *incoming_tail;
	_Atomic int shutdown;
	void *owner;
	ior_threads_poller_cb cb;
	ior_poller_fd_node *fds; /* descriptor nodes, multishot ones included */
	ior_poller_fd_node *procs; /* process watch nodes */
	ior_poller_fd_node **by_fd; /* one-shot node per descriptor */
	size_t by_fd_cap;
	ior_poller_fd_node *dirty;
	ior_poller_req **heap; /* requests with a deadline, nearest first */
	size_t heap_len;
	size_t heap_cap;
	ior_poller_req **map; /* cancellable requests by owner request */
	size_t map_cap; /* a power of two */
	size_t map_len;
};

/* ===== Cancel lookup (lock held) ===== */

static size_t ior_poller_map_slot(const ior_threads_poller *poller, const void *req)
{
	uintptr_t v = (uintptr_t) req;
	v ^= v >> 17;
	v *= (uintptr_t) 0x9E3779B97F4A7C15ULL;
	v ^= v >> 29;
	return (size_t) v & (poller->map_cap - 1);
}

// Grow the table when it holds more requests than slots; failing keeps it.
static void ior_poller_map_grow(ior_threads_poller *poller)
{
	size_t cap = poller->map_cap * 2;
	ior_poller_req **map = calloc(cap, sizeof(*map));
	if (!map) {
		return;
	}
	ior_poller_req **old = poller->map;
	size_t old_cap = poller->map_cap;
	poller->map = map;
	poller->map_cap = cap;
	for (size_t i = 0; i < old_cap; i++) {
		ior_poller_req *r = old[i];
		while (r) {
			ior_poller_req *next = r->hnext;
			size_t slot = ior_poller_map_slot(poller, r->req);
			r->hnext = map[slot];
			map[slot] = r;
			r = next;
		}
	}
	free(old);
}

static void ior_poller_map_insert(ior_threads_poller *poller, ior_poller_req *r)
{
	if (poller->map_len >= poller->map_cap) {
		ior_poller_map_grow(poller);
	}
	size_t slot = ior_poller_map_slot(poller, r->req);
	r->hnext = poller->map[slot];
	poller->map[slot] = r;
	poller->map_len++;
}

static void ior_poller_map_remove(ior_threads_poller *poller, ior_poller_req *r)
{
	ior_poller_req **pp = &poller->map[ior_poller_map_slot(poller, r->req)];
	while (*pp && *pp != r) {
		pp = &(*pp)->hnext;
	}
	if (*pp) {
		*pp = r->hnext;
		r->hnext = NULL;
		poller->map_len--;
	}
}

/* ===== Deadline heap (lock held) ===== */

static void ior_poller_heap_set(ior_threads_poller *poller, size_t i, ior_poller_req *r)
{
	poller->heap[i] = r;
	r->heap_slot = i;
}

static void ior_poller_heap_up(ior_threads_poller *poller, size_t i)
{
	ior_poller_req *r = poller->heap[i];
	while (i > 0) {
		size_t parent = (i - 1) / 2;
		if (poller->heap[parent]->deadline_ns <= r->deadline_ns) {
			break;
		}
		ior_poller_heap_set(poller, i, poller->heap[parent]);
		i = parent;
	}
	ior_poller_heap_set(poller, i, r);
}

static void ior_poller_heap_down(ior_threads_poller *poller, size_t i)
{
	ior_poller_req *r = poller->heap[i];
	for (;;) {
		size_t child = 2 * i + 1;
		if (child >= poller->heap_len) {
			break;
		}
		if (child + 1 < poller->heap_len
				&& poller->heap[child + 1]->deadline_ns < poller->heap[child]->deadline_ns) {
			child++;
		}
		if (r->deadline_ns <= poller->heap[child]->deadline_ns) {
			break;
		}
		ior_poller_heap_set(poller, i, poller->heap[child]);
		i = child;
	}
	ior_poller_heap_set(poller, i, r);
}

// Room for one more; checked before a request is linked anywhere.
static int ior_poller_heap_reserve(ior_threads_poller *poller)
{
	if (poller->heap_len < poller->heap_cap) {
		return 0;
	}
	size_t cap = poller->heap_cap ? poller->heap_cap * 2 : 64;
	ior_poller_req **heap = realloc(poller->heap, cap * sizeof(*heap));
	if (!heap) {
		return -ENOMEM;
	}
	poller->heap = heap;
	poller->heap_cap = cap;
	return 0;
}

static void ior_poller_heap_push(ior_threads_poller *poller, ior_poller_req *r)
{
	size_t i = poller->heap_len++;
	ior_poller_heap_set(poller, i, r);
	ior_poller_heap_up(poller, i);
}

static void ior_poller_heap_remove(ior_threads_poller *poller, ior_poller_req *r)
{
	size_t i = r->heap_slot;
	r->heap_slot = IOR_POLLER_NO_SLOT;
	ior_poller_req *last = poller->heap[--poller->heap_len];
	if (i == poller->heap_len) {
		return;
	}
	ior_poller_heap_set(poller, i, last);
	ior_poller_heap_down(poller, i);
	ior_poller_heap_up(poller, last->heap_slot);
}

/* ===== Nodes (lock held) ===== */

// Room in the descriptor table for fd.
static int ior_poller_by_fd_reserve(ior_threads_poller *poller, int fd)
{
	if ((size_t) fd < poller->by_fd_cap) {
		return 0;
	}
	size_t cap = poller->by_fd_cap ? poller->by_fd_cap : 64;
	while (cap <= (size_t) fd) {
		cap *= 2;
	}
	ior_poller_fd_node **by_fd = realloc(poller->by_fd, cap * sizeof(*by_fd));
	if (!by_fd) {
		return -ENOMEM;
	}
	memset(by_fd + poller->by_fd_cap, 0, (cap - poller->by_fd_cap) * sizeof(*by_fd));
	poller->by_fd = by_fd;
	poller->by_fd_cap = cap;
	return 0;
}

static void ior_poller_dirty_push(ior_threads_poller *poller, ior_poller_fd_node *node)
{
	if (node->dirty) {
		return;
	}
	node->dirty = 1;
	node->dirty_prev = NULL;
	node->dirty_next = poller->dirty;
	if (poller->dirty) {
		poller->dirty->dirty_prev = node;
	}
	poller->dirty = node;
}

static void ior_poller_dirty_remove(ior_threads_poller *poller, ior_poller_fd_node *node)
{
	if (!node->dirty) {
		return;
	}
	if (node->dirty_prev) {
		node->dirty_prev->dirty_next = node->dirty_next;
	} else {
		poller->dirty = node->dirty_next;
	}
	if (node->dirty_next) {
		node->dirty_next->dirty_prev = node->dirty_prev;
	}
	node->dirty = 0;
}

static void ior_poller_node_link(ior_threads_poller *poller, ior_poller_fd_node *node)
{
	ior_poller_fd_node **head = node->proc ? &poller->procs : &poller->fds;
	node->prev = NULL;
	node->next = *head;
	if (*head) {
		(*head)->prev = node;
	}
	*head = node;
	if (!node->proc && !node->multi) {
		poller->by_fd[node->fd] = node;
	}
}

/*
 * Stage r for completion with res on the done list (lock held). A retired
 * request has been unlinked and is freed after its callback; a multishot
 * request reporting an edge stays in its node.
 */
static void ior_poller_stage(ior_poller_req **done, ior_poller_req *r, int res, int retired)
{
	r->res = res;
	r->retired = retired;
	r->done_next = *done;
	*done = r;
}

/*
 * Stage r's last result, r already unlinked from its node (lock held). From
 * here a cancel no longer finds it (-ENOENT): its callback is on the way.
 */
static void ior_poller_retire(
		ior_threads_poller *poller, ior_poller_req *r, int res, ior_poller_req **done)
{
	if (r->heap_slot != IOR_POLLER_NO_SLOT) {
		ior_poller_heap_remove(poller, r);
	}
	ior_poller_map_remove(poller, r);
	r->node = NULL;
	ior_poller_stage(done, r, res, 1);
}

/*
 * Complete a batch of staged requests. Must run with the lock released and
 * only after the fd's kevent registrations have been updated: once the
 * callback fires, the owner may close the fd, so the poller must no longer
 * reference it.
 */
static void ior_poller_complete_list(ior_threads_poller *poller, ior_poller_req *done)
{
	while (done) {
		ior_poller_req *next = done->done_next;
		int declined = poller->cb(poller->owner, done->req, done->res, !done->retired);
		if (done->retired) {
			free(done);
		} else if (declined) {
			/* The next pass retires the request, with res still holding the
			 * declined readiness. Its node lasts while it is in it. */
			pthread_mutex_lock(&poller->lock);
			done->ended = 1;
			ior_poller_dirty_push(poller, done->node);
			pthread_mutex_unlock(&poller->lock);
		}
		done = next;
	}
}

/* A regular file has no readiness edges: it is always ready, matching poll(). */
static int ior_poller_fd_is_regular(int fd)
{
	struct stat st;
	return fstat(fd, &st) == 0 && S_ISREG(st.st_mode);
}

/* Unlink and free an empty node; a multishot node owns its dup. Lock held. */
static void ior_poller_node_drop(ior_threads_poller *poller, ior_poller_fd_node *node)
{
	if (node->multi) {
		close(node->fd);
	} else if (!node->proc && (size_t) node->fd < poller->by_fd_cap
			&& poller->by_fd[node->fd] == node) {
		poller->by_fd[node->fd] = NULL;
	}
	if (node->prev) {
		node->prev->next = node->next;
	} else if (node->proc) {
		poller->procs = node->next;
	} else {
		poller->fds = node->next;
	}
	if (node->next) {
		node->next->prev = node->prev;
	}
	ior_poller_dirty_remove(poller, node);
	free(node);
}

/*
 * Sync the node's kevent registrations to the union of its remaining masks
 * and drop the node once empty. A failed EV_ADD stages the requests that
 * needed that filter: as the requested mask for a regular file (always
 * ready, matching poll()), as -errno otherwise. Lock held.
 */
static void ior_poller_node_sync(
		ior_threads_poller *poller, ior_poller_fd_node *node, ior_poller_req **done)
{
	if (node->proc) {
		/* One NOTE_EXIT registration serves every request on the pid. A
		 * process that is already gone (ESRCH) fails them all; the owner
		 * collects its state with waitpid. */
		struct kevent kev;
		if (node->reqs && !(node->reg & IOR_THREADS_POLLER_PROC)) {
			EV_SET(&kev, node->fd, EVFILT_PROC, EV_ADD, NOTE_EXIT, 0, node);
			if (kevent(poller->kq, &kev, 1, NULL, 0, NULL) == 0) {
				node->reg |= IOR_THREADS_POLLER_PROC;
			} else {
				int err = errno;
				ior_poller_req *r = node->reqs;
				node->reqs = NULL;
				while (r) {
					ior_poller_req *next = r->next;
					ior_poller_retire(poller, r, -err, done);
					r = next;
				}
			}
		}
		if (!node->reqs) {
			if (node->reg & IOR_THREADS_POLLER_PROC) {
				EV_SET(&kev, node->fd, EVFILT_PROC, EV_DELETE, 0, 0, NULL);
				(void) kevent(poller->kq, &kev, 1, NULL, 0, NULL);
			}
			ior_poller_node_drop(poller, node);
		}
		return;
	}

	for (int pass = 0; pass < 2; pass++) {
		uint32_t bit = pass == 0 ? IOR_POLL_IN : IOR_POLL_OUT;
		int16_t filter = pass == 0 ? EVFILT_READ : EVFILT_WRITE;

		uint32_t want = 0;
		for (ior_poller_req *r = node->reqs; r; r = r->next) {
			want |= r->mask;
		}

		struct kevent kev;
		if ((want & bit) && !(node->reg & bit)) {
			EV_SET(&kev, node->fd, filter, EV_ADD | (node->multi ? EV_CLEAR : 0), 0, 0, node);
			if (kevent(poller->kq, &kev, 1, NULL, 0, NULL) == 0) {
				node->reg |= bit;
			} else {
				int err = errno;
				int regular = ior_poller_fd_is_regular(node->fd);
				ior_poller_req **pp = &node->reqs;
				while (*pp) {
					ior_poller_req *r = *pp;
					if (r->mask & bit) {
						*pp = r->next;
						uint32_t ready = r->mask & (IOR_POLL_IN | IOR_POLL_OUT);
						ior_poller_retire(poller, r, regular ? (int) ready : -err, done);
					} else {
						pp = &r->next;
					}
				}
			}
		} else if (!(want & bit) && (node->reg & bit)) {
			EV_SET(&kev, node->fd, filter, EV_DELETE, 0, 0, NULL);
			(void) kevent(poller->kq, &kev, 1, NULL, 0, NULL);
			node->reg &= ~bit;
		}
	}

	if (!node->reqs) {
		for (int pass = 0; pass < 2; pass++) {
			uint32_t bit = pass == 0 ? IOR_POLL_IN : IOR_POLL_OUT;
			if (node->reg & bit) {
				struct kevent kev;
				EV_SET(&kev, node->fd, pass == 0 ? EVFILT_READ : EVFILT_WRITE, EV_DELETE, 0, 0,
						NULL);
				(void) kevent(poller->kq, &kev, 1, NULL, 0, NULL);
			}
		}
		ior_poller_node_drop(poller, node);
	}
}

static void ior_poller_ingest_one(
		ior_threads_poller *poller, ior_poller_req *r, ior_poller_req **done)
{
	if (r->cancelled) {
		ior_poller_retire(poller, r, -ECANCELED, done);
		return;
	}

	int proc = (r->mask & IOR_THREADS_POLLER_PROC) != 0;
	if ((r->deadline_ns && ior_poller_heap_reserve(poller) < 0)
			|| (!r->multi && !proc && ior_poller_by_fd_reserve(poller, r->fd) < 0)) {
		ior_poller_retire(poller, r, -ENOMEM, done);
		return;
	}

	/*
	 * kqueue registers a regular file happily and reports it readable, where
	 * epoll refuses it (EPERM). There are no edges to watch for, so a
	 * multishot ends here with the requested mask as its last result, as it
	 * does on epoll and io_uring. A one-shot needs no special case: its
	 * single completion is the same either way.
	 */
	if (r->multi && !proc && ior_poller_fd_is_regular(r->fd)) {
		uint32_t ready = r->mask & (IOR_POLL_IN | IOR_POLL_OUT);
		ior_poller_retire(poller, r, ready ? (int) ready : -EINVAL, done);
		return;
	}

	ior_poller_fd_node *node = NULL;
	if (proc) {
		node = poller->procs;
		while (node && node->fd != r->fd) {
			node = node->next;
		}
	} else if (!r->multi) {
		node = poller->by_fd[r->fd];
	}

	if (!node) {
		int fd = r->fd;
		if (r->multi) {
			fd = fcntl(r->fd, F_DUPFD_CLOEXEC, 0);
			if (fd < 0) {
				ior_poller_retire(poller, r, -errno, done);
				return;
			}
		}
		node = calloc(1, sizeof(*node));
		if (!node) {
			if (r->multi) {
				close(fd);
			}
			ior_poller_retire(poller, r, -ENOMEM, done);
			return;
		}
		node->fd = fd;
		node->proc = proc;
		node->multi = r->multi;
		ior_poller_node_link(poller, node);
	}

	r->next = node->reqs;
	node->reqs = r;
	r->node = node;
	if (r->deadline_ns) {
		ior_poller_heap_push(poller, r);
	}
	ior_poller_node_sync(poller, node, done); /* may retire r and free node */
}

/* Lock held. */
static void ior_poller_ingest_incoming(ior_threads_poller *poller, ior_poller_req **done)
{
	ior_poller_req *r = poller->incoming_head;
	poller->incoming_head = NULL;
	poller->incoming_tail = NULL;

	while (r) {
		ior_poller_req *next = r->next;
		ior_poller_ingest_one(poller, r, done);
		r = next;
	}
}

/* Nearest deadline as a kevent timeout (NULL = none). Lock held. */
static struct timespec *ior_poller_timeout(ior_threads_poller *poller, struct timespec *ts)
{
	if (!poller->heap_len) {
		return NULL;
	}
	uint64_t nearest = poller->heap[0]->deadline_ns;
	uint64_t now = ior_worker_pool_monotonic_ns();
	uint64_t left = nearest > now ? nearest - now : 0;
	ts->tv_sec = (time_t) (left / 1000000000ULL);
	ts->tv_nsec = (long) (left % 1000000000ULL);
	return ts;
}

/*
 * Retire what cancel() marked or what declined its last edge, on the nodes
 * the dirty list names, then every request whose deadline has passed,
 * staging -ECANCELED / its readiness / -ETIME. Lock held.
 */
static void ior_poller_sweep(ior_threads_poller *poller, ior_poller_req **done)
{
	while (poller->dirty) {
		ior_poller_fd_node *node = poller->dirty;
		ior_poller_dirty_remove(poller, node);
		int changed = 0;
		ior_poller_req **pp = &node->reqs;
		while (*pp) {
			ior_poller_req *r = *pp;
			if (r->cancelled || r->ended) {
				*pp = r->next;
				ior_poller_retire(poller, r, r->cancelled ? -ECANCELED : r->res, done);
				changed = 1;
			} else {
				pp = &r->next;
			}
		}
		if (changed) {
			ior_poller_node_sync(poller, node, done); /* may free node */
		}
	}

	uint64_t now = ior_worker_pool_monotonic_ns();
	while (poller->heap_len && poller->heap[0]->deadline_ns <= now) {
		ior_poller_req *r = poller->heap[0];
		ior_poller_fd_node *node = r->node;
		ior_poller_req **pp = &node->reqs;
		while (*pp != r) {
			pp = &(*pp)->next;
		}
		*pp = r->next;
		ior_poller_retire(poller, r, -ETIME, done);
		ior_poller_node_sync(poller, node, done); /* may free node */
	}
}

/*
 * Lock held. A request cancelled while kevent() ran still reports
 * -ECANCELED: cancel() has already promised that result. A multishot request
 * reports the edge and stays registered; a cancel that lands meanwhile is
 * delivered by the next sweep, after the edge.
 */
static void ior_poller_dispatch(
		ior_threads_poller *poller, ior_poller_fd_node *node, uint32_t ready, ior_poller_req **done)
{
	int changed = 0;
	ior_poller_req **pp = &node->reqs;
	while (*pp) {
		ior_poller_req *r = *pp;
		/* A process watch has one event, exit, which every request on the
		 * pid is waiting for. */
		uint32_t res = node->proc ? ready : ready & (r->mask | IOR_POLL_ERR | IOR_POLL_HUP);
		if (r->cancelled) {
			*pp = r->next;
			ior_poller_retire(poller, r, -ECANCELED, done);
			changed = 1;
		} else if (res && r->multi) {
			ior_poller_stage(done, r, (int) res, 0);
			pp = &r->next;
		} else if (res) {
			*pp = r->next;
			ior_poller_retire(poller, r, (int) res, done);
			changed = 1;
		} else {
			pp = &r->next;
		}
	}
	if (changed) {
		ior_poller_node_sync(poller, node, done); /* may free node */
	}
}

/* Lock held. No kevent bookkeeping: the kq is closed right after in destroy. */
static void ior_poller_cancel_all(ior_threads_poller *poller, ior_poller_req **done)
{
	while (poller->fds || poller->procs) {
		ior_poller_fd_node *node = poller->fds ? poller->fds : poller->procs;
		while (node->reqs) {
			ior_poller_req *r = node->reqs;
			node->reqs = r->next;
			ior_poller_retire(poller, r, -ECANCELED, done);
		}
		ior_poller_node_drop(poller, node);
	}
}

static void *ior_poller_thread(void *arg)
{
	ior_threads_poller *poller = arg;
	struct kevent events[IOR_POLLER_MAX_EVENTS];
	/* One wakeup can deliver a READ and a WRITE event for the same node, and
	 * dispatching one may free it - merge per node before dispatching. */
	struct {
		ior_poller_fd_node *node;
		uint32_t ready;
	} hits[IOR_POLLER_MAX_EVENTS];

	for (;;) {
		ior_poller_req *done = NULL;

		pthread_mutex_lock(&poller->lock);
		ior_poller_ingest_incoming(poller, &done);
		ior_poller_sweep(poller, &done);
		int shutdown = atomic_load_explicit(&poller->shutdown, memory_order_acquire);
		struct timespec ts;
		struct timespec *timeout = shutdown ? NULL : ior_poller_timeout(poller, &ts);
		pthread_mutex_unlock(&poller->lock);

		ior_poller_complete_list(poller, done);
		if (shutdown) {
			break;
		}

		int n = kevent(poller->kq, NULL, 0, events, IOR_POLLER_MAX_EVENTS, timeout);
		if (n < 0 && errno != EINTR) {
			break;
		}

		int nhits = 0;
		for (int i = 0; i < n; i++) {
			if (!events[i].udata) {
				ior_threads_event_clear(&poller->event);
				continue;
			}
			ior_poller_fd_node *node = events[i].udata;
			uint32_t ready;
			if (events[i].flags & EV_ERROR) {
				ready = IOR_POLL_ERR;
			} else if (events[i].filter == EVFILT_PROC) {
				ready = IOR_POLL_IN; /* NOTE_EXIT */
			} else {
				ready = events[i].filter == EVFILT_READ ? IOR_POLL_IN : IOR_POLL_OUT;
				if (events[i].flags & EV_EOF) {
					ready |= IOR_POLL_HUP;
					if (events[i].fflags != 0) {
						ready |= IOR_POLL_ERR;
					}
				}
			}
			int j = 0;
			while (j < nhits && hits[j].node != node) {
				j++;
			}
			if (j == nhits) {
				hits[nhits].node = node;
				hits[nhits].ready = 0;
				nhits++;
			}
			hits[j].ready |= ready;
		}

		done = NULL;
		pthread_mutex_lock(&poller->lock);
		for (int j = 0; j < nhits; j++) {
			ior_poller_dispatch(poller, hits[j].node, hits[j].ready, &done);
		}
		pthread_mutex_unlock(&poller->lock);
		ior_poller_complete_list(poller, done);
	}

	/* Shutdown: fail everything still pending, including late arrivals. */
	ior_poller_req *done = NULL;
	pthread_mutex_lock(&poller->lock);
	ior_poller_ingest_incoming(poller, &done);
	ior_poller_cancel_all(poller, &done);
	pthread_mutex_unlock(&poller->lock);
	ior_poller_complete_list(poller, done);
	return NULL;
}

int ior_threads_poller_create(
		ior_threads_poller **poller_out, void *owner, ior_threads_poller_cb cb)
{
	if (!poller_out || !cb) {
		return -EINVAL;
	}

	ior_threads_poller *poller = calloc(1, sizeof(*poller));
	if (!poller) {
		return -ENOMEM;
	}
	poller->owner = owner;
	poller->cb = cb;
	atomic_init(&poller->shutdown, 0);

	poller->map_cap = 64;
	poller->map = calloc(poller->map_cap, sizeof(*poller->map));
	if (!poller->map) {
		free(poller);
		return -ENOMEM;
	}
	poller->kq = kqueue();
	if (poller->kq < 0) {
		int err = errno;
		free(poller->map);
		free(poller);
		return -err;
	}
	if (ior_threads_event_init(&poller->event) < 0) {
		close(poller->kq);
		free(poller->map);
		free(poller);
		return -ENOMEM;
	}
	if (pthread_mutex_init(&poller->lock, NULL) != 0) {
		ior_threads_event_destroy(&poller->event);
		close(poller->kq);
		free(poller->map);
		free(poller);
		return -ENOMEM;
	}

	/* Wakeup fd is marked by a NULL udata pointer. */
	struct kevent kev;
	EV_SET(&kev, ior_threads_event_get_fd(&poller->event), EVFILT_READ, EV_ADD, 0, 0, NULL);
	if (kevent(poller->kq, &kev, 1, NULL, 0, NULL) < 0
			|| ior_thread_create(&poller->thread, NULL, ior_poller_thread, poller) != 0) {
		pthread_mutex_destroy(&poller->lock);
		ior_threads_event_destroy(&poller->event);
		close(poller->kq);
		free(poller->map);
		free(poller);
		return -ENOMEM;
	}

	*poller_out = poller;
	return 0;
}

int ior_threads_poller_add(
		ior_threads_poller *poller, int fd, uint32_t ior_mask, uint64_t deadline_ns, void *req)
{
	if (!poller) {
		return -EINVAL;
	}
	if (fd < 0) {
		return -EBADF;
	}

	ior_poller_req *r = calloc(1, sizeof(*r));
	if (!r) {
		return -ENOMEM;
	}
	r->fd = fd;
	r->mask = ior_mask & ~IOR_THREADS_POLLER_MULTI;
	r->multi = (ior_mask & IOR_THREADS_POLLER_MULTI) != 0;
	r->deadline_ns = deadline_ns;
	r->req = req;
	r->heap_slot = IOR_POLLER_NO_SLOT;

	/* The poller drains the whole queue once woken: only the add that makes
	 * it non-empty needs to wake it. */
	pthread_mutex_lock(&poller->lock);
	int wake = poller->incoming_head == NULL;
	if (poller->incoming_tail) {
		poller->incoming_tail->next = r;
	} else {
		poller->incoming_head = r;
	}
	poller->incoming_tail = r;
	ior_poller_map_insert(poller, r);
	pthread_mutex_unlock(&poller->lock);

	if (wake) {
		ior_threads_event_signal(&poller->event);
	}
	return 0;
}

int ior_threads_poller_cancel(ior_threads_poller *poller, void *req)
{
	if (!poller) {
		return -EINVAL;
	}

	int found = 0;
	int wake = 0;
	pthread_mutex_lock(&poller->lock);
	ior_poller_req *r = poller->map[ior_poller_map_slot(poller, req)];
	while (r && (r->req != req || r->cancelled)) {
		r = r->hnext;
	}
	if (r) {
		r->cancelled = 1;
		found = 1;
		/* Still incoming, it is retired when the pending wakeup ingests it;
		 * in a node, the node is swept, and like add(), only the cancel that
		 * starts the dirty list needs to wake the poller. */
		if (r->node) {
			wake = poller->dirty == NULL;
			ior_poller_dirty_push(poller, r->node);
		}
	}
	pthread_mutex_unlock(&poller->lock);

	if (!found) {
		return -ENOENT;
	}
	if (wake) {
		ior_threads_event_signal(&poller->event);
	}
	return 0;
}

void ior_threads_poller_forget(ior_threads_poller *poller)
{
	// A multishot node owns a dup of its descriptor.
	if (pthread_mutex_trylock(&poller->lock) == 0) {
		for (ior_poller_fd_node *node = poller->fds; node; node = node->next) {
			if (node->multi) {
				close(node->fd);
			}
		}
		pthread_mutex_unlock(&poller->lock);
	}
	ior_threads_event_destroy(&poller->event);
	/* A kqueue is not inherited: the child has no descriptor to close, and
	 * the number may be one of its own by now. */
}

void ior_threads_poller_destroy(ior_threads_poller *poller)
{
	if (!poller) {
		return;
	}

	atomic_store_explicit(&poller->shutdown, 1, memory_order_release);
	ior_threads_event_signal(&poller->event);
	pthread_join(poller->thread, NULL);

	pthread_mutex_destroy(&poller->lock);
	ior_threads_event_destroy(&poller->event);
	close(poller->kq);
	free(poller->by_fd);
	free(poller->heap);
	free(poller->map);
	free(poller);
}

#endif /* IOR_HAVE_THREADS */
