/* SPDX-License-Identifier: BSD-3-Clause */
#include "config.h"

#ifdef IOR_HAVE_THREADS

#include "ior.h"
#include "ior_threads_poller.h"
#include "ior_threads_event.h"
#include "ior_worker_pool.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
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
 * epoll allows one registration per fd, so one-shot requests are grouped in
 * per-fd nodes registered level-triggered with the union of their masks,
 * found through a table indexed by the descriptor. The trigger mode is per
 * registration, so a multishot request, which needs EPOLLET, watches a dup(2)
 * of its descriptor in a node of its own (`multi`, with `fd` the dup), which
 * the table does not name. A node's requests are a plain list: there are few
 * per descriptor.
 */
struct ior_poller_fd_node {
	int fd;
	int multi;
	int dirty; /* on the dirty list: a request in it was cancelled or ended */
	ior_poller_req *reqs;
	ior_poller_fd_node *prev; /* every node, for shutdown and fork */
	ior_poller_fd_node *next;
	ior_poller_fd_node *dirty_prev;
	ior_poller_fd_node *dirty_next;
};

/*
 * `lock` guards the incoming queue, the nodes and everything that finds them
 * so that cancel() can find a request from any thread. The poller thread
 * holds it for all this work and drops it only around epoll_wait() and around
 * completion callbacks, which run with no poller lock held (owners take their
 * own locks in them).
 *
 * Nothing a pass does is linear in the number of registered descriptors: a
 * one-shot request finds its node through `by_fd`, a cancel its request
 * through `map` (keyed by the owner's request), a pass sweeps only the nodes
 * on the dirty list, and deadlines are a min-heap. A thousand idle
 * connections cost nothing while they stay idle.
 */
struct ior_threads_poller {
	pthread_t thread;
	int epfd;
	ior_threads_event event; /* wakeup for add()/cancel()/destroy() */
	pthread_mutex_t lock;
	ior_poller_req *incoming_head;
	ior_poller_req *incoming_tail;
	_Atomic int shutdown;
	void *owner;
	ior_threads_poller_cb cb;
	ior_poller_fd_node *fds;
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

static uint32_t ior_poller_to_epoll(uint32_t ior_mask)
{
	uint32_t ev = 0;
	if (ior_mask & IOR_POLL_IN) {
		ev |= EPOLLIN;
	}
	if (ior_mask & IOR_POLL_OUT) {
		ev |= EPOLLOUT;
	}
	/* ERR/HUP are always reported by epoll; nothing to request. */
	return ev;
}

static uint32_t ior_poller_from_epoll(uint32_t ep_events)
{
	uint32_t mask = 0;
	if (ep_events & EPOLLIN) {
		mask |= IOR_POLL_IN;
	}
	if (ep_events & EPOLLOUT) {
		mask |= IOR_POLL_OUT;
	}
	if (ep_events & EPOLLERR) {
		mask |= IOR_POLL_ERR;
	}
	if (ep_events & EPOLLHUP) {
		mask |= IOR_POLL_HUP;
	}
	return mask;
}

/* Union of all requested events for the fd, as an epoll mask. */
static uint32_t ior_poller_node_events(const ior_poller_fd_node *node)
{
	uint32_t mask = 0;
	for (const ior_poller_req *r = node->reqs; r; r = r->next) {
		mask |= r->mask;
	}
	return ior_poller_to_epoll(mask) | (node->multi ? EPOLLET : 0);
}

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

// Drop a node with no requests left.
static void ior_poller_node_free(ior_threads_poller *poller, ior_poller_fd_node *node)
{
	epoll_ctl(poller->epfd, EPOLL_CTL_DEL, node->fd, NULL);
	if (node->multi) {
		close(node->fd);
	} else if ((size_t) node->fd < poller->by_fd_cap && poller->by_fd[node->fd] == node) {
		poller->by_fd[node->fd] = NULL;
	}
	if (node->prev) {
		node->prev->next = node->next;
	} else {
		poller->fds = node->next;
	}
	if (node->next) {
		node->next->prev = node->prev;
	}
	ior_poller_dirty_remove(poller, node);
	free(node);
}

/* Re-register the node with the union of the remaining masks, or drop it. */
static void ior_poller_node_update(ior_threads_poller *poller, ior_poller_fd_node *node)
{
	if (!node->reqs) {
		ior_poller_node_free(poller, node);
		return;
	}
	struct epoll_event ev = {
		.events = ior_poller_node_events(node),
		.data.ptr = node,
	};
	epoll_ctl(poller->epfd, EPOLL_CTL_MOD, node->fd, &ev);
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
 * only after the fd's epoll registration has been updated: once the callback
 * fires, the owner may close the fd, so the poller must no longer reference
 * it.
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

static void ior_poller_ingest_one(
		ior_threads_poller *poller, ior_poller_req *r, ior_poller_req **done)
{
	if (r->cancelled) {
		ior_poller_retire(poller, r, -ECANCELED, done);
		return;
	}
	if ((r->deadline_ns && ior_poller_heap_reserve(poller) < 0)
			|| (!r->multi && ior_poller_by_fd_reserve(poller, r->fd) < 0)) {
		ior_poller_retire(poller, r, -ENOMEM, done);
		return;
	}

	ior_poller_fd_node *node = r->multi ? NULL : poller->by_fd[r->fd];
	if (node) {
		r->next = node->reqs;
		node->reqs = r;
		r->node = node;
		if (r->deadline_ns) {
			ior_poller_heap_push(poller, r);
		}
		ior_poller_node_update(poller, node);
		return;
	}

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
	node->multi = r->multi;
	node->reqs = r;
	r->next = NULL;

	struct epoll_event ev = {
		.events = ior_poller_node_events(node),
		.data.ptr = node,
	};
	if (epoll_ctl(poller->epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
		int err = errno;
		if (r->multi) {
			close(fd);
		}
		free(node);
		if (err == EPERM) {
			/* Regular file: always ready, matching poll()/io_uring. Nothing
			 * to watch for edges either, so a multishot ends here. */
			uint32_t ready = r->mask & (IOR_POLL_IN | IOR_POLL_OUT);
			ior_poller_retire(poller, r, ready ? (int) ready : -EINVAL, done);
		} else {
			ior_poller_retire(poller, r, -err, done);
		}
		return;
	}
	node->next = poller->fds;
	if (poller->fds) {
		poller->fds->prev = node;
	}
	poller->fds = node;
	if (!node->multi) {
		poller->by_fd[fd] = node;
	}
	r->node = node;
	if (r->deadline_ns) {
		ior_poller_heap_push(poller, r);
	}
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
			ior_poller_node_update(poller, node);
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
		ior_poller_node_update(poller, node);
	}
}

/* Nearest deadline as an epoll timeout in ms (-1 = none). Lock held. */
static int ior_poller_timeout_ms(ior_threads_poller *poller)
{
	if (!poller->heap_len) {
		return -1;
	}
	uint64_t nearest = poller->heap[0]->deadline_ns;
	uint64_t now = ior_worker_pool_monotonic_ns();
	if (nearest <= now) {
		return 0;
	}
	uint64_t ms = (nearest - now + 999999ULL) / 1000000ULL;
	return ms > (uint64_t) INT_MAX ? INT_MAX : (int) ms;
}

/*
 * Lock held. A request cancelled while epoll_wait() ran still reports
 * -ECANCELED: cancel() has already promised that result. A multishot request
 * reports the edge and stays registered; a cancel that lands meanwhile is
 * delivered by the next sweep, after the edge.
 */
static void ior_poller_dispatch(ior_threads_poller *poller, ior_poller_fd_node *node,
		uint32_t ep_events, ior_poller_req **done)
{
	uint32_t ready = ior_poller_from_epoll(ep_events);
	int changed = 0;
	ior_poller_req **pp = &node->reqs;
	while (*pp) {
		ior_poller_req *r = *pp;
		uint32_t res = ready & (r->mask | IOR_POLL_ERR | IOR_POLL_HUP);
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
		ior_poller_node_update(poller, node);
	}
}

/* Lock held. */
static void ior_poller_cancel_all(ior_threads_poller *poller, ior_poller_req **done)
{
	while (poller->fds) {
		ior_poller_fd_node *node = poller->fds;
		while (node->reqs) {
			ior_poller_req *r = node->reqs;
			node->reqs = r->next;
			ior_poller_retire(poller, r, -ECANCELED, done);
		}
		ior_poller_node_free(poller, node);
	}
}

static void *ior_poller_thread(void *arg)
{
	ior_threads_poller *poller = arg;
	struct epoll_event events[IOR_POLLER_MAX_EVENTS];

	for (;;) {
		ior_poller_req *done = NULL;

		pthread_mutex_lock(&poller->lock);
		ior_poller_ingest_incoming(poller, &done);
		ior_poller_sweep(poller, &done);
		int shutdown = atomic_load_explicit(&poller->shutdown, memory_order_acquire);
		int timeout_ms = shutdown ? 0 : ior_poller_timeout_ms(poller);
		pthread_mutex_unlock(&poller->lock);

		ior_poller_complete_list(poller, done);
		if (shutdown) {
			break;
		}

		int n = epoll_wait(poller->epfd, events, IOR_POLLER_MAX_EVENTS, timeout_ms);
		if (n < 0 && errno != EINTR) {
			break;
		}

		done = NULL;
		pthread_mutex_lock(&poller->lock);
		for (int i = 0; i < n; i++) {
			if (!events[i].data.ptr) {
				ior_threads_event_clear(&poller->event);
				continue;
			}
			ior_poller_dispatch(poller, events[i].data.ptr, events[i].events, &done);
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
	poller->epfd = epoll_create1(EPOLL_CLOEXEC);
	if (poller->epfd < 0) {
		int err = errno;
		free(poller->map);
		free(poller);
		return -err;
	}
	if (ior_threads_event_init(&poller->event) < 0) {
		close(poller->epfd);
		free(poller->map);
		free(poller);
		return -ENOMEM;
	}
	if (pthread_mutex_init(&poller->lock, NULL) != 0) {
		ior_threads_event_destroy(&poller->event);
		close(poller->epfd);
		free(poller->map);
		free(poller);
		return -ENOMEM;
	}

	/* Wakeup fd is marked by a NULL data pointer. */
	struct epoll_event ev = { .events = EPOLLIN, .data.ptr = NULL };
	if (epoll_ctl(poller->epfd, EPOLL_CTL_ADD, ior_threads_event_get_fd(&poller->event), &ev) < 0
			|| ior_thread_create(&poller->thread, NULL, ior_poller_thread, poller) != 0) {
		pthread_mutex_destroy(&poller->lock);
		ior_threads_event_destroy(&poller->event);
		close(poller->epfd);
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
	close(poller->epfd);
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
	close(poller->epfd);
	free(poller->by_fd);
	free(poller->heap);
	free(poller->map);
	free(poller);
}

#endif /* IOR_HAVE_THREADS */
