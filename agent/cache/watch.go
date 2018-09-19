package cache

import (
	"context"
	"fmt"
	"time"
)

// WatchUpdate is a struct summarising an update to a cache entry
type WatchUpdate struct {
	Result interface{}
	Meta   ResultMeta
	Err    error
}

// indexManagedRequest is a wrapper around a user-passed Request that allows us
// to manage the blocking index without knowing how to modify the concrete
// Request type.
type indexManagedRequest struct {
	r        Request
	minIndex uint64
}

// CacheInfo implements Request
func (r *indexManagedRequest) CacheInfo() RequestInfo {
	info := r.r.CacheInfo()
	info.MinIndex = r.minIndex
	info.Timeout = 10 * time.Minute
	return info
}

// Watch behaves like Get but return a chan to be watched for updates
// asynchronously. It is a helper that abstracts code from perfroming their own
// "blocking" query logic against a cache key to watch for changes and to
// maintain the key in cache actively. It will continue to perform blocking Get
// requests until the context is canceled.
func (c *Cache) Watch(ctx context.Context, t string, r Request) (<-chan WatchUpdate, error) {
	ch := make(chan WatchUpdate, 1)

	// Get the type that we're fetching
	c.typesLock.RLock()
	tEntry, ok := c.types[t]
	c.typesLock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown type in cache: %s", t)
	}
	if !tEntry.Type.SupportsBlocking() {
		return nil, fmt.Errorf("watch requires the type to support blocking")
	}

	// Always start at 0 index to deliver the inital (possibly currently cached
	// value).
	wrappedR := &indexManagedRequest{r, 0}

	go func() {
		defer close(ch)

		var failures uint

		for {
			// Check context hasn't been cancelled
			if ctx.Err() != nil {
				return
			}

			// Blocking request
			res, meta, err := c.Get(t, wrappedR)

			// Check context hasn't been cancelled
			if ctx.Err() != nil {
				return
			}

			// Check the index of the value returned in the cache entry to be sure it
			// changed
			if wrappedR.minIndex < meta.Index {
				u := WatchUpdate{res, meta, err}
				ch <- u

				// Update index for next request
				wrappedR.minIndex = meta.Index
			}

			// Handle errors with backoff. Badly behaved blocking calls that returned
			// a zero index are considered as failures since we need to not get stuck
			// in a busy loop.
			if err == nil && meta.Index > 0 {
				failures = 0
			} else {
				failures++
			}
			if wait := backOffWait(failures); wait > 0 {
				time.Sleep(wait)
			}
			// Sanity check we always request blocking on second pass
			if wrappedR.minIndex < 1 {
				wrappedR.minIndex = 1
			}
		}
	}()

	return ch, nil
}
