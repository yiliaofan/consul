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

// Watch behaves like Get but return a chan to be watched for updates
// asynchronously. It is a helper that abstracts code from perfroming their own
// "blocking" query logic against a cache key to watch for changes and to
// maintain the key in cache actively. It will continue to perform blocking Get
// requests until the context is canceled.
//
// The returned chan is only minimally buffered so if the caller doesn't consume
// from it quickly enough, the Watch loop will block. When the chan is later
// drained, watching resumes correctly, however if the pause is long enough, it
// will prevent active blocking Get on the cache and may allow the Cache-Type's
// TTL to remove it locally. Even then when the chan is drained again, the new
// Get will re-fetch the entry from servers and resume behaviour transparently.
//
// The passed context must be cancelled or timeout in order to free resources
// and stop maintaining the value in cache. Typically request-scoped resources
// do this but if a long-lived context like context.Background is used, then the
// caller must arrange for it to be cancelled when the watch is no longer
// needed.
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
	index := uint64(0)

	go func() {
		defer close(ch)

		var failures uint

		for {
			// Check context hasn't been cancelled
			if ctx.Err() != nil {
				return
			}

			// Blocking request
			res, meta, err := c.getWithIndex(t, r, index)

			// Check context hasn't been cancelled
			if ctx.Err() != nil {
				return
			}

			// Check the index of the value returned in the cache entry to be sure it
			// changed
			if index < meta.Index {
				u := WatchUpdate{res, meta, err}
				select {
				case ch <- u:
				case <-ctx.Done():
					return
				}

				// Update index for next request
				index = meta.Index
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
			if index < 1 {
				index = 1
			}
		}
	}()

	return ch, nil
}
