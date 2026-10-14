// Package credentials holds the vendor keys river-service leases from ogen.
// A key is kept in memory only, never logged, and re-leased shortly before
// its lease ends, so a rotation in Harbor reaches jobs within one lease.
package credentials

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
)

// Names of the keys ogen lends.
const (
	Resend    = "resend_api_key"
	Zernio    = "zernio_api_key"
	Gemini    = "gemini_api_key"
	Firecrawl = "firecrawl_api_key"
)

// renewBefore is how long before a lease ends it is renewed.
const renewBefore = 30 * time.Second

// Leaser is the part of ogen's CredentialService the cache calls.
type Leaser interface {
	Lease(ctx context.Context, in *jobrpcv1.LeaseRequest, opts ...grpc.CallOption) (*jobrpcv1.LeaseResponse, error)
}

type entry struct {
	value   string
	renewAt time.Time
}

// Cache serves leased keys, leasing again when a lease is about to end.
type Cache struct {
	leaser Leaser
	now    func() time.Time

	mu      sync.Mutex
	entries map[string]entry
}

// NewCache returns a cache over leaser.
func NewCache(leaser Leaser) *Cache {
	return &Cache{leaser: leaser, now: time.Now, entries: map[string]entry{}}
}

// Get returns the current value of name. An empty value with a nil error
// means the key is not set in ogen, so the feature is off. Any other error is
// ogen being unreachable or busy: the caller retries later.
func (c *Cache) Get(ctx context.Context, name string) (string, error) {
	c.mu.Lock()
	e, ok := c.entries[name]
	c.mu.Unlock()
	if ok && c.now().Before(e.renewAt) {
		return e.value, nil
	}

	res, err := c.leaser.Lease(ctx, &jobrpcv1.LeaseRequest{Name: name})
	if status.Code(err) == codes.NotFound {
		c.forget(name)
		return "", nil
	}
	if err != nil {
		return "", err
	}
	renewAt := res.GetExpiresAt().AsTime().Add(-renewBefore)
	c.mu.Lock()
	c.entries[name] = entry{value: res.GetValue(), renewAt: renewAt}
	c.mu.Unlock()
	return res.GetValue(), nil
}

func (c *Cache) forget(name string) {
	c.mu.Lock()
	delete(c.entries, name)
	c.mu.Unlock()
}
