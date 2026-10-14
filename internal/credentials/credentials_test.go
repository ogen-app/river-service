package credentials

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
)

type fakeLeaser struct {
	calls int
	value string
	ttl   time.Duration
	err   error
	now   func() time.Time
}

func (f *fakeLeaser) Lease(context.Context, *jobrpcv1.LeaseRequest, ...grpc.CallOption) (*jobrpcv1.LeaseResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &jobrpcv1.LeaseResponse{Value: f.value, ExpiresAt: timestamppb.New(f.now().Add(f.ttl))}, nil
}

func TestCacheLeasesOncePerLease(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	leaser := &fakeLeaser{value: "re_1", ttl: 5 * time.Minute, now: clock}
	c := NewCache(leaser)
	c.now = clock

	for range 3 {
		if v, err := c.Get(t.Context(), Resend); err != nil || v != "re_1" {
			t.Fatalf("Get = %q, %v", v, err)
		}
	}
	if leaser.calls != 1 {
		t.Fatalf("leases = %d, want 1 within the lease", leaser.calls)
	}

	// Within the renewal margin, the next Get leases again and picks up a
	// rotated key.
	now = now.Add(5*time.Minute - 10*time.Second)
	leaser.value = "re_2"
	if v, _ := c.Get(t.Context(), Resend); v != "re_2" || leaser.calls != 2 {
		t.Errorf("after renewal: %q with %d leases, want re_2 with 2", v, leaser.calls)
	}
}

func TestCacheUnsetAndUnreachable(t *testing.T) {
	c := NewCache(&fakeLeaser{err: status.Error(codes.NotFound, "not set")})
	if v, err := c.Get(t.Context(), Resend); v != "" || err != nil {
		t.Errorf("unset key = %q, %v; want empty and no error", v, err)
	}
	c = NewCache(&fakeLeaser{err: status.Error(codes.Unavailable, "down")})
	if _, err := c.Get(t.Context(), Resend); status.Code(err) != codes.Unavailable {
		t.Errorf("unreachable ogen = %v, want Unavailable", err)
	}
}
