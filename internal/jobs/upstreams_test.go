package jobs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/credentials"
)

// leaseRecorder hands out one key per lease name and records the names asked
// for.
type leaseRecorder struct {
	mu    sync.Mutex
	keys  map[string]string
	asked []string
}

func (l *leaseRecorder) Get(_ context.Context, name string) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asked = append(l.asked, name)
	return l.keys[name], nil
}

// TestPolarUpstreamsAreWired sends a planned Polar call through the executor
// newActions builds: each upstream reaches its own API with the leased
// polar_access_token and the API version ogen pinned.
func TestPolarUpstreamsAreWired(t *testing.T) {
	type seen struct{ path, auth, version string }
	// mu guards both servers' records: the handlers write them on the
	// servers' goroutines, the test reads and resets them.
	var mu sync.Mutex
	server := func(t *testing.T) (*httptest.Server, *seen) {
		var got seen
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			got = seen{path: r.URL.Path, auth: r.Header.Get("Authorization"), version: r.Header.Get("Polar-Version")}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)
		return srv, &got
	}
	production, prodSeen := server(t)
	sandbox, sandboxSeen := server(t)
	cfg := &config.Config{PolarBaseURL: production.URL + "/v1", PolarSandboxBaseURL: sandbox.URL + "/v1", PolarHTTPTimeout: 5 * time.Second}

	for _, tc := range []struct {
		upstream string
		hit      *seen
		missed   *seen
	}{
		{upstreamPolar, prodSeen, sandboxSeen},
		{upstreamPolarSandbox, sandboxSeen, prodSeen},
	} {
		t.Run(tc.upstream, func(t *testing.T) {
			mu.Lock()
			*prodSeen, *sandboxSeen = seen{}, seen{}
			mu.Unlock()
			keys := &leaseRecorder{keys: map[string]string{credentials.Polar: "polar_oat"}}
			res := newActions(cfg, keys).HTTP(t.Context(), &jobrpcv1.HttpCall{
				Upstream: tc.upstream, Method: http.MethodGet, Path: "/subscriptions/sub_1",
				Headers: map[string]string{"Polar-Version": "2026-10"},
			})
			if res.GetStatus() != http.StatusOK || res.GetError() != "" {
				t.Fatalf("result = %v", res)
			}
			mu.Lock()
			hit, missed := *tc.hit, *tc.missed
			mu.Unlock()
			if hit != (seen{path: "/v1/subscriptions/sub_1", auth: "Bearer polar_oat", version: "2026-10"}) {
				t.Errorf("request = %+v", hit)
			}
			if missed != (seen{}) {
				t.Errorf("the other API was called: %+v", missed)
			}
			if len(keys.asked) != 1 || keys.asked[0] != credentials.Polar {
				t.Errorf("leased %v, want %s", keys.asked, credentials.Polar)
			}
		})
	}
}
