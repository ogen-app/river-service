// Package health serves the HTTP checks Railway probes: /healthz answers while
// the process runs, and /readyz answers once the service is serving and its
// database is reachable, and stops answering when shutdown begins.
package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

const pingTimeout = 2 * time.Second

// Pinger checks the database connection.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Checks holds the readiness state.
type Checks struct {
	db    Pinger
	ready atomic.Bool
}

// New returns the checks for db, not ready until SetReady(true).
func New(db Pinger) *Checks { return &Checks{db: db} }

// SetReady marks the service as serving or draining.
func (c *Checks) SetReady(ready bool) { c.ready.Store(ready) }

// Handler serves /healthz and /readyz.
func (c *Checks) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !c.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), pingTimeout)
		defer cancel()
		if err := c.db.Ping(ctx); err != nil {
			http.Error(w, "database unreachable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
