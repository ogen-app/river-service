package jobs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

type fakeMaintenance struct {
	mu    sync.Mutex
	err   error
	tasks []string
}

func (f *fakeMaintenance) Run(_ context.Context, in *jobrpcv1.RunRequest, _ ...grpc.CallOption) (*jobrpcv1.RunResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks = append(f.tasks, in.GetTask())
	return &jobrpcv1.RunResponse{}, f.err
}

func (f *fakeMaintenance) ran() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.tasks)
}

func TestSweepWorkerOutcomes(t *testing.T) {
	errBoom := errors.New("boom")
	tests := []struct {
		name       string
		err        error
		wantSnooze bool
		wantErr    error
	}{
		{name: "done"},
		{name: "ogen busy", err: status.Error(codes.ResourceExhausted, "limit"), wantSnooze: true},
		{name: "ogen unreachable", err: status.Error(codes.Unavailable, "down"), wantSnooze: true},
		{name: "ogen without the RPC yet", err: status.Error(codes.Unimplemented, "no"), wantSnooze: true},
		{name: "real failure", err: errBoom, wantErr: errBoom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rpc := &fakeMaintenance{err: tt.err}
			w := &sweepWorker[CleanupPostLogsArgs]{rpc: rpc}
			err := w.Work(t.Context(), &river.Job[CleanupPostLogsArgs]{JobRow: &rivertype.JobRow{Kind: "cleanup_post_logs"}})
			if got := rpc.ran(); len(got) != 1 || got[0] != "cleanup_post_logs" {
				t.Fatalf("ran %v, want cleanup_post_logs", got)
			}
			_, snoozed := errors.AsType[*rivertype.JobSnoozeError](err)
			if snoozed != tt.wantSnooze {
				t.Fatalf("Work() = %v, want snooze %v", err, tt.wantSnooze)
			}
			if !tt.wantSnooze && !errors.Is(err, tt.wantErr) {
				t.Errorf("Work() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRegistryWithoutOgenIsEmpty(t *testing.T) {
	if n := Registry(&config.Config{}, nil).Len(); n != 0 {
		t.Errorf("Registry without ogen has %d kinds, want 0", n)
	}
}

func TestHousekeepingSchedule(t *testing.T) {
	reg := queue.NewRegistry()
	registerHousekeeping(reg, &config.Config{NotificationsCleanupEvery: 24 * time.Hour}, &fakeMaintenance{})
	if reg.Len() != 5 {
		t.Errorf("kinds = %d, want 5", reg.Len())
	}
	// PENDING_UPLOAD_SWEEP_INTERVAL is zero here, so that sweep is off.
	if n := len(reg.PeriodicJobs()); n != 4 {
		t.Errorf("scheduled sweeps = %d, want 4", n)
	}
}

// TestSweepsRunOnStart starts a real River client on a fresh schema and checks
// that every sweep runs once on boot through ogen's MaintenanceService.
func TestSweepsRunOnStart(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_DSN")
	if dsn == "" {
		t.Skip("TEST_DATABASE_DSN not set")
	}
	ctx := t.Context()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema := fmt.Sprintf("rs_sweep_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})
	if err := queue.Migrate(ctx, pool, schema); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		RiverSchema: schema, RiverQueues: []string{config.QueueDefault}, DefaultWorkers: 2,
		ShutdownTimeout: time.Minute, RescueStuckJobsAfter: time.Hour,
		NotificationsCleanupEvery: 24 * time.Hour, PendingUploadSweepInterval: time.Hour,
	}
	rpc := &fakeMaintenance{}
	reg := queue.NewRegistry()
	registerHousekeeping(reg, cfg, rpc)
	client, err := queue.NewClient(pool, cfg, reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Stop(stopCtx)
	})

	want := []string{"cleanup_login_security", "cleanup_notifications", "cleanup_plugin_pairings", "cleanup_post_logs", "sweep_pending_uploads"}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		got := rpc.ran()
		slices.Sort(got)
		if slices.Equal(slices.Compact(got), want) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("sweeps run on start = %v, want %v", rpc.ran(), want)
}
