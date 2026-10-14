package jobs

import (
	"context"
	"encoding/json"
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
	"google.golang.org/protobuf/types/known/durationpb"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

// scriptedSteps answers Step from a script and records each request.
type scriptedSteps struct {
	mu       sync.Mutex
	script   []*jobrpcv1.StepResponse
	err      error
	requests []*jobrpcv1.StepRequest
	byKind   func(*jobrpcv1.StepRequest) *jobrpcv1.StepResponse
}

func (s *scriptedSteps) Step(_ context.Context, in *jobrpcv1.StepRequest, _ ...grpc.CallOption) (*jobrpcv1.StepResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, in)
	if s.err != nil {
		return nil, s.err
	}
	if s.byKind != nil {
		return s.byKind(in), nil
	}
	resp := s.script[0]
	s.script = s.script[1:]
	return resp, nil
}

type recordingActions struct {
	calls     []*jobrpcv1.HttpCall
	transfers []*jobrpcv1.MediaTransfer
	sidecars  []*jobrpcv1.SidecarCall
}

func (r *recordingActions) HTTP(_ context.Context, c *jobrpcv1.HttpCall) *jobrpcv1.StepResult {
	r.calls = append(r.calls, c)
	return &jobrpcv1.StepResult{Status: 200, Body: []byte(`{"ok":true}`)}
}

func (r *recordingActions) Transfer(_ context.Context, t *jobrpcv1.MediaTransfer) *jobrpcv1.StepResult {
	r.transfers = append(r.transfers, t)
	return &jobrpcv1.StepResult{Status: 200}
}

func (r *recordingActions) Sidecar(_ context.Context, c *jobrpcv1.SidecarCall) *jobrpcv1.StepResult {
	r.sidecars = append(r.sidecars, c)
	return &jobrpcv1.StepResult{Status: 0, Body: []byte{1}}
}

func call(path string) *jobrpcv1.StepResponse {
	return &jobrpcv1.StepResponse{State: []byte(path), Next: &jobrpcv1.StepResponse_Http{Http: &jobrpcv1.HttpCall{Upstream: "zernio", Method: "GET", Path: path}}}
}

func doneWith(outcome string) *jobrpcv1.StepResponse {
	return &jobrpcv1.StepResponse{Next: &jobrpcv1.StepResponse_Done{Done: &jobrpcv1.StepDone{Outcome: outcome}}}
}

func bootstrapJob() *river.Job[BootstrapZernioProfileArgs] {
	return &river.Job[BootstrapZernioProfileArgs]{JobRow: &rivertype.JobRow{Kind: "bootstrap_zernio_profile", Attempt: 1, MaxAttempts: 5}, Args: BootstrapZernioProfileArgs{TenantID: "t1"}}
}

func TestStepWorkerRunsTheLoop(t *testing.T) {
	steps := &scriptedSteps{script: []*jobrpcv1.StepResponse{
		call("/profiles"),
		{State: []byte("t"), Next: &jobrpcv1.StepResponse_Transfer{Transfer: &jobrpcv1.MediaTransfer{SourceUrl: "s", UploadUrl: "u"}}},
		{State: []byte("s"), Next: &jobrpcv1.StepResponse_Sidecar{Sidecar: &jobrpcv1.SidecarCall{Service: "pdf", Method: "/pdf.v1.PdfService/Parse"}}},
		doneWith("bootstrapped"),
	}}
	acts := &recordingActions{}
	w := newStepWorker[BootstrapZernioProfileArgs](stepDeps{ogen: steps, actions: acts, children: newChildKinds()}, 0)
	if err := w.Work(t.Context(), bootstrapJob()); err != nil {
		t.Fatal(err)
	}
	if len(acts.calls) != 1 || len(acts.transfers) != 1 || len(acts.sidecars) != 1 {
		t.Fatalf("calls %v transfers %v sidecars %v", acts.calls, acts.transfers, acts.sidecars)
	}
	if len(steps.requests) != 4 {
		t.Fatalf("steps = %d, want 4", len(steps.requests))
	}
	first, second := steps.requests[0], steps.requests[1]
	var args BootstrapZernioProfileArgs
	if err := json.Unmarshal(first.GetArgs(), &args); err != nil || args.TenantID != "t1" || first.GetKind() != "bootstrap_zernio_profile" || first.GetResult() != nil {
		t.Errorf("first step = %v", first)
	}
	if string(second.GetState()) != "/profiles" || second.GetResult().GetStatus() != 200 {
		t.Errorf("second step carried state %q and result %v", second.GetState(), second.GetResult())
	}
}

func TestStepWorkerOutcomes(t *testing.T) {
	tests := []struct {
		name       string
		steps      *scriptedSteps
		wantSnooze bool
		wantErr    bool
	}{
		{name: "done", steps: &scriptedSteps{script: []*jobrpcv1.StepResponse{doneWith("ok")}}},
		{name: "ogen busy", steps: &scriptedSteps{err: status.Error(codes.ResourceExhausted, "x")}, wantSnooze: true},
		{name: "ogen without the kind yet", steps: &scriptedSteps{err: status.Error(codes.Unimplemented, "x")}, wantSnooze: true},
		{name: "retry later", steps: &scriptedSteps{script: []*jobrpcv1.StepResponse{{Next: &jobrpcv1.StepResponse_Retry{Retry: &jobrpcv1.StepRetry{Reason: "zernio 503"}}}}}, wantErr: true},
		{name: "retry after", steps: &scriptedSteps{script: []*jobrpcv1.StepResponse{{Next: &jobrpcv1.StepResponse_Retry{Retry: &jobrpcv1.StepRetry{After: durationpb.New(time.Minute)}}}}}, wantSnooze: true},
		{name: "endless loop", steps: &scriptedSteps{byKind: func(*jobrpcv1.StepRequest) *jobrpcv1.StepResponse { return call("/again") }}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newStepWorker[BootstrapZernioProfileArgs](stepDeps{ogen: tt.steps, actions: &recordingActions{}, children: newChildKinds()}, 0)
			err := w.Work(t.Context(), bootstrapJob())
			_, snoozed := errors.AsType[*rivertype.JobSnoozeError](err)
			if snoozed != tt.wantSnooze || (!tt.wantSnooze && (err != nil) != tt.wantErr) {
				t.Fatalf("Work() = %v, want snooze %v error %v", err, tt.wantSnooze, tt.wantErr)
			}
		})
	}
}

func TestChildKinds(t *testing.T) {
	c := &childKinds{build: map[string]func([]byte) (river.JobArgs, error){"detect_expiring_connections_tenant": childOf[ExpiringConnectionsTenantArgs]()}}
	args, err := c.args("detect_expiring_connections_tenant", []byte(`{"tenant_id":"t1","profile_id":"p1"}`))
	if err != nil || args.(ExpiringConnectionsTenantArgs).ProfileID != "p1" {
		t.Fatalf("args = %v, %v", args, err)
	}
	if _, err := c.args("unknown_child", nil); err == nil {
		t.Error("unknown child kind built")
	}
}

// TestSweepFansOutIntoTenantJobs runs the connection-health sweep on a real
// River client: ogen's (scripted) step fans it out, and River then runs one
// tenant job per child.
func TestSweepFansOutIntoTenantJobs(t *testing.T) {
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
	schema := fmt.Sprintf("rs_zernio_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})
	if err := queue.Migrate(ctx, pool, schema); err != nil {
		t.Fatal(err)
	}

	steps := &scriptedSteps{byKind: func(in *jobrpcv1.StepRequest) *jobrpcv1.StepResponse {
		if in.GetKind() == "detect_expiring_connections" {
			return &jobrpcv1.StepResponse{Next: &jobrpcv1.StepResponse_Done{Done: &jobrpcv1.StepDone{Enqueue: []*jobrpcv1.ChildJob{
				{Kind: "detect_expiring_connections_tenant", Args: []byte(`{"tenant_id":"t1","profile_id":"p1"}`)},
				{Kind: "detect_expiring_connections_tenant", Args: []byte(`{"tenant_id":"t2","profile_id":"p2"}`)},
			}}}}
		}
		return doneWith("checked")
	}}
	cfg := &config.Config{
		RiverSchema: schema, RiverQueues: []string{config.QueueDefault}, DefaultWorkers: 2,
		ShutdownTimeout: time.Minute, RescueStuckJobsAfter: time.Hour, ZernioHealthCheckInterval: time.Hour,
	}
	reg := queue.NewRegistry()
	registerZernioLifecycle(reg, cfg, stepDeps{ogen: steps, actions: &recordingActions{}, children: newChildKinds()}, &fakeMaintenance{})
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

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var tenants []string
		steps.mu.Lock()
		for _, r := range steps.requests {
			if r.GetKind() == "detect_expiring_connections_tenant" {
				var a ExpiringConnectionsTenantArgs
				_ = json.Unmarshal(r.GetArgs(), &a)
				tenants = append(tenants, a.TenantID)
			}
		}
		steps.mu.Unlock()
		slices.Sort(tenants)
		if slices.Equal(tenants, []string{"t1", "t2"}) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the sweep's tenant jobs did not run")
}
