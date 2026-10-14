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

type recordingSender struct {
	calls     []*jobrpcv1.ZernioCall
	transfers []*jobrpcv1.MediaTransfer
	keys      []string
}

func (r *recordingSender) Do(_ context.Context, key string, c *jobrpcv1.ZernioCall) *jobrpcv1.ZernioResult {
	r.keys = append(r.keys, key)
	r.calls = append(r.calls, c)
	return &jobrpcv1.ZernioResult{Status: 200, Body: []byte(`{"ok":true}`)}
}

func (r *recordingSender) Transfer(_ context.Context, t *jobrpcv1.MediaTransfer) *jobrpcv1.ZernioResult {
	r.transfers = append(r.transfers, t)
	return &jobrpcv1.ZernioResult{Status: 200}
}

func call(path string) *jobrpcv1.StepResponse {
	return &jobrpcv1.StepResponse{State: []byte(path), Next: &jobrpcv1.StepResponse_Call{Call: &jobrpcv1.ZernioCall{Method: "GET", Path: path}}}
}

func doneWith(outcome string) *jobrpcv1.StepResponse {
	return &jobrpcv1.StepResponse{Next: &jobrpcv1.StepResponse_Done{Done: &jobrpcv1.ZernioStepDone{Outcome: outcome}}}
}

func bootstrapJob() *river.Job[BootstrapZernioProfileArgs] {
	return &river.Job[BootstrapZernioProfileArgs]{JobRow: &rivertype.JobRow{Kind: "bootstrap_zernio_profile", Attempt: 1, MaxAttempts: 5}, Args: BootstrapZernioProfileArgs{TenantID: "t1"}}
}

func TestZernioWorkerRunsTheLoop(t *testing.T) {
	steps := &scriptedSteps{script: []*jobrpcv1.StepResponse{
		call("/profiles"),
		{State: []byte("t"), Next: &jobrpcv1.StepResponse_Transfer{Transfer: &jobrpcv1.MediaTransfer{SourceUrl: "s", UploadUrl: "u"}}},
		doneWith("bootstrapped"),
	}}
	sender := &recordingSender{}
	w := newZernioWorker[BootstrapZernioProfileArgs](zernioDeps{ogen: steps, keys: fakeKeys{key: "zk"}, sender: sender, children: &childKinds{}})
	if err := w.Work(t.Context(), bootstrapJob()); err != nil {
		t.Fatal(err)
	}
	if len(sender.calls) != 1 || sender.keys[0] != "zk" || len(sender.transfers) != 1 {
		t.Fatalf("calls %v transfers %v", sender.calls, sender.transfers)
	}
	if len(steps.requests) != 3 {
		t.Fatalf("steps = %d, want 3", len(steps.requests))
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

func TestZernioWorkerOutcomes(t *testing.T) {
	tests := []struct {
		name       string
		steps      *scriptedSteps
		keys       fakeKeys
		wantSnooze bool
		wantErr    bool
	}{
		{name: "no zernio key", steps: &scriptedSteps{}, keys: fakeKeys{}},
		{name: "ogen busy", steps: &scriptedSteps{err: status.Error(codes.ResourceExhausted, "x")}, keys: fakeKeys{key: "zk"}, wantSnooze: true},
		{name: "ogen without the kind yet", steps: &scriptedSteps{err: status.Error(codes.Unimplemented, "x")}, keys: fakeKeys{key: "zk"}, wantSnooze: true},
		{name: "retry later", steps: &scriptedSteps{script: []*jobrpcv1.StepResponse{{Next: &jobrpcv1.StepResponse_Retry{Retry: &jobrpcv1.ZernioStepRetry{Reason: "zernio 503"}}}}}, keys: fakeKeys{key: "zk"}, wantErr: true},
		{name: "retry after", steps: &scriptedSteps{script: []*jobrpcv1.StepResponse{{Next: &jobrpcv1.StepResponse_Retry{Retry: &jobrpcv1.ZernioStepRetry{After: durationpb.New(time.Minute)}}}}}, keys: fakeKeys{key: "zk"}, wantSnooze: true},
		{name: "endless loop", steps: &scriptedSteps{byKind: func(*jobrpcv1.StepRequest) *jobrpcv1.StepResponse { return call("/again") }}, keys: fakeKeys{key: "zk"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newZernioWorker[BootstrapZernioProfileArgs](zernioDeps{ogen: tt.steps, keys: tt.keys, sender: &recordingSender{}, children: &childKinds{}})
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
			return &jobrpcv1.StepResponse{Next: &jobrpcv1.StepResponse_Done{Done: &jobrpcv1.ZernioStepDone{Enqueue: []*jobrpcv1.ChildJob{
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
	children := &childKinds{build: map[string]func([]byte) (river.JobArgs, error){}}
	registerZernioLifecycle(reg, cfg, steps, &fakeMaintenance{}, fakeKeys{key: "zk"}, &recordingSender{}, children)
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
