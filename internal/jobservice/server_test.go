package jobservice

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

type sendEmailArgs struct {
	UserID string `json:"user_id"`
}

func (sendEmailArgs) Kind() string { return "send_email" }

// newServer migrates a fresh schema in TEST_DATABASE_DSN and returns a
// JobService that knows send_email. Skips without a database.
func newServer(t *testing.T) (*Server, *pgxpool.Pool) {
	t.Helper()
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

	schema := fmt.Sprintf("rs_test_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})
	if err := queue.Migrate(ctx, pool, schema); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// Migrating again is a no-op.
	if err := queue.Migrate(ctx, pool, schema); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}

	reg := queue.NewRegistry()
	queue.Register(reg, river.WorkFunc(func(context.Context, *river.Job[sendEmailArgs]) error { return nil }),
		func(m *jobsv1.SendEmailArgs) sendEmailArgs { return sendEmailArgs{UserID: m.GetUserId()} })
	cfg := &config.Config{RiverSchema: schema, ShutdownTimeout: time.Minute, RescueStuckJobsAfter: time.Hour}
	client, err := queue.NewClient(pool, cfg, reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	return New(pool, client, reg, schema), pool
}

func sendEmail(key string) *jobsv1.EnqueueRequest {
	return &jobsv1.EnqueueRequest{
		Args:           &jobsv1.EnqueueRequest_SendEmail{SendEmail: &jobsv1.SendEmailArgs{UserId: "u1"}},
		IdempotencyKey: key,
		RequestId:      "req-1",
	}
}

func TestEnqueueDeduplicatesDeliveries(t *testing.T) {
	s, _ := newServer(t)
	ctx := t.Context()

	first, err := s.Enqueue(ctx, sendEmail("outbox-1"))
	if err != nil || first.GetDuplicate() || first.GetJobId() == 0 {
		t.Fatalf("first Enqueue = %v, %v", first, err)
	}
	again, err := s.Enqueue(ctx, sendEmail("outbox-1"))
	if err != nil || !again.GetDuplicate() || again.GetJobId() != first.GetJobId() {
		t.Fatalf("retried Enqueue = %v, %v; want duplicate of job %d", again, err, first.GetJobId())
	}
	other, err := s.Enqueue(ctx, sendEmail("outbox-2"))
	if err != nil || other.GetDuplicate() || other.GetJobId() == first.GetJobId() {
		t.Fatalf("new key Enqueue = %v, %v; want a new job", other, err)
	}

	got, err := s.GetJob(ctx, &jobsv1.GetJobRequest{JobId: first.GetJobId()})
	if err != nil {
		t.Fatal(err)
	}
	if j := got.GetJob(); j.GetKind() != "send_email" || j.GetState() != jobsv1.JobState_JOB_STATE_AVAILABLE {
		t.Errorf("GetJob = %v, want an available send_email", j)
	}
}

func TestEnqueueConcurrentDeliveriesMakeOneJob(t *testing.T) {
	s, _ := newServer(t)
	ctx := t.Context()
	const n = 8
	ids := make([]int64, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			res, err := s.Enqueue(ctx, sendEmail("outbox-race"))
			if err != nil {
				t.Errorf("Enqueue: %v", err)
				return
			}
			ids[i] = res.GetJobId()
		})
	}
	wg.Wait()
	for _, id := range ids[1:] {
		if id != ids[0] {
			t.Fatalf("job ids = %v, want one job", ids)
		}
	}
}

func TestEnqueueRejects(t *testing.T) {
	s, _ := newServer(t)
	ctx := t.Context()
	tests := []struct {
		name string
		req  *jobsv1.EnqueueRequest
		want codes.Code
	}{
		{name: "no idempotency key", req: sendEmail(""), want: codes.InvalidArgument},
		{name: "kind newer than this build", req: &jobsv1.EnqueueRequest{IdempotencyKey: "k1"}, want: codes.Unimplemented},
		{name: "kind without a worker", req: &jobsv1.EnqueueRequest{
			Args:           &jobsv1.EnqueueRequest_ProcessPdf{ProcessPdf: &jobsv1.ProcessPdfArgs{AssetId: "a1"}},
			IdempotencyKey: "k2",
		}, want: codes.Unimplemented},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := s.Enqueue(ctx, tt.req); status.Code(err) != tt.want {
				t.Errorf("Enqueue() = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestEnqueueScheduledAndCancel(t *testing.T) {
	s, _ := newServer(t)
	ctx := t.Context()
	req := sendEmail("outbox-later")
	req.ScheduledAt = timestamppb.New(time.Now().Add(48 * time.Hour))
	res, err := s.Enqueue(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJob(ctx, &jobsv1.GetJobRequest{JobId: res.GetJobId()})
	if err != nil || got.GetJob().GetState() != jobsv1.JobState_JOB_STATE_SCHEDULED {
		t.Fatalf("GetJob = %v, %v; want scheduled", got, err)
	}

	cancelled, err := s.CancelJob(ctx, &jobsv1.CancelJobRequest{JobId: res.GetJobId()})
	if err != nil || cancelled.GetJob().GetState() != jobsv1.JobState_JOB_STATE_CANCELLED {
		t.Fatalf("CancelJob = %v, %v; want cancelled", cancelled, err)
	}

	if _, err := s.GetJob(ctx, &jobsv1.GetJobRequest{JobId: 999999999}); status.Code(err) != codes.NotFound {
		t.Errorf("GetJob(missing) = %v, want NotFound", err)
	}
}
