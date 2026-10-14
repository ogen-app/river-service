// Package jobservice serves jobs.v1.JobService: the intake ogen's outbox relay
// delivers jobs to, plus job lookup and cancel for operator tooling.
package jobservice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/logging"
	"github.com/ogen-app/river-service/internal/queue"
)

// Server implements jobs.v1.JobService.
type Server struct {
	jobsv1.UnimplementedJobServiceServer

	pool      *pgxpool.Pool
	client    *river.Client[pgx.Tx]
	reg       *queue.Registry
	keysTable string
}

// New returns a JobService over the River client. Enqueue de-duplicates in the
// enqueue-keys table of schema.
func New(pool *pgxpool.Pool, client *river.Client[pgx.Tx], reg *queue.Registry, schema string) *Server {
	return &Server{pool: pool, client: client, reg: reg, keysTable: queue.EnqueueKeysTable(schema)}
}

// Enqueue inserts the job and records its idempotency key in one transaction,
// so a retried delivery finds the key and returns the first job. The key is
// written first: a concurrent delivery of the same key waits on its row until
// this transaction ends, then sees the conflict.
func (s *Server) Enqueue(ctx context.Context, req *jobsv1.EnqueueRequest) (*jobsv1.EnqueueResponse, error) {
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	args, kind, ok := s.reg.Args(req)
	switch {
	case kind == "":
		return nil, status.Error(codes.Unimplemented, "no args set; the job kind may be newer than this river-service")
	case !ok:
		return nil, status.Errorf(codes.Unimplemented, "job kind %q is not supported by this river-service", kind)
	}

	ctx = logging.WithRequestID(ctx, req.GetRequestId())
	opts := &river.InsertOpts{Metadata: queue.EncodeMetadata(ctx, req.GetRequestId())}
	if req.GetScheduledAt() != nil {
		opts.ScheduledAt = req.GetScheduledAt().AsTime()
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, internal(ctx, "begin", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	tag, err := tx.Exec(ctx, `INSERT INTO `+s.keysTable+` (idempotency_key, job_id) VALUES ($1, 0) ON CONFLICT DO NOTHING`, req.GetIdempotencyKey())
	if err != nil {
		return nil, internal(ctx, "record idempotency key", err)
	}
	if tag.RowsAffected() == 0 {
		_ = tx.Rollback(ctx)
		var jobID int64
		if err := s.pool.QueryRow(ctx, `SELECT job_id FROM `+s.keysTable+` WHERE idempotency_key = $1`, req.GetIdempotencyKey()).Scan(&jobID); err != nil {
			return nil, internal(ctx, "read idempotency key", err)
		}
		return &jobsv1.EnqueueResponse{JobId: jobID, Duplicate: true}, nil
	}

	res, err := s.client.InsertTx(ctx, tx, args, opts)
	if err != nil {
		return nil, internal(ctx, "insert "+kind, err)
	}
	if _, err := tx.Exec(ctx, `UPDATE `+s.keysTable+` SET job_id = $2 WHERE idempotency_key = $1`, req.GetIdempotencyKey(), res.Job.ID); err != nil {
		return nil, internal(ctx, "record job id", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, internal(ctx, "commit", err)
	}
	slog.InfoContext(ctx, "job enqueued", logging.AttrComponent, "jobservice",
		"job_kind", kind, "job_id", res.Job.ID, "unique_skipped", res.UniqueSkippedAsDuplicate)
	// A River uniqueness fold (an identical job already waiting) creates no new
	// job either.
	return &jobsv1.EnqueueResponse{JobId: res.Job.ID, Duplicate: res.UniqueSkippedAsDuplicate}, nil
}

// GetJob returns one job.
func (s *Server) GetJob(ctx context.Context, req *jobsv1.GetJobRequest) (*jobsv1.GetJobResponse, error) {
	row, err := s.client.JobGet(ctx, req.GetJobId())
	if err != nil {
		return nil, jobError(ctx, "get", err)
	}
	return &jobsv1.GetJobResponse{Job: toJob(row)}, nil
}

// CancelJob cancels a job that has not finished.
func (s *Server) CancelJob(ctx context.Context, req *jobsv1.CancelJobRequest) (*jobsv1.CancelJobResponse, error) {
	row, err := s.client.JobCancel(ctx, req.GetJobId())
	if err != nil {
		return nil, jobError(ctx, "cancel", err)
	}
	return &jobsv1.CancelJobResponse{Job: toJob(row)}, nil
}

func jobError(ctx context.Context, op string, err error) error {
	if errors.Is(err, rivertype.ErrNotFound) {
		return status.Error(codes.NotFound, "job not found")
	}
	return internal(ctx, op+" job", err)
}

func internal(ctx context.Context, op string, err error) error {
	slog.ErrorContext(ctx, "jobservice: "+op+" failed", logging.AttrComponent, "jobservice", logging.AttrError, err)
	return status.Error(codes.Internal, fmt.Sprintf("%s failed", op))
}

var states = map[rivertype.JobState]jobsv1.JobState{
	rivertype.JobStateAvailable: jobsv1.JobState_JOB_STATE_AVAILABLE,
	rivertype.JobStateScheduled: jobsv1.JobState_JOB_STATE_SCHEDULED,
	rivertype.JobStatePending:   jobsv1.JobState_JOB_STATE_PENDING,
	rivertype.JobStateRunning:   jobsv1.JobState_JOB_STATE_RUNNING,
	rivertype.JobStateRetryable: jobsv1.JobState_JOB_STATE_RETRYABLE,
	rivertype.JobStateCompleted: jobsv1.JobState_JOB_STATE_COMPLETED,
	rivertype.JobStateCancelled: jobsv1.JobState_JOB_STATE_CANCELLED,
	rivertype.JobStateDiscarded: jobsv1.JobState_JOB_STATE_DISCARDED,
}

func toJob(row *rivertype.JobRow) *jobsv1.Job {
	j := &jobsv1.Job{
		Id:          row.ID,
		Kind:        row.Kind,
		State:       states[row.State],
		Attempt:     int32(row.Attempt),
		MaxAttempts: int32(row.MaxAttempts),
		CreatedAt:   timestamppb.New(row.CreatedAt),
		ScheduledAt: timestamppb.New(row.ScheduledAt),
	}
	if row.FinalizedAt != nil {
		j.FinalizedAt = timestamppb.New(*row.FinalizedAt)
	}
	if n := len(row.Errors); n > 0 {
		j.LastError = row.Errors[n-1].Error
	}
	return j
}
