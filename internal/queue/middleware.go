package queue

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ogen-app/river-service/internal/logging"
	"github.com/ogen-app/river-service/internal/telemetry"
)

// Middleware returns the worker middleware every job runs through, outermost
// first: tracing wraps the drain handling, so a job rescheduled by a shutdown
// shows as a snooze in its span, not a failure.
func Middleware() []rivertype.Middleware {
	return []rivertype.Middleware{&tracingMiddleware{tracer: otel.Tracer("river-service/jobs")}, &drainMiddleware{}}
}

// tracingMiddleware runs each attempt in a span that continues the trace of
// the request that enqueued the job, so one trace id covers the ogen request,
// the job and every call the job makes. The span also links to the enqueuing
// span. A job with no trace context (a periodic sweep) starts its own trace.
// Failures go to Sentry once retries are exhausted.
type tracingMiddleware struct {
	river.MiddlewareDefaults
	tracer trace.Tracer
}

func (m *tracingMiddleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	parent, requestID := restoreMetadata(ctx, job.Metadata)
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("job.kind", job.Kind),
			attribute.String("job.queue", job.Queue),
			attribute.Int("job.attempt", job.Attempt),
			attribute.Int64("job.id", job.ID),
		),
	}
	if sc := trace.SpanContextFromContext(parent); sc.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
	}
	ctx, span := m.tracer.Start(parent, "job "+job.Kind, opts...)
	defer span.End()
	ctx = logging.WithRequestID(ctx, requestID)

	err := doInner(ctx)
	if _, snoozed := errors.AsType[*rivertype.JobSnoozeError](err); snoozed {
		span.SetAttributes(attribute.Bool("job.snoozed", true))
		return err
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if job.Attempt >= job.MaxAttempts {
			telemetry.CaptureError(ctx, err,
				attribute.String("job.kind", job.Kind),
				attribute.Int("job.attempt", job.Attempt))
		}
	}
	return err
}

// drainSnooze is how long a job interrupted by a client stop waits before it
// is worked again. During a deploy the replacement replica is already up, so
// it picks the job up within seconds.
const drainSnooze = 5 * time.Second

// drainMiddleware reschedules a job whose run was cut short by the client
// stopping. A stop that outlasts the soft-stop timeout cancels every running
// job's context; without this, the cancelled run would count as a failed
// attempt, and a job on its last attempt would give up on work that was only
// interrupted. A snooze consumes no attempt, and the job resumes from its last
// checkpoint.
//
// The middleware sits outside River's per-job timeout, so its context ends
// only when the client stops or the job is cancelled remotely. A remote
// JobCancel keeps River's own handling.
type drainMiddleware struct {
	river.MiddlewareDefaults
}

func (*drainMiddleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	err := doInner(ctx)
	if err == nil || ctx.Err() == nil || errors.Is(context.Cause(ctx), river.ErrJobCancelledRemotely) {
		return err
	}
	if _, ok := errors.AsType[*rivertype.JobSnoozeError](err); ok {
		return err
	}
	if _, ok := errors.AsType[*rivertype.JobCancelError](err); ok {
		return err
	}
	slog.InfoContext(ctx, "job interrupted by shutdown; rescheduled",
		logging.AttrComponent, "jobs",
		"job_kind", job.Kind, "job_id", job.ID, "job_attempt", job.Attempt,
		logging.AttrError, err)
	return river.JobSnooze(drainSnooze)
}
