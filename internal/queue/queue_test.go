package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/logging"
)

type sendEmailArgs struct {
	UserID string `json:"user_id"`
}

func (sendEmailArgs) Kind() string { return "send_email" }

type notAKindArgs struct{}

func (notAKindArgs) Kind() string { return "not_a_kind" }

func TestRegistryArgs(t *testing.T) {
	reg := NewRegistry()
	Register(reg, river.WorkFunc(func(context.Context, *river.Job[sendEmailArgs]) error { return nil }),
		func(m *jobsv1.SendEmailArgs) sendEmailArgs { return sendEmailArgs{UserID: m.GetUserId()} })

	args, kind, ok := reg.Args(&jobsv1.EnqueueRequest{Args: &jobsv1.EnqueueRequest_SendEmail{SendEmail: &jobsv1.SendEmailArgs{UserId: "u1"}}})
	if !ok || kind != "send_email" || args.(sendEmailArgs).UserID != "u1" {
		t.Fatalf("Args(send_email) = %v, %q, %v", args, kind, ok)
	}

	_, kind, ok = reg.Args(&jobsv1.EnqueueRequest{Args: &jobsv1.EnqueueRequest_ProcessPdf{ProcessPdf: &jobsv1.ProcessPdfArgs{}}})
	if ok || kind != "process_pdf" {
		t.Errorf("Args(unregistered) = %q, %v; want process_pdf, false", kind, ok)
	}

	_, kind, ok = reg.Args(&jobsv1.EnqueueRequest{})
	if ok || kind != "" {
		t.Errorf("Args(no args) = %q, %v; want empty kind", kind, ok)
	}
}

func TestRegisterRejectsMismatchedKinds(t *testing.T) {
	assertPanics := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: want a panic", name)
			}
		}()
		fn()
	}
	worker := river.WorkFunc(func(context.Context, *river.Job[notAKindArgs]) error { return nil })
	assertPanics("kind outside the args oneof", func() {
		Register(NewRegistry(), worker, func(*jobsv1.SendEmailArgs) notAKindArgs { return notAKindArgs{} })
	})

	reg := NewRegistry()
	email := river.WorkFunc(func(context.Context, *river.Job[sendEmailArgs]) error { return nil })
	conv := func(*jobsv1.SendEmailArgs) sendEmailArgs { return sendEmailArgs{} }
	Register(reg, email, conv)
	assertPanics("duplicate kind", func() { Register(reg, email, conv) })
}

func TestTracingContinuesTheEnqueuingTrace(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	enqueueCtx, enqueueSpan := tp.Tracer("test").Start(context.Background(), "enqueue")
	md := EncodeMetadata(enqueueCtx, "req-1")
	enqueueSpan.End()

	mw := &tracingMiddleware{tracer: tp.Tracer("jobs")}
	job := &rivertype.JobRow{ID: 9, Kind: "send_email", Queue: "default", Attempt: 1, MaxAttempts: 5, Metadata: md}
	var gotRequestID string
	var gotSpan trace.SpanContext
	if err := mw.Work(context.Background(), job, func(ctx context.Context) error {
		gotRequestID = logging.RequestID(ctx)
		gotSpan = trace.SpanContextFromContext(ctx)
		return nil
	}); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if gotSpan.TraceID() != enqueueSpan.SpanContext().TraceID() {
		t.Errorf("job trace id = %s, want the enqueuing trace %s", gotSpan.TraceID(), enqueueSpan.SpanContext().TraceID())
	}
	if gotRequestID != "req-1" {
		t.Errorf("request id = %q, want req-1", gotRequestID)
	}
	var jobSpan sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.Name() == "job send_email" {
			jobSpan = s
		}
	}
	if jobSpan == nil {
		t.Fatal("no job span recorded")
	}
	if jobSpan.Parent().SpanID() != enqueueSpan.SpanContext().SpanID() {
		t.Errorf("job span parent = %s, want the enqueue span", jobSpan.Parent().SpanID())
	}
	if links := jobSpan.Links(); len(links) != 1 || links[0].SpanContext.SpanID() != enqueueSpan.SpanContext().SpanID() {
		t.Errorf("job span links = %v, want one link to the enqueue span", links)
	}
}

func TestTracingStartsATraceWithoutMetadata(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	mw := &tracingMiddleware{tracer: tp.Tracer("jobs")}
	job := &rivertype.JobRow{ID: 1, Kind: "cleanup_post_logs", Attempt: 1, MaxAttempts: 1}
	err := mw.Work(context.Background(), job, func(ctx context.Context) error {
		if !trace.SpanContextFromContext(ctx).IsValid() {
			t.Error("periodic job ran without a span")
		}
		return river.JobSnooze(0)
	})
	if _, ok := errors.AsType[*rivertype.JobSnoozeError](err); !ok {
		t.Errorf("Work() = %v, want the snooze passed through", err)
	}
}

func TestDrainMiddleware(t *testing.T) {
	errWork := errors.New("upstream call failed")
	stopped := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	remotelyCancelled := func() context.Context {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(river.ErrJobCancelledRemotely)
		return ctx
	}
	tests := []struct {
		name       string
		ctx        context.Context
		err        error
		wantSnooze bool
	}{
		{name: "success while running", ctx: context.Background()},
		{name: "failure while running keeps the error", ctx: context.Background(), err: errWork},
		{name: "success after stop", ctx: stopped()},
		{name: "failure after stop is rescheduled", ctx: stopped(), err: errWork, wantSnooze: true},
		{name: "remote cancel keeps the error", ctx: remotelyCancelled(), err: errWork},
		{name: "job cancel after stop keeps the cancel", ctx: stopped(), err: river.JobCancel(errWork)},
	}
	mw := &drainMiddleware{}
	job := &rivertype.JobRow{ID: 7, Kind: "process_audio", Attempt: 5, MaxAttempts: 5}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mw.Work(tt.ctx, job, func(context.Context) error { return tt.err })
			snooze, isSnooze := errors.AsType[*rivertype.JobSnoozeError](got)
			if isSnooze != tt.wantSnooze {
				t.Fatalf("Work() = %v, want snooze %v", got, tt.wantSnooze)
			}
			if isSnooze {
				if snooze.Duration != drainSnooze {
					t.Errorf("snooze = %v, want %v", snooze.Duration, drainSnooze)
				}
				return
			}
			if !errors.Is(got, tt.err) {
				t.Errorf("Work() = %v, want %v", got, tt.err)
			}
		})
	}
}
