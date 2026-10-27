package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river"
	"google.golang.org/grpc"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/credentials"
	"github.com/ogen-app/river-service/internal/logging"
	"github.com/ogen-app/river-service/internal/ogenrpc"
	"github.com/ogen-app/river-service/internal/queue"
	"github.com/ogen-app/river-service/internal/resend"
)

// emailLogCleanupEvery is the email-log retention sweep's cadence, as in ogen.
const emailLogCleanupEvery = time.Hour

// sendEmailTimeout bounds one attempt: two ogen calls and one Resend call.
const sendEmailTimeout = 30 * time.Second

// SendEmailArgs sends one templated email. The fields mirror jobs.v1
// SendEmailArgs.
type SendEmailArgs struct {
	UserID         string            `json:"user_id"`
	TenantID       string            `json:"tenant_id"`
	TemplateKey    string            `json:"template_key"`
	EmailKind      string            `json:"email_kind"`
	IdempotencyKey string            `json:"idempotency_key"`
	ToEmail        string            `json:"to_email,omitempty"`
	ToName         string            `json:"to_name,omitempty"`
	Vars           map[string]string `json:"vars,omitempty"`
	// Report is a weekly report as JSON. It is ogen's: this service only
	// passes it back.
	Report json.RawMessage `json:"report,omitempty"`
}

func (SendEmailArgs) Kind() string { return "send_email" }

// InsertOpts matches ogen's: five attempts, and an identical job waiting is
// not queued twice. Resend's idempotency key and ogen's email-log index are
// the hard guarantees against a double send.
func (SendEmailArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5, UniqueOpts: river.UniqueOpts{ByArgs: true}}
}

func (a SendEmailArgs) proto() *jobsv1.SendEmailArgs {
	return &jobsv1.SendEmailArgs{
		UserId: a.UserID, TenantId: a.TenantID, TemplateKey: a.TemplateKey, EmailKind: a.EmailKind,
		IdempotencyKey: a.IdempotencyKey, ToEmail: a.ToEmail, ToName: a.ToName, Vars: a.Vars, Report: a.Report,
	}
}

func sendEmailArgs(m *jobsv1.SendEmailArgs) SendEmailArgs {
	return SendEmailArgs{
		UserID: m.GetUserId(), TenantID: m.GetTenantId(), TemplateKey: m.GetTemplateKey(), EmailKind: m.GetEmailKind(),
		IdempotencyKey: m.GetIdempotencyKey(), ToEmail: m.GetToEmail(), ToName: m.GetToName(), Vars: m.GetVars(),
		Report: m.GetReport(),
	}
}

// EmailJobs is the part of ogen's EmailJobService the worker calls.
type EmailJobs interface {
	PrepareEmail(ctx context.Context, in *jobrpcv1.PrepareEmailRequest, opts ...grpc.CallOption) (*jobrpcv1.PrepareEmailResponse, error)
	RecordEmailOutcome(ctx context.Context, in *jobrpcv1.RecordEmailOutcomeRequest, opts ...grpc.CallOption) (*jobrpcv1.RecordEmailOutcomeResponse, error)
}

// Keys returns a vendor key, empty when it is not set.
type Keys interface {
	Get(ctx context.Context, name string) (string, error)
}

// Mailer sends one rendered email.
type Mailer interface {
	Send(ctx context.Context, apiKey string, msg resend.Message) (string, error)
}

// sendEmailWorker has ogen prepare the message, sends it through Resend, and
// has ogen record the outcome. Everything that needs ogen's data stays there.
type sendEmailWorker struct {
	river.WorkerDefaults[SendEmailArgs]
	ogen   EmailJobs
	keys   Keys
	mailer Mailer
}

func (w *sendEmailWorker) Timeout(*river.Job[SendEmailArgs]) time.Duration { return sendEmailTimeout }

func (w *sendEmailWorker) Work(ctx context.Context, job *river.Job[SendEmailArgs]) error {
	args := job.Args.proto()
	prep, err := w.ogen.PrepareEmail(ctx, &jobrpcv1.PrepareEmailRequest{Args: args})
	if err != nil {
		return retryLater(err)
	}
	msg := prep.GetSend()
	if msg == nil {
		slog.InfoContext(ctx, "send_email settled in ogen", logging.AttrComponent, "jobs.send_email",
			"template", job.Args.TemplateKey, "reason", prep.GetSettled().GetReason())
		return nil
	}

	key, err := w.keys.Get(ctx, credentials.Resend)
	if err != nil {
		return retryLater(err)
	}
	if key == "" {
		return w.record(ctx, args, jobrpcv1.EmailOutcome_EMAIL_OUTCOME_SKIPPED_DISABLED, nil, "", "")
	}

	id, err := w.mailer.Send(ctx, key, resend.Message{
		From: msg.GetFrom(), ReplyTo: msg.GetReplyTo(), To: msg.GetTo(), Subject: msg.GetSubject(),
		HTML: msg.GetHtml(), Text: msg.GetText(), Headers: msg.GetHeaders(), IdempotencyKey: msg.GetIdempotencyKey(),
	})
	if sendErr, ok := errors.AsType[*resend.Error](err); ok && !sendErr.Transient {
		slog.WarnContext(ctx, "send_email rejected by resend", logging.AttrComponent, "jobs.send_email",
			"template", job.Args.TemplateKey, logging.AttrError, err)
		return w.record(ctx, args, jobrpcv1.EmailOutcome_EMAIL_OUTCOME_FAILED, msg, "", sendErr.Error())
	}
	if err != nil {
		if job.Attempt >= job.MaxAttempts {
			// Out of retries: record the failure so the send never ends
			// without an outcome in ogen.
			return w.record(ctx, args, jobrpcv1.EmailOutcome_EMAIL_OUTCOME_FAILED, msg, "", err.Error())
		}
		return err // transient: River retries, and the idempotency key stops a double send
	}
	return w.record(ctx, args, jobrpcv1.EmailOutcome_EMAIL_OUTCOME_SENT, msg, id, "")
}

// record has ogen log the outcome. If that fails after a send, the retry
// prepares and sends again; Resend's idempotency key returns the first send,
// so the mail still goes out once.
func (w *sendEmailWorker) record(ctx context.Context, args *jobsv1.SendEmailArgs, outcome jobrpcv1.EmailOutcome, msg *jobrpcv1.EmailMessage, providerID, errMsg string) error {
	_, err := w.ogen.RecordEmailOutcome(ctx, &jobrpcv1.RecordEmailOutcomeRequest{
		Args: args, Outcome: outcome, Message: msg, ProviderMessageId: providerID, Error: errMsg,
	})
	if err != nil {
		return retryLater(fmt.Errorf("record email outcome: %w", err))
	}
	return nil
}

// retryLater snoozes on an ogen RPC that is busy, unreachable or not deployed
// yet, and returns any other error for River to retry.
func retryLater(err error) error {
	if ogenrpc.Transient(err) {
		return river.JobSnooze(transientSnooze)
	}
	return err
}

// CleanupEmailLogsArgs deletes email logs past their retention.
type CleanupEmailLogsArgs struct{}

func (CleanupEmailLogsArgs) Kind() string                 { return "cleanup_email_logs" }
func (CleanupEmailLogsArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

func registerEmail(reg *queue.Registry, ogen EmailJobs, maintenance Maintenance, keys Keys, mailer Mailer) {
	queue.Register(reg, &sendEmailWorker{ogen: ogen, keys: keys, mailer: mailer}, sendEmailArgs)
	queue.RegisterPeriodic(reg, &sweepWorker[CleanupEmailLogsArgs]{rpc: maintenance}, emailLogCleanupEvery, CleanupEmailLogsArgs{})
}
