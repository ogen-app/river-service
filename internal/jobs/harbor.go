package jobs

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/riverqueue/river"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/logging"
	"github.com/ogen-app/river-service/internal/queue"
)

const harborTimeout = 20 * time.Second

// NotifyHarborTenantRegisteredArgs tells Harbor a tenant signed up.
type NotifyHarborTenantRegisteredArgs struct {
	TenantID string `json:"tenant_id"`
}

func (NotifyHarborTenantRegisteredArgs) Kind() string { return "notify_harbor_tenant_registered" }

func (NotifyHarborTenantRegisteredArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5, UniqueOpts: river.UniqueOpts{ByArgs: true}}
}

// NotifyHarborReceiptReceivedArgs tells Harbor a vendor receipt arrived at
// ogen's inbound address. It carries only the Resend email id: Harbor fetches
// and extracts the email through ogen, so no email content sits in the job.
type NotifyHarborReceiptReceivedArgs struct {
	ResendEmailID string `json:"resend_email_id"`
}

func (NotifyHarborReceiptReceivedArgs) Kind() string { return "notify_harbor_receipt_received" }

func (NotifyHarborReceiptReceivedArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5, UniqueOpts: river.UniqueOpts{ByArgs: true}}
}

// harborEvent is a job that becomes one Harbor webhook.
type harborEvent interface {
	river.JobArgs
	// event names the webhook and the one field that identifies it. An empty
	// value means there is nothing to send.
	event() (name, field, value string)
}

func (a NotifyHarborTenantRegisteredArgs) event() (name, field, value string) {
	return "tenant.registered", "tenant_id", a.TenantID
}

func (a NotifyHarborReceiptReceivedArgs) event() (name, field, value string) {
	return "receipt.received", "resend_email_id", a.ResendEmailID
}

// harborWebhookKey is the lease name of the webhook signing key ogen holds.
const harborWebhookKey = "harbor_webhook_secret"

// harborWorker posts one signed webhook to Harbor per job: the event name and
// a single id. The signing key is leased from ogen.
type harborWorker[A harborEvent] struct {
	river.WorkerDefaults[A]
	url  string
	keys Keys
	http *http.Client
}

func newHarborWorker[A harborEvent](url string, keys Keys) *harborWorker[A] {
	return &harborWorker[A]{url: url, keys: keys, http: &http.Client{
		Timeout:       harborTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (w *harborWorker[A]) Timeout(*river.Job[A]) time.Duration {
	return harborTimeout
}

// Work delivers the webhook. 5xx, 429 and network errors retry; any other
// status is final, because a retry would get the same answer.
func (w *harborWorker[A]) Work(ctx context.Context, job *river.Job[A]) error {
	comp := "jobs." + job.Kind
	event, field, id := job.Args.event()
	if id == "" || w.url == "" {
		slog.DebugContext(ctx, "notify_harbor skipped", logging.AttrComponent, comp, "has_url", w.url != "")
		return nil
	}
	body, err := json.Marshal(map[string]string{"event": event, field: id})
	if err != nil {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		slog.WarnContext(ctx, "notify_harbor: bad webhook url", logging.AttrComponent, comp, logging.AttrError, err)
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	secret, err := w.keys.Get(ctx, harborWebhookKey)
	if err != nil {
		return retryLater(ctx, job.Kind, err)
	}
	// No key set in ogen sends the webhook unsigned, as ogen always has.
	if secret != "" {
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(body)
		req.Header.Set("X-Ogen-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := w.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		slog.InfoContext(ctx, "notify_harbor delivered", logging.AttrComponent, comp, field, id)
		return nil
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
		return fmt.Errorf("harbor webhook returned status %d", resp.StatusCode)
	default:
		slog.WarnContext(ctx, "notify_harbor rejected", logging.AttrComponent, comp, field, id, "status", resp.StatusCode)
		return nil
	}
}

func registerHarbor(reg *queue.Registry, url string, keys Keys) {
	queue.Register(reg, newHarborWorker[NotifyHarborTenantRegisteredArgs](url, keys), func(m *jobsv1.NotifyHarborTenantRegisteredArgs) NotifyHarborTenantRegisteredArgs {
		return NotifyHarborTenantRegisteredArgs{TenantID: m.GetTenantId()}
	})
	queue.Register(reg, newHarborWorker[NotifyHarborReceiptReceivedArgs](url, keys), func(m *jobsv1.NotifyHarborReceiptReceivedArgs) NotifyHarborReceiptReceivedArgs {
		return NotifyHarborReceiptReceivedArgs{ResendEmailID: m.GetResendEmailId()}
	})
}
