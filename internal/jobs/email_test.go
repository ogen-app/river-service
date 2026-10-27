package jobs

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
	"github.com/ogen-app/river-service/internal/resend"
)

type fakeEmailJobs struct {
	prepare    *jobrpcv1.PrepareEmailResponse
	prepareErr error
	recorded   []*jobrpcv1.RecordEmailOutcomeRequest
}

func (f *fakeEmailJobs) PrepareEmail(context.Context, *jobrpcv1.PrepareEmailRequest, ...grpc.CallOption) (*jobrpcv1.PrepareEmailResponse, error) {
	return f.prepare, f.prepareErr
}

func (f *fakeEmailJobs) RecordEmailOutcome(_ context.Context, in *jobrpcv1.RecordEmailOutcomeRequest, _ ...grpc.CallOption) (*jobrpcv1.RecordEmailOutcomeResponse, error) {
	f.recorded = append(f.recorded, in)
	return &jobrpcv1.RecordEmailOutcomeResponse{}, nil
}

type fakeKeys struct {
	key string
	err error
}

func (f fakeKeys) Get(context.Context, string) (string, error) { return f.key, f.err }

type fakeMailer struct {
	id   string
	err  error
	sent []resend.Message
}

func (f *fakeMailer) Send(_ context.Context, _ string, msg resend.Message) (string, error) {
	f.sent = append(f.sent, msg)
	return f.id, f.err
}

func toSend() *jobrpcv1.PrepareEmailResponse {
	return &jobrpcv1.PrepareEmailResponse{Result: &jobrpcv1.PrepareEmailResponse_Send{Send: &jobrpcv1.EmailMessage{
		To: "ann@example.com", Subject: "Hi", Html: "<p>Hi</p>", IdempotencyKey: "welcome:u1",
	}}}
}

func emailJob() *river.Job[SendEmailArgs] {
	return &river.Job[SendEmailArgs]{JobRow: &rivertype.JobRow{Kind: "send_email", Attempt: 1, MaxAttempts: 5}, Args: SendEmailArgs{
		UserID: "u1", TenantID: "t1", TemplateKey: "welcome", EmailKind: "transactional", IdempotencyKey: "welcome:u1",
	}}
}

func TestSendEmailWorker(t *testing.T) {
	tests := []struct {
		name        string
		ogen        *fakeEmailJobs
		keys        fakeKeys
		mailer      *fakeMailer
		wantSends   int
		wantOutcome jobrpcv1.EmailOutcome
		wantSnooze  bool
		wantErr     bool
	}{
		{name: "sent", ogen: &fakeEmailJobs{prepare: toSend()}, keys: fakeKeys{key: "re_1"}, mailer: &fakeMailer{id: "msg_1"},
			wantSends: 1, wantOutcome: jobrpcv1.EmailOutcome_EMAIL_OUTCOME_SENT},
		{name: "settled in ogen", ogen: &fakeEmailJobs{prepare: &jobrpcv1.PrepareEmailResponse{Result: &jobrpcv1.PrepareEmailResponse_Settled{Settled: &jobrpcv1.EmailSettled{}}}},
			keys: fakeKeys{key: "re_1"}, mailer: &fakeMailer{}},
		{name: "no resend key", ogen: &fakeEmailJobs{prepare: toSend()}, mailer: &fakeMailer{},
			wantOutcome: jobrpcv1.EmailOutcome_EMAIL_OUTCOME_SKIPPED_DISABLED},
		{name: "rejected by resend", ogen: &fakeEmailJobs{prepare: toSend()}, keys: fakeKeys{key: "re_1"},
			mailer:    &fakeMailer{err: &resend.Error{Status: 422, Message: "bad recipient"}},
			wantSends: 1, wantOutcome: jobrpcv1.EmailOutcome_EMAIL_OUTCOME_FAILED},
		{name: "resend down", ogen: &fakeEmailJobs{prepare: toSend()}, keys: fakeKeys{key: "re_1"},
			mailer:    &fakeMailer{err: &resend.Error{Status: 503, Transient: true}},
			wantSends: 1, wantErr: true},
		{name: "ogen busy", ogen: &fakeEmailJobs{prepareErr: status.Error(codes.ResourceExhausted, "limit")}, mailer: &fakeMailer{},
			wantSnooze: true},
		{name: "key lease unreachable", ogen: &fakeEmailJobs{prepare: toSend()}, keys: fakeKeys{err: status.Error(codes.Unavailable, "down")},
			mailer: &fakeMailer{}, wantSnooze: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &sendEmailWorker{ogen: tt.ogen, keys: tt.keys, mailer: tt.mailer}
			err := w.Work(t.Context(), emailJob())

			_, snoozed := errors.AsType[*rivertype.JobSnoozeError](err)
			if snoozed != tt.wantSnooze {
				t.Fatalf("Work() = %v, want snooze %v", err, tt.wantSnooze)
			}
			if !tt.wantSnooze && (err != nil) != tt.wantErr {
				t.Fatalf("Work() = %v, want error %v", err, tt.wantErr)
			}
			if len(tt.mailer.sent) != tt.wantSends {
				t.Errorf("sends = %d, want %d", len(tt.mailer.sent), tt.wantSends)
			}
			if tt.wantSends > 0 && tt.mailer.sent[0].IdempotencyKey != "welcome:u1" {
				t.Errorf("idempotency key = %q", tt.mailer.sent[0].IdempotencyKey)
			}
			if tt.wantOutcome == jobrpcv1.EmailOutcome_EMAIL_OUTCOME_UNSPECIFIED {
				if len(tt.ogen.recorded) != 0 {
					t.Errorf("recorded %v, want nothing", tt.ogen.recorded)
				}
				return
			}
			if len(tt.ogen.recorded) != 1 || tt.ogen.recorded[0].GetOutcome() != tt.wantOutcome {
				t.Fatalf("recorded %v, want %v", tt.ogen.recorded, tt.wantOutcome)
			}
			if got := tt.ogen.recorded[0]; got.GetArgs().GetIdempotencyKey() != "welcome:u1" {
				t.Errorf("recorded args = %v", got.GetArgs())
			}
			// Every outcome carries the prepared message: ogen logs the
			// recipient it resolved from it, a skipped send included.
			if got := tt.ogen.recorded[0].GetMessage().GetTo(); got == "" {
				t.Errorf("recorded outcome has no recipient")
			}
		})
	}
}

func TestHarborWorker(t *testing.T) {
	var gotSig, gotBody string
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody, gotSig = string(b), r.Header.Get("X-Ogen-Signature")
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	job := &river.Job[NotifyHarborTenantRegisteredArgs]{JobRow: &rivertype.JobRow{}, Args: NotifyHarborTenantRegisteredArgs{TenantID: "t1"}}

	w := newHarborWorker(srv.URL, fakeKeys{key: "s3cret"})
	if err := w.Work(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, []byte("s3cret"))
	mac.Write([]byte(gotBody))
	if gotSig != "sha256="+hex.EncodeToString(mac.Sum(nil)) || gotBody != `{"event":"tenant.registered","tenant_id":"t1"}` {
		t.Errorf("body %s, signature %s", gotBody, gotSig)
	}

	status = http.StatusBadGateway
	if err := w.Work(t.Context(), job); err == nil {
		t.Error("5xx: want an error so River retries")
	}
	status = http.StatusBadRequest
	if err := w.Work(t.Context(), job); err != nil {
		t.Errorf("4xx: %v, want a final nil", err)
	}
	if err := newHarborWorker("", fakeKeys{}).Work(t.Context(), job); err != nil {
		t.Errorf("no url: %v, want a no-op", err)
	}
	status = http.StatusOK
	if err := newHarborWorker(srv.URL, fakeKeys{}).Work(t.Context(), job); err != nil || gotSig != "" {
		t.Errorf("no key set in ogen: %v, signature %q; want an unsigned delivery", err, gotSig)
	}
}

// TestSendEmailLastAttemptRecordsTheFailure checks that a send Resend keeps
// refusing transiently ends with a failed outcome in ogen, not with no record.
func TestSendEmailLastAttemptRecordsTheFailure(t *testing.T) {
	ogen := &fakeEmailJobs{prepare: toSend()}
	w := &sendEmailWorker{ogen: ogen, keys: fakeKeys{key: "re_1"}, mailer: &fakeMailer{err: &resend.Error{Status: 503, Transient: true, Message: "down"}}}
	job := emailJob()
	job.Attempt = job.MaxAttempts
	if err := w.Work(t.Context(), job); err != nil {
		t.Fatalf("Work() = %v, want nil once the failure is recorded", err)
	}
	if len(ogen.recorded) != 1 || ogen.recorded[0].GetOutcome() != jobrpcv1.EmailOutcome_EMAIL_OUTCOME_FAILED || ogen.recorded[0].GetError() == "" {
		t.Fatalf("recorded %v, want one failed outcome with the error", ogen.recorded)
	}
}
