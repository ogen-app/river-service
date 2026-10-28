package jobs

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/ogenrpc"
	"github.com/ogen-app/river-service/internal/queue"
)

// TestEveryEnqueueKindIsRegistered keeps this build in step with the
// contract: a jobs.v1 args field with no worker here would sit in ogen's
// outbox, refused, once ogen relays its kind.
func TestEveryEnqueueKindIsRegistered(t *testing.T) {
	reg := Registry(&config.Config{}, &ogenrpc.Client{})
	oneof := (&jobsv1.EnqueueRequest{}).ProtoReflect().Descriptor().Oneofs().ByName("args")
	fields := oneof.Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		req := &jobsv1.EnqueueRequest{}
		m := req.ProtoReflect()
		m.Set(f, protoreflect.ValueOfMessage(m.NewField(f).Message()))
		if _, kind, ok := reg.Args(req); !ok {
			t.Errorf("no worker for %s", kind)
		}
	}
}

func TestCatchUpKindsCarryTheirFields(t *testing.T) {
	reg := Registry(&config.Config{}, &ogenrpc.Client{})
	report := []byte(`{"schema_version":1,"headline":"12 posts went out"}`)
	args, _, ok := reg.Args(&jobsv1.EnqueueRequest{Args: &jobsv1.EnqueueRequest_SendEmail{SendEmail: &jobsv1.SendEmailArgs{UserId: "u1", Report: report}}})
	if !ok {
		t.Fatal("send_email not registered")
	}
	email := args.(SendEmailArgs)
	if string(email.Report) != string(report) || string(email.proto().GetReport()) != string(report) {
		t.Fatalf("report = %s", email.Report)
	}
	// The report rides River's JSON args as an object, so a stored job reads
	// back the same.
	raw, err := json.Marshal(email)
	if err != nil {
		t.Fatal(err)
	}
	var back SendEmailArgs
	if err := json.Unmarshal(raw, &back); err != nil || string(back.Report) != string(report) {
		t.Fatalf("round trip: %s, %v", back.Report, err)
	}

	args, _, _ = reg.Args(&jobsv1.EnqueueRequest{Args: &jobsv1.EnqueueRequest_TeardownZernioProfile{TeardownZernioProfile: &jobsv1.TeardownZernioProfileArgs{TenantId: "t1", Reason: "trial_unpaid"}}})
	if args.(TeardownZernioProfileArgs).Reason != "trial_unpaid" {
		t.Fatalf("teardown args = %+v", args)
	}
}

func TestCatchUpSweepsAndChildren(t *testing.T) {
	cfg := &config.Config{BillingReconcileInterval: time.Hour}
	d := stepDeps{ogen: &scriptedSteps{}, actions: &recordingActions{}, children: newChildKinds()}
	reg := queue.NewRegistry()
	registerWeeklyReport(reg, cfg, d)
	registerBilling(reg, cfg, d, &fakeMaintenance{})
	registerBrandDistill(reg, d, &fakeMaintenance{})
	// WEEKLY_REPORT_CHECK_EVERY is zero here, so only the billing reconcile,
	// the webhook prune and the brand expiry are scheduled.
	if reg.Len() != 9 || len(reg.PeriodicJobs()) != 3 {
		t.Errorf("kinds %d, periodic %d; want 9 and 3", reg.Len(), len(reg.PeriodicJobs()))
	}
	for kind, args := range map[string]string{
		"build_weekly_report":         `{"tenant_id":"t1","week_start":"2026-10-26"}`,
		"billing_resync_subscription": `{"provider_subscription_id":"42"}`,
		"billing_backfill_payments":   `{}`,
	} {
		if _, err := d.children.args(kind, []byte(args)); err != nil {
			t.Errorf("child %s: %v", kind, err)
		}
	}
}
