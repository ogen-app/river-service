package jobs

import (
	"time"

	"github.com/riverqueue/river"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

// --- billing ---------------------------------------------------------------

// BillingApplyScheduledChangeArgs advances one scheduled plan change: an hour
// before it takes effect the Lemon Squeezy switch, at the effective time the
// tier move. ogen picks the due step from the change's status.
type BillingApplyScheduledChangeArgs struct {
	ChangeID string `json:"change_id"`
}

func (BillingApplyScheduledChangeArgs) Kind() string { return "billing_apply_scheduled_change" }

// InsertOpts retry provider failures; ogen fails the change on the last
// attempt.
func (BillingApplyScheduledChangeArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5}
}

// BillingReconcileArgs is the periodic re-sync behind the Lemon Squeezy
// webhooks. ogen does the part in its own tables and answers with one
// billing_resync_subscription child per subscription to re-read.
type BillingReconcileArgs struct{}

func (BillingReconcileArgs) Kind() string                 { return "billing_reconcile" }
func (BillingReconcileArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// BillingResyncSubscriptionArgs re-reads one subscription for the reconcile.
type BillingResyncSubscriptionArgs struct {
	ProviderSubscriptionID string `json:"provider_subscription_id"`
}

func (BillingResyncSubscriptionArgs) Kind() string { return "billing_resync_subscription" }

// InsertOpts: a few tries; the next reconcile reads it again anyway.
func (BillingResyncSubscriptionArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 3}
}

// BillingWebhookEventsSweepArgs prunes the webhook log. It only touches
// ogen's tables, so it runs in ogen (MaintenanceService.Run).
type BillingWebhookEventsSweepArgs struct{}

func (BillingWebhookEventsSweepArgs) Kind() string                 { return "billing_webhook_events_sweep" }
func (BillingWebhookEventsSweepArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// billingWebhookSweepEvery is the webhook-log prune's cadence, as in ogen.
const billingWebhookSweepEvery = 24 * time.Hour

func registerBilling(reg *queue.Registry, cfg *config.Config, d stepDeps, maintenance Maintenance) {
	queue.Register(reg, newStepWorker[BillingApplyScheduledChangeArgs](d, 0), func(m *jobsv1.BillingApplyScheduledChangeArgs) BillingApplyScheduledChangeArgs {
		return BillingApplyScheduledChangeArgs{ChangeID: m.GetChangeId()}
	})
	queue.RegisterPeriodic(reg, newStepWorker[BillingReconcileArgs](d, 0), cfg.BillingReconcileInterval, BillingReconcileArgs{})
	queue.RegisterInternal(reg, newStepWorker[BillingResyncSubscriptionArgs](d, 0))
	d.children.build[BillingResyncSubscriptionArgs{}.Kind()] = childOf[BillingResyncSubscriptionArgs]()
	queue.RegisterPeriodic(reg, &sweepWorker[BillingWebhookEventsSweepArgs]{rpc: maintenance}, billingWebhookSweepEvery, BillingWebhookEventsSweepArgs{})
}
