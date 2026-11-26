package jobs

import (
	"time"

	"github.com/riverqueue/river"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

// --- billing ---------------------------------------------------------------

// BillingApplyScheduledChangeArgs advanced a Lemon Squeezy plan change in two
// timed steps. Polar holds a downgrade itself, so ogen no longer queues it and
// answers any job still queued as done.
type BillingApplyScheduledChangeArgs struct {
	ChangeID string `json:"change_id"`
}

func (BillingApplyScheduledChangeArgs) Kind() string { return "billing_apply_scheduled_change" }

// InsertOpts retry provider failures; ogen fails the change on the last
// attempt.
func (BillingApplyScheduledChangeArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5}
}

// BillingReconcileArgs is the periodic re-sync behind the billing provider's
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

// BillingSyncPaymentsArgs fetches the ECB exchange rates, then pages through
// the whole payment history of the billing provider (Polar: orders, then
// refunds) in the billing mode ogen runs in. Each step is one call: ogen plans
// it, decodes it and upserts the rates or the payments. The first run backfills what predates
// the webhooks; later runs pick up refunds whose webhook was lost. A retry
// starts over from the first page, which is safe because every page is an
// upsert. ogen skips the pass while billing is off.
type BillingSyncPaymentsArgs struct{}

func (BillingSyncPaymentsArgs) Kind() string { return "billing_sync_payments" }

// InsertOpts keep one sync waiting or running; a tick that lands while one
// is still paging folds into it.
func (BillingSyncPaymentsArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 3, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: waitingStates}}
}

// billingSyncPaymentsTimeout bounds one sync attempt: many pages, each a
// short provider call, well under RESCUE_STUCK_JOBS_AFTER.
const billingSyncPaymentsTimeout = 10 * time.Minute

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
	queue.RegisterPeriodic(reg, newStepWorker[BillingSyncPaymentsArgs](d, billingSyncPaymentsTimeout), cfg.BillingPaymentsSyncInterval, BillingSyncPaymentsArgs{})
	queue.RegisterPeriodic(reg, &sweepWorker[BillingWebhookEventsSweepArgs]{rpc: maintenance}, billingWebhookSweepEvery, BillingWebhookEventsSweepArgs{})
}
