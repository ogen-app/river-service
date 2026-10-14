package jobs

import (
	"time"

	"github.com/riverqueue/river"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

// --- the publishing family -------------------------------------------------

// zernioCallTimeout bounds one attempt of a publishing job that makes a single
// Zernio call, as in ogen.
const zernioCallTimeout = 20 * time.Second

// submitTimeout bounds one submit attempt, which streams the post's media to
// Zernio; it stays below RESCUE_STUCK_JOBS_AFTER's default.
const submitTimeout = 25 * time.Minute

// SubmitPostToZernioArgs submits a scheduled post to Zernio.
type SubmitPostToZernioArgs struct {
	PostID string `json:"post_id"`
}

func (SubmitPostToZernioArgs) Kind() string                 { return "submit_post_to_zernio" }
func (SubmitPostToZernioArgs) InsertOpts() river.InsertOpts { return river.InsertOpts{MaxAttempts: 5} }

// PollZernioStatusArgs checks a submitted post's publish status. It snoozes
// itself until the post is published or fails.
type PollZernioStatusArgs struct {
	PostID          string `json:"post_id"`
	PublisherPostID string `json:"publisher_post_id,omitempty"`
}

func (PollZernioStatusArgs) Kind() string { return "poll_zernio_status" }

// InsertOpts: one live poll per submission however often it is enqueued.
func (PollZernioStatusArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 3, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: waitingStates}}
}

// CancelZernioJobArgs takes a post back out of Zernio's queue.
type CancelZernioJobArgs struct {
	PostID string `json:"post_id"`
	Target string `json:"target"`
	Actor  string `json:"actor"`
}

func (CancelZernioJobArgs) Kind() string                 { return "cancel_zernio_job" }
func (CancelZernioJobArgs) InsertOpts() river.InsertOpts { return river.InsertOpts{MaxAttempts: 3} }

// WithdrawZernioPostArgs deletes a Zernio post no Ogen post holds any more.
type WithdrawZernioPostArgs struct {
	PublisherPostID string `json:"publisher_post_id"`
	PostID          string `json:"post_id"`
	TenantID        string `json:"tenant_id"`
	Reason          string `json:"reason"`
	Actor           string `json:"actor"`
}

func (WithdrawZernioPostArgs) Kind() string                 { return "withdraw_zernio_post" }
func (WithdrawZernioPostArgs) InsertOpts() river.InsertOpts { return river.InsertOpts{MaxAttempts: 8} }

// PostFirstCommentArgs posts a published post's delayed first comment.
type PostFirstCommentArgs struct {
	PostID          string `json:"post_id"`
	PublisherPostID string `json:"publisher_post_id"`
}

func (PostFirstCommentArgs) Kind() string { return "post_first_comment" }

// InsertOpts: one live job per comment.
func (PostFirstCommentArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: waitingStates}}
}

// The publishing sweeps. Reconcile and manual-publish-due only touch ogen's
// tables, so they run in ogen (MaintenanceService.Run). The orphan sweep and
// the analytics and follower refreshes call Zernio, so each fans out into one
// step-run job per tenant profile.

// ReconcileScheduledPostsArgs fails posts stuck in Scheduled past their grace.
type ReconcileScheduledPostsArgs struct{}

func (ReconcileScheduledPostsArgs) Kind() string                 { return "reconcile_scheduled_posts" }
func (ReconcileScheduledPostsArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// DetectManualPublishDueArgs notifies owners of manual posts now due.
type DetectManualPublishDueArgs struct{}

func (DetectManualPublishDueArgs) Kind() string                 { return "detect_manual_publish_due" }
func (DetectManualPublishDueArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// SweepZernioOrphansArgs withdraws Zernio posts no scheduled Ogen post holds.
type SweepZernioOrphansArgs struct{}

func (SweepZernioOrphansArgs) Kind() string                 { return "sweep_zernio_orphans" }
func (SweepZernioOrphansArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// RefreshZernioAnalyticsArgs refreshes published posts' analytics.
type RefreshZernioAnalyticsArgs struct{}

func (RefreshZernioAnalyticsArgs) Kind() string                 { return "refresh_zernio_analytics" }
func (RefreshZernioAnalyticsArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// RefreshZernioFollowersArgs snapshots connected accounts' follower counts.
type RefreshZernioFollowersArgs struct{}

func (RefreshZernioFollowersArgs) Kind() string                 { return "refresh_zernio_followers" }
func (RefreshZernioFollowersArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// OrphanSweepProfileArgs sweeps one tenant profile.
type OrphanSweepProfileArgs struct {
	TenantID  string `json:"tenant_id"`
	ProfileID string `json:"profile_id"`
}

func (OrphanSweepProfileArgs) Kind() string                 { return "sweep_zernio_orphans_profile" }
func (OrphanSweepProfileArgs) InsertOpts() river.InsertOpts { return tenantChildOpts() }

// AnalyticsTenantArgs refreshes one tenant's analytics.
type AnalyticsTenantArgs struct {
	TenantID string `json:"tenant_id"`
}

func (AnalyticsTenantArgs) Kind() string                 { return "refresh_zernio_analytics_tenant" }
func (AnalyticsTenantArgs) InsertOpts() river.InsertOpts { return tenantChildOpts() }

// FollowersTenantArgs refreshes one tenant's follower stats.
type FollowersTenantArgs struct {
	TenantID  string `json:"tenant_id"`
	ProfileID string `json:"profile_id"`
}

func (FollowersTenantArgs) Kind() string                 { return "refresh_zernio_followers_tenant" }
func (FollowersTenantArgs) InsertOpts() river.InsertOpts { return tenantChildOpts() }

// tenantChildOpts: one attempt, as the sweep had (the next tick tries again),
// and one waiting job per tenant.
func tenantChildOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 1, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: waitingStates}}
}

// Attempt bounds of the sweeps' jobs. A fan-out step is one query; a tenant
// job pages through Zernio, a few seconds a page.
const (
	fanOutTimeout      = time.Minute
	tenantChildTimeout = 5 * time.Minute
)

func registerPublishingSweeps(reg *queue.Registry, cfg *config.Config, d stepDeps, maintenance Maintenance) {
	queue.RegisterPeriodic(reg, &sweepWorker[ReconcileScheduledPostsArgs]{rpc: maintenance}, cfg.ReconcileEvery, ReconcileScheduledPostsArgs{})
	queue.RegisterPeriodic(reg, &sweepWorker[DetectManualPublishDueArgs]{rpc: maintenance}, cfg.ManualPublishDueSweepEvery, DetectManualPublishDueArgs{})

	queue.RegisterPeriodic(reg, newStepWorker[SweepZernioOrphansArgs](d, fanOutTimeout), cfg.ZernioOrphanSweepInterval, SweepZernioOrphansArgs{})
	queue.RegisterPeriodic(reg, newStepWorker[RefreshZernioAnalyticsArgs](d, fanOutTimeout), cfg.ZernioAnalyticsRefreshInterval, RefreshZernioAnalyticsArgs{})
	queue.RegisterPeriodic(reg, newStepWorker[RefreshZernioFollowersArgs](d, fanOutTimeout), cfg.ZernioFollowerRefreshInterval, RefreshZernioFollowersArgs{})

	queue.RegisterInternal(reg, newStepWorker[OrphanSweepProfileArgs](d, tenantChildTimeout))
	d.children.build[OrphanSweepProfileArgs{}.Kind()] = childOf[OrphanSweepProfileArgs]()
	queue.RegisterInternal(reg, newStepWorker[AnalyticsTenantArgs](d, tenantChildTimeout))
	d.children.build[AnalyticsTenantArgs{}.Kind()] = childOf[AnalyticsTenantArgs]()
	queue.RegisterInternal(reg, newStepWorker[FollowersTenantArgs](d, tenantChildTimeout))
	d.children.build[FollowersTenantArgs{}.Kind()] = childOf[FollowersTenantArgs]()
}

func registerPublishing(reg *queue.Registry, d stepDeps) {
	queue.Register(reg, newStepWorker[SubmitPostToZernioArgs](d, submitTimeout), func(m *jobsv1.SubmitPostToZernioArgs) SubmitPostToZernioArgs {
		return SubmitPostToZernioArgs{PostID: m.GetPostId()}
	})
	queue.Register(reg, newStepWorker[PollZernioStatusArgs](d, zernioCallTimeout), func(m *jobsv1.PollZernioStatusArgs) PollZernioStatusArgs {
		return PollZernioStatusArgs{PostID: m.GetPostId(), PublisherPostID: m.GetPublisherPostId()}
	})
	queue.Register(reg, newStepWorker[CancelZernioJobArgs](d, zernioCallTimeout), func(m *jobsv1.CancelZernioJobArgs) CancelZernioJobArgs {
		return CancelZernioJobArgs{PostID: m.GetPostId(), Target: m.GetTarget(), Actor: m.GetActor()}
	})
	queue.Register(reg, newStepWorker[WithdrawZernioPostArgs](d, zernioCallTimeout), func(m *jobsv1.WithdrawZernioPostArgs) WithdrawZernioPostArgs {
		return WithdrawZernioPostArgs{
			PublisherPostID: m.GetPublisherPostId(), PostID: m.GetPostId(), TenantID: m.GetTenantId(),
			Reason: m.GetReason(), Actor: m.GetActor(),
		}
	})
	queue.Register(reg, newStepWorker[PostFirstCommentArgs](d, zernioCallTimeout), func(m *jobsv1.PostFirstCommentArgs) PostFirstCommentArgs {
		return PostFirstCommentArgs{PostID: m.GetPostId(), PublisherPostID: m.GetPublisherPostId()}
	})
}
