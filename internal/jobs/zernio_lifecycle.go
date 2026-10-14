package jobs

import (
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

// --- the Zernio lifecycle family -----------------------------------------

// BootstrapZernioProfileArgs creates or adopts a tenant's Zernio profile.
type BootstrapZernioProfileArgs struct {
	TenantID string `json:"tenant_id"`
}

func (BootstrapZernioProfileArgs) Kind() string { return "bootstrap_zernio_profile" }
func (BootstrapZernioProfileArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5}
}

// TeardownZernioProfileArgs deletes a deleted workspace's Zernio profile.
type TeardownZernioProfileArgs struct {
	TenantID string `json:"tenant_id"`
}

func (TeardownZernioProfileArgs) Kind() string { return "teardown_zernio_profile" }
func (TeardownZernioProfileArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5}
}

// DetectExpiringConnectionsArgs is the connection-health sweep. ogen fans it
// out into one ExpiringConnectionsTenantArgs job per tenant.
type DetectExpiringConnectionsArgs struct{}

func (DetectExpiringConnectionsArgs) Kind() string                 { return "detect_expiring_connections" }
func (DetectExpiringConnectionsArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// ExpiringConnectionsTenantArgs checks one tenant's account health.
type ExpiringConnectionsTenantArgs struct {
	TenantID  string `json:"tenant_id"`
	ProfileID string `json:"profile_id"`
}

func (ExpiringConnectionsTenantArgs) Kind() string { return "detect_expiring_connections_tenant" }

// InsertOpts: one tenant check waiting at a time; a few attempts, then the
// next sweep tries again.
func (ExpiringConnectionsTenantArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 3, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: waitingStates}}
}

// waitingStates fold a duplicate child into one still waiting or running.
var waitingStates = []rivertype.JobState{
	rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning,
	rivertype.JobStateRetryable, rivertype.JobStateScheduled,
}

// CleanupZernioConnectSessionsArgs deletes expired headless-connect sessions.
type CleanupZernioConnectSessionsArgs struct{}

func (CleanupZernioConnectSessionsArgs) Kind() string                 { return "cleanup_zernio_connect_sessions" }
func (CleanupZernioConnectSessionsArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// connectSessionCleanupEvery is that sweep's cadence, as in ogen.
const connectSessionCleanupEvery = 15 * time.Minute

func registerZernioLifecycle(reg *queue.Registry, cfg *config.Config, d stepDeps, maintenance Maintenance) {
	queue.Register(reg, newStepWorker[BootstrapZernioProfileArgs](d, 0), func(m *jobsv1.BootstrapZernioProfileArgs) BootstrapZernioProfileArgs {
		return BootstrapZernioProfileArgs{TenantID: m.GetTenantId()}
	})
	queue.Register(reg, newStepWorker[TeardownZernioProfileArgs](d, 0), func(m *jobsv1.TeardownZernioProfileArgs) TeardownZernioProfileArgs {
		return TeardownZernioProfileArgs{TenantID: m.GetTenantId()}
	})
	queue.RegisterPeriodic(reg, newStepWorker[DetectExpiringConnectionsArgs](d, 0), cfg.ZernioHealthCheckInterval, DetectExpiringConnectionsArgs{})
	queue.RegisterInternal(reg, newStepWorker[ExpiringConnectionsTenantArgs](d, 0))
	d.children.build[ExpiringConnectionsTenantArgs{}.Kind()] = childOf[ExpiringConnectionsTenantArgs]()
	queue.RegisterPeriodic(reg, &sweepWorker[CleanupZernioConnectSessionsArgs]{rpc: maintenance}, connectSessionCleanupEvery, CleanupZernioConnectSessionsArgs{})
}
