package jobs

import (
	"context"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"google.golang.org/grpc"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/ogenrpc"
	"github.com/ogen-app/river-service/internal/queue"
)

// The housekeeping sweeps. Each only touches ogen's own tables and storage,
// so the work runs in ogen (MaintenanceService.Run) and river-service just
// keeps the schedule. Cadences match what ogen ran them at.
const (
	postLogCleanupEvery       = time.Hour
	loginSecurityCleanupEvery = 24 * time.Hour
	pluginPairingCleanupEvery = 15 * time.Minute
)

// sweepTimeout bounds one pass, inside the deploy drain window.
const sweepTimeout = 4 * time.Minute

// transientSnooze is how long a sweep waits after ogen was busy or
// unreachable before it tries the same pass again.
const transientSnooze = 30 * time.Second

// periodicUniqueStates lets a new tick queue only while no earlier tick is
// still waiting or running. Completed ticks are left out, or the next tick
// would wait for River's cleaner to remove the finished one.
var periodicUniqueStates = []rivertype.JobState{
	rivertype.JobStateAvailable,
	rivertype.JobStatePending,
	rivertype.JobStateRunning,
	rivertype.JobStateRetryable,
	rivertype.JobStateScheduled,
}

func sweepOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 1, UniqueOpts: river.UniqueOpts{ByState: periodicUniqueStates}}
}

// CleanupPostLogsArgs deletes post logs past their retention.
type CleanupPostLogsArgs struct{}

func (CleanupPostLogsArgs) Kind() string                 { return "cleanup_post_logs" }
func (CleanupPostLogsArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// CleanupNotificationsArgs deletes expired and old notifications.
type CleanupNotificationsArgs struct{}

func (CleanupNotificationsArgs) Kind() string                 { return "cleanup_notifications" }
func (CleanupNotificationsArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// CleanupLoginSecurityArgs deletes expired sessions, stale known devices and
// old login-alert tokens.
type CleanupLoginSecurityArgs struct{}

func (CleanupLoginSecurityArgs) Kind() string                 { return "cleanup_login_security" }
func (CleanupLoginSecurityArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// CleanupPluginPairingsArgs deletes expired plugin pairings.
type CleanupPluginPairingsArgs struct{}

func (CleanupPluginPairingsArgs) Kind() string                 { return "cleanup_plugin_pairings" }
func (CleanupPluginPairingsArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// SweepPendingUploadsArgs deletes presigned uploads never finalized.
type SweepPendingUploadsArgs struct{}

func (SweepPendingUploadsArgs) Kind() string                 { return "sweep_pending_uploads" }
func (SweepPendingUploadsArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// Maintenance is the part of ogen's MaintenanceService a sweep calls.
type Maintenance interface {
	Run(ctx context.Context, in *jobrpcv1.RunRequest, opts ...grpc.CallOption) (*jobrpcv1.RunResponse, error)
}

// sweepWorker runs one housekeeping pass in ogen.
type sweepWorker[A river.JobArgs] struct {
	river.WorkerDefaults[A]
	rpc Maintenance
}

func (w *sweepWorker[A]) Timeout(*river.Job[A]) time.Duration { return sweepTimeout }

func (w *sweepWorker[A]) Work(ctx context.Context, job *river.Job[A]) error {
	_, err := w.rpc.Run(ctx, &jobrpcv1.RunRequest{Task: job.Kind})
	if ogenrpc.Transient(err) {
		return river.JobSnooze(transientSnooze)
	}
	return err
}

func registerHousekeeping(reg *queue.Registry, cfg *config.Config, rpc Maintenance) {
	queue.RegisterPeriodic(reg, &sweepWorker[CleanupPostLogsArgs]{rpc: rpc}, postLogCleanupEvery, CleanupPostLogsArgs{})
	queue.RegisterPeriodic(reg, &sweepWorker[CleanupNotificationsArgs]{rpc: rpc}, cfg.NotificationsCleanupEvery, CleanupNotificationsArgs{})
	queue.RegisterPeriodic(reg, &sweepWorker[CleanupLoginSecurityArgs]{rpc: rpc}, loginSecurityCleanupEvery, CleanupLoginSecurityArgs{})
	queue.RegisterPeriodic(reg, &sweepWorker[CleanupPluginPairingsArgs]{rpc: rpc}, pluginPairingCleanupEvery, CleanupPluginPairingsArgs{})
	queue.RegisterPeriodic(reg, &sweepWorker[SweepPendingUploadsArgs]{rpc: rpc}, cfg.PendingUploadSweepInterval, SweepPendingUploadsArgs{})
}
