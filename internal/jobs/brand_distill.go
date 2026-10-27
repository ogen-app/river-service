package jobs

import (
	"time"

	"github.com/riverqueue/river"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

// --- brand distillation ----------------------------------------------------

// DistillBrandArgs moves one brand distillation run on. ogen does the quick
// work in its steps, this service makes the site map (Firecrawl), the job
// snoozes while the run's sources ingest, and the model stages run detached
// in ogen while the job waits on them.
type DistillBrandArgs struct {
	TenantID string `json:"tenant_id" river:"unique"`
	RunID    string `json:"run_id"    river:"unique"`
}

func (DistillBrandArgs) Kind() string { return "distill_brand" }

// InsertOpts: a run is queued once; a second insert while it waits or runs
// folds into it.
func (DistillBrandArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueAnalysis, MaxAttempts: 5, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: waitingStates}}
}

// ExpireBrandDistillsArgs expires proposals nobody committed in time. It only
// touches ogen's tables, so it runs in ogen (MaintenanceService.Run).
type ExpireBrandDistillsArgs struct{}

func (ExpireBrandDistillsArgs) Kind() string { return "expire_brand_distills" }
func (ExpireBrandDistillsArgs) InsertOpts() river.InsertOpts {
	opts := sweepOpts()
	opts.Queue = config.QueueAnalysis
	return opts
}

const (
	// distillBrandTimeout covers ogen's 20-minute detached model stages
	// plus the wait around them.
	distillBrandTimeout = 25 * time.Minute
	// brandDistillExpireEvery is the expiry sweep's cadence, as in ogen.
	brandDistillExpireEvery = 6 * time.Hour
)

func registerBrandDistill(reg *queue.Registry, d stepDeps, maintenance Maintenance) {
	queue.Register(reg, newStepWorker[DistillBrandArgs](d, distillBrandTimeout), func(m *jobsv1.DistillBrandArgs) DistillBrandArgs {
		return DistillBrandArgs{TenantID: m.GetTenantId(), RunID: m.GetRunId()}
	})
	queue.RegisterPeriodic(reg, &sweepWorker[ExpireBrandDistillsArgs]{rpc: maintenance}, brandDistillExpireEvery, ExpireBrandDistillsArgs{})
}
