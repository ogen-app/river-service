package jobs

import (
	"github.com/riverqueue/river"

	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

// --- automatic post quality ---------------------------------------------------

// AssessDuePostsArgs runs the automatic post quality assessments whose
// lifecycle checkpoint delay has passed. ogen keeps the queue and runs the
// model calls (MaintenanceService.Run); this service keeps the cadence. It
// runs on the analysis queue so a pass, which waits on model calls, never
// holds a default worker.
type AssessDuePostsArgs struct{}

func (AssessDuePostsArgs) Kind() string { return "assess_due_posts" }
func (AssessDuePostsArgs) InsertOpts() river.InsertOpts {
	opts := sweepOpts()
	opts.Queue = config.QueueAnalysis
	return opts
}

func registerPostQuality(reg *queue.Registry, cfg *config.Config, maintenance Maintenance) {
	queue.RegisterPeriodic(reg, &sweepWorker[AssessDuePostsArgs]{rpc: maintenance}, cfg.QualitySweepEvery, AssessDuePostsArgs{})
}
