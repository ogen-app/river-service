package jobs

import (
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

// --- the asset analysis family ---------------------------------------------

// The analysis model calls run in ogen, detached; the job only waits on them,
// a poll every few seconds. analyzeTimeout covers ogen's 15-minute run plus
// the wait around it.
const (
	analyzeTimeout = 20 * time.Minute
	rebriefTimeout = time.Minute
)

// AnalyzeAssetArgs analyses one lens of an asset. Reason only labels the run;
// uniqueness is per lens.
type AnalyzeAssetArgs struct {
	TenantID   string  `json:"tenant_id"   river:"unique"`
	AssetID    string  `json:"asset_id"    river:"unique"`
	CampaignID *string `json:"campaign_id" river:"unique"`
	Reason     string  `json:"reason"`
}

func (AnalyzeAssetArgs) Kind() string { return "analyze_asset" }

// InsertOpts fold a new job for a lens into one waiting or running, as in
// ogen; the running job re-runs itself when the asset changed under it.
func (AnalyzeAssetArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueAnalysis, MaxAttempts: 5, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: waitingStates}}
}

// ReanalyzeCampaignArgs re-runs a campaign's lenses after its brief changed.
type ReanalyzeCampaignArgs struct {
	TenantID   string `json:"tenant_id"   river:"unique"`
	CampaignID string `json:"campaign_id" river:"unique"`
}

func (ReanalyzeCampaignArgs) Kind() string { return "reanalyze_campaign" }

// InsertOpts: a burst of brief saves collapses into the one waiting run.
func (ReanalyzeCampaignArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueAnalysis, MaxAttempts: 3, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: []rivertype.JobState{
		rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning, rivertype.JobStateScheduled,
	}}}
}

// BackfillAssetAnalysesArgs queues the lenses no trigger made. It only
// touches ogen's tables, so it runs in ogen (MaintenanceService.Run).
type BackfillAssetAnalysesArgs struct{}

func (BackfillAssetAnalysesArgs) Kind() string { return "backfill_asset_analyses" }
func (BackfillAssetAnalysesArgs) InsertOpts() river.InsertOpts {
	opts := sweepOpts()
	opts.Queue = config.QueueAnalysis
	return opts
}

// DistillAssetIdeasArgs runs one "distill more ideas" run; the run row in
// ogen carries the lens, focus and count. Its model call runs detached in
// ogen, like an analysis.
type DistillAssetIdeasArgs struct {
	TenantID string `json:"tenant_id" river:"unique"`
	RunID    string `json:"run_id"    river:"unique"`
}

func (DistillAssetIdeasArgs) Kind() string { return "distill_asset_ideas" }

// InsertOpts: a run is queued once; a second insert while it waits or runs
// folds into it.
func (DistillAssetIdeasArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueAnalysis, MaxAttempts: 5, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: waitingStates}}
}

// distillIdeasTimeout covers ogen's 10-minute detached run plus the wait
// around it.
const distillIdeasTimeout = 15 * time.Minute

func registerAnalysis(reg *queue.Registry, cfg *config.Config, d stepDeps, maintenance Maintenance) {
	queue.Register(reg, newStepWorker[DistillAssetIdeasArgs](d, distillIdeasTimeout), func(m *jobsv1.DistillAssetIdeasArgs) DistillAssetIdeasArgs {
		return DistillAssetIdeasArgs{TenantID: m.GetTenantId(), RunID: m.GetRunId()}
	})
	queue.Register(reg, newStepWorker[AnalyzeAssetArgs](d, analyzeTimeout), func(m *jobsv1.AnalyzeAssetArgs) AnalyzeAssetArgs {
		a := AnalyzeAssetArgs{TenantID: m.GetTenantId(), AssetID: m.GetAssetId(), Reason: m.GetReason()}
		if m.CampaignId != nil {
			a.CampaignID = new(m.GetCampaignId())
		}
		return a
	})
	queue.Register(reg, newStepWorker[ReanalyzeCampaignArgs](d, rebriefTimeout), func(m *jobsv1.ReanalyzeCampaignArgs) ReanalyzeCampaignArgs {
		return ReanalyzeCampaignArgs{TenantID: m.GetTenantId(), CampaignID: m.GetCampaignId()}
	})
	queue.RegisterPeriodic(reg, &sweepWorker[BackfillAssetAnalysesArgs]{rpc: maintenance}, cfg.AnalysisBackfillInterval, BackfillAssetAnalysesArgs{})
}
