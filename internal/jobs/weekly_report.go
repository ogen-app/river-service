package jobs

import (
	"github.com/riverqueue/river"

	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

// --- the weekly report -----------------------------------------------------

// WeeklyReportDispatchArgs is the hourly tick that finds the workspaces whose
// Monday 09:00 has come, in their own zone. ogen answers with one
// build_weekly_report child per workspace due.
type WeeklyReportDispatchArgs struct{}

func (WeeklyReportDispatchArgs) Kind() string                 { return "weekly_report_dispatch" }
func (WeeklyReportDispatchArgs) InsertOpts() river.InsertOpts { return sweepOpts() }

// BuildWeeklyReportArgs builds one workspace's report for the week starting
// WeekStart and has ogen queue the owners' emails.
type BuildWeeklyReportArgs struct {
	TenantID  string `json:"tenant_id"`
	WeekStart string `json:"week_start"`
}

func (BuildWeeklyReportArgs) Kind() string { return "build_weekly_report" }

// InsertOpts make the build unique per workspace and week in every state,
// completed included, so a later tick the same Monday builds nothing.
// Completed jobs outlast the Monday send window; the emails' idempotency keys
// stop a second send even past it.
func (BuildWeeklyReportArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 3, UniqueOpts: river.UniqueOpts{ByArgs: true}}
}

func registerWeeklyReport(reg *queue.Registry, cfg *config.Config, d stepDeps) {
	queue.RegisterPeriodic(reg, newStepWorker[WeeklyReportDispatchArgs](d, fanOutTimeout), cfg.WeeklyReportCheckEvery, WeeklyReportDispatchArgs{})
	queue.RegisterInternal(reg, newStepWorker[BuildWeeklyReportArgs](d, tenantChildTimeout))
	d.children.build[BuildWeeklyReportArgs{}.Kind()] = childOf[BuildWeeklyReportArgs]()
}
