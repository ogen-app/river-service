package jobs

import (
	"slices"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

func TestPostQualitySweep(t *testing.T) {
	reg := queue.NewRegistry()
	registerPostQuality(reg, &config.Config{QualitySweepEvery: time.Minute}, &fakeMaintenance{})
	if reg.Len() != 1 || len(reg.PeriodicJobs()) != 1 {
		t.Fatalf("kinds %d, periodic %d; want 1 and 1", reg.Len(), len(reg.PeriodicJobs()))
	}
	// QUALITY_SWEEP_EVERY=0 keeps the worker but schedules nothing.
	off := queue.NewRegistry()
	registerPostQuality(off, &config.Config{}, &fakeMaintenance{})
	if off.Len() != 1 || len(off.PeriodicJobs()) != 0 {
		t.Fatalf("off: kinds %d, periodic %d; want 1 and 0", off.Len(), len(off.PeriodicJobs()))
	}
	if q := (AssessDuePostsArgs{}).InsertOpts().Queue; q != config.QueueAnalysis {
		t.Errorf("queue = %q, want %q", q, config.QueueAnalysis)
	}
}

func TestPostQualitySweepRunsTheOgenTask(t *testing.T) {
	// ogen's MaintenanceService knows the sweep by this name
	// (queues.AssessDuePostsKind); any other name is refused.
	rpc := &fakeMaintenance{}
	w := &sweepWorker[AssessDuePostsArgs]{rpc: rpc}
	job := &river.Job[AssessDuePostsArgs]{JobRow: &rivertype.JobRow{Kind: (AssessDuePostsArgs{}).Kind()}}
	if err := w.Work(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if got := rpc.ran(); !slices.Equal(got, []string{"assess_due_posts"}) {
		t.Fatalf("ran %v", got)
	}
}
