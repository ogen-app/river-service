package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"google.golang.org/grpc"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
	"github.com/ogen-app/river-service/internal/logging"
	"github.com/ogen-app/river-service/internal/queue"
)

// maxSteps bounds one run of a step-run job, so a misbehaving flow can't loop
// forever. Ingestion of a long document takes a step per embedding batch.
const maxSteps = 512

// defaultStepTimeout bounds one attempt of a step-run job unless its kind sets
// its own.
const defaultStepTimeout = 60 * time.Second

// StepRunner is ogen's JobStepService.
type StepRunner interface {
	Step(ctx context.Context, in *jobrpcv1.StepRequest, opts ...grpc.CallOption) (*jobrpcv1.StepResponse, error)
}

// Actions carries out what ogen plans.
type Actions interface {
	HTTP(ctx context.Context, call *jobrpcv1.HttpCall) *jobrpcv1.StepResult
	Transfer(ctx context.Context, t *jobrpcv1.MediaTransfer) *jobrpcv1.StepResult
	Sidecar(ctx context.Context, call *jobrpcv1.SidecarCall) *jobrpcv1.StepResult
}

// stepDeps is what every step-run worker needs.
type stepDeps struct {
	ogen     StepRunner
	actions  Actions
	children *childKinds
}

// stepWorker runs any step-run job: it asks ogen what to do next, does it,
// and reports back, until ogen says done or retry. It carries no domain
// logic: ogen plans every outside call and decodes every result.
type stepWorker[A river.JobArgs] struct {
	river.WorkerDefaults[A]
	stepDeps
	timeout time.Duration
}

func newStepWorker[A river.JobArgs](d stepDeps, timeout time.Duration) *stepWorker[A] {
	return &stepWorker[A]{stepDeps: d, timeout: timeout}
}

func (w *stepWorker[A]) Timeout(*river.Job[A]) time.Duration {
	if w.timeout > 0 {
		return w.timeout
	}
	return defaultStepTimeout
}

func (w *stepWorker[A]) Work(ctx context.Context, job *river.Job[A]) error {
	args, err := json.Marshal(job.Args)
	if err != nil {
		return fmt.Errorf("encode %s args: %w", job.Kind, err)
	}
	var state []byte
	var result *jobrpcv1.StepResult
	for range maxSteps {
		resp, err := w.ogen.Step(ctx, &jobrpcv1.StepRequest{
			Kind: job.Kind, Args: args, State: state, Result: result,
			Attempt: int32(job.Attempt), MaxAttempts: int32(job.MaxAttempts),
		})
		if err != nil {
			return retryLater(err)
		}
		state = resp.GetState()
		switch next := resp.GetNext().(type) {
		case *jobrpcv1.StepResponse_Http:
			result = w.actions.HTTP(ctx, next.Http)
		case *jobrpcv1.StepResponse_Transfer:
			result = w.actions.Transfer(ctx, next.Transfer)
		case *jobrpcv1.StepResponse_Sidecar:
			result = w.actions.Sidecar(ctx, next.Sidecar)
		case *jobrpcv1.StepResponse_Wait:
			wait := next.Wait.AsDuration()
			if wait > maxInProcessWait {
				// Don't hold a worker through a long wait: snooze, and the job
				// restarts without state, as it does after a deploy.
				return river.JobSnooze(wait)
			}
			if err := sleep(ctx, wait); err != nil {
				return err
			}
			result = nil
		case *jobrpcv1.StepResponse_Retry:
			if after := next.Retry.GetAfter().AsDuration(); after > 0 {
				return river.JobSnooze(after)
			}
			return fmt.Errorf("%s: %s", job.Kind, next.Retry.GetReason())
		case *jobrpcv1.StepResponse_Done:
			return w.finish(ctx, job.Kind, next.Done)
		default:
			return fmt.Errorf("%s: ogen answered no next step", job.Kind)
		}
	}
	return fmt.Errorf("%s: no result after %d steps", job.Kind, maxSteps)
}

// maxInProcessWait is the longest Wait the loop sleeps through; a longer one
// is a snooze.
const maxInProcessWait = 30 * time.Second

// sleep waits d, or until ctx ends. A stop that cuts the wait leaves the job
// to the drain middleware, which snoozes it; the next run picks up the work
// ogen is still doing.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// finish enqueues the child jobs and cancels the queued jobs ogen asked for,
// then ends the job.
func (w *stepWorker[A]) finish(ctx context.Context, kind string, done *jobrpcv1.StepDone) error {
	cancelled := 0
	for _, m := range done.GetCancel() {
		n, err := cancelMatching(ctx, m)
		cancelled += n
		if err != nil {
			return fmt.Errorf("%s: cancel %s jobs: %w", kind, m.GetKind(), err)
		}
	}
	if len(done.GetEnqueue()) > 0 {
		// Children continue this job's trace and request, as a job enqueued
		// through JobService continues ogen's.
		md := queue.EncodeMetadata(ctx, logging.RequestID(ctx))
		params := make([]river.InsertManyParams, 0, len(done.GetEnqueue()))
		for _, c := range done.GetEnqueue() {
			args, err := w.children.args(c.GetKind(), c.GetArgs())
			if err != nil {
				return err
			}
			params = append(params, river.InsertManyParams{Args: args, InsertOpts: &river.InsertOpts{Metadata: md}})
		}
		client, err := river.ClientFromContextSafely[pgx.Tx](ctx)
		if err != nil {
			return fmt.Errorf("%s: enqueue children: %w", kind, err)
		}
		if _, err := client.InsertMany(ctx, params); err != nil {
			return fmt.Errorf("%s: enqueue children: %w", kind, err)
		}
	}
	slog.InfoContext(ctx, "job done", logging.AttrComponent, "jobs.steps",
		"job_kind", kind, "outcome", done.GetOutcome(), "children", len(done.GetEnqueue()), "cancelled", cancelled)
	return nil
}

// cancelLimit bounds the jobs one match cancels.
const cancelLimit = 100

// cancelMatching cancels the queued jobs m selects and returns how many River
// cancelled outright; a running one finishes its attempt.
func cancelMatching(ctx context.Context, m *jobrpcv1.JobMatch) (int, error) {
	client, err := river.ClientFromContextSafely[pgx.Tx](ctx)
	if err != nil {
		return 0, err
	}
	params := river.NewJobListParams().Kinds(m.GetKind()).States(liveStates...).First(cancelLimit)
	if len(m.GetArgs()) > 0 {
		where, named := matchSQL(m.GetArgs())
		params = params.Where(where, named)
	}
	res, err := client.JobList(ctx, params)
	if err != nil {
		return 0, err
	}
	cancelled := 0
	for _, job := range res.Jobs {
		row, err := client.JobCancel(ctx, job.ID)
		if errors.Is(err, rivertype.ErrNotFound) {
			continue
		}
		if err != nil {
			return cancelled, err
		}
		if row.State == rivertype.JobStateCancelled {
			cancelled++
		}
	}
	return cancelled, nil
}

// liveStates are the states a job can still be cancelled in.
var liveStates = []rivertype.JobState{
	rivertype.JobStateAvailable, rivertype.JobStateScheduled, rivertype.JobStateRetryable,
	rivertype.JobStatePending, rivertype.JobStateRunning,
}

// matchSQL turns arg conditions into a JobList filter. Keys and values are
// bound, never spliced in.
func matchSQL(args []*jobrpcv1.ArgMatch) (string, river.NamedArgs) {
	conds := make([]string, 0, len(args))
	named := river.NamedArgs{}
	for i, a := range args {
		k, v := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		conds = append(conds, fmt.Sprintf("COALESCE(args->>@%s, '') = ANY(@%s)", k, v))
		named[k], named[v] = a.GetKey(), a.GetValues()
	}
	return strings.Join(conds, " AND "), named
}

// childKinds builds the args of the child jobs ogen's sweeps fan out into.
type childKinds struct {
	build map[string]func([]byte) (river.JobArgs, error)
}

func newChildKinds() *childKinds {
	return &childKinds{build: map[string]func([]byte) (river.JobArgs, error){}}
}

func (c *childKinds) args(kind string, raw []byte) (river.JobArgs, error) {
	build, ok := c.build[kind]
	if !ok {
		return nil, fmt.Errorf("ogen asked for child job kind %q, which this river-service doesn't run", kind)
	}
	return build(raw)
}

func childOf[A river.JobArgs]() func([]byte) (river.JobArgs, error) {
	return func(raw []byte) (river.JobArgs, error) {
		var a A
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return a, nil
	}
}
