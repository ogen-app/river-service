package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"google.golang.org/grpc"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/credentials"
	"github.com/ogen-app/river-service/internal/logging"
	"github.com/ogen-app/river-service/internal/queue"
	"github.com/ogen-app/river-service/internal/zernio"
)

// maxZernioSteps bounds one run of a Zernio job, so a misbehaving flow can't
// loop forever.
const maxZernioSteps = 64

// zernioStepTimeout bounds one attempt of a short Zernio job: a handful of
// ogen steps and Zernio calls.
const zernioStepTimeout = 60 * time.Second

// ZernioSteps is the part of ogen's ZernioJobService a Zernio job calls.
type ZernioSteps interface {
	Step(ctx context.Context, in *jobrpcv1.StepRequest, opts ...grpc.CallOption) (*jobrpcv1.StepResponse, error)
}

// ZernioSender sends planned Zernio calls and media transfers.
type ZernioSender interface {
	Do(ctx context.Context, apiKey string, call *jobrpcv1.ZernioCall) *jobrpcv1.ZernioResult
	Transfer(ctx context.Context, t *jobrpcv1.MediaTransfer) *jobrpcv1.ZernioResult
}

// zernioWorker runs any Zernio job: it asks ogen what to do next, does it, and
// reports back, until ogen says done or retry. It carries no Zernio domain
// logic. With no Zernio key set in ogen the integration is off and the job
// ends quietly.
type zernioWorker[A river.JobArgs] struct {
	river.WorkerDefaults[A]
	ogen     ZernioSteps
	keys     Keys
	sender   ZernioSender
	children *childKinds
	timeout  time.Duration
}

func (w *zernioWorker[A]) Timeout(*river.Job[A]) time.Duration {
	if w.timeout > 0 {
		return w.timeout
	}
	return zernioStepTimeout
}

func (w *zernioWorker[A]) Work(ctx context.Context, job *river.Job[A]) error {
	key, err := w.keys.Get(ctx, credentials.Zernio)
	if err != nil {
		return retryLater(err)
	}
	if key == "" {
		slog.InfoContext(ctx, "zernio job skipped: no zernio key in ogen", logging.AttrComponent, "jobs.zernio", "job_kind", job.Kind)
		return nil
	}
	args, err := json.Marshal(job.Args)
	if err != nil {
		return fmt.Errorf("encode %s args: %w", job.Kind, err)
	}

	var state []byte
	var result *jobrpcv1.ZernioResult
	for range maxZernioSteps {
		resp, err := w.ogen.Step(ctx, &jobrpcv1.StepRequest{Kind: job.Kind, Args: args, State: state, Result: result})
		if err != nil {
			return retryLater(err)
		}
		state, result = resp.GetState(), nil
		switch next := resp.GetNext().(type) {
		case *jobrpcv1.StepResponse_Call:
			result = w.sender.Do(ctx, key, next.Call)
		case *jobrpcv1.StepResponse_Transfer:
			result = w.sender.Transfer(ctx, next.Transfer)
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
	return fmt.Errorf("%s: no result after %d steps", job.Kind, maxZernioSteps)
}

// finish enqueues the child jobs ogen asked for, then ends the job.
func (w *zernioWorker[A]) finish(ctx context.Context, kind string, done *jobrpcv1.ZernioStepDone) error {
	if len(done.GetEnqueue()) > 0 {
		params := make([]river.InsertManyParams, 0, len(done.GetEnqueue()))
		for _, c := range done.GetEnqueue() {
			args, err := w.children.args(c.GetKind(), c.GetArgs())
			if err != nil {
				return err
			}
			params = append(params, river.InsertManyParams{Args: args})
		}
		client, err := river.ClientFromContextSafely[pgx.Tx](ctx)
		if err != nil {
			return fmt.Errorf("%s: enqueue children: %w", kind, err)
		}
		if _, err := client.InsertMany(ctx, params); err != nil {
			return fmt.Errorf("%s: enqueue children: %w", kind, err)
		}
	}
	slog.InfoContext(ctx, "zernio job done", logging.AttrComponent, "jobs.zernio",
		"job_kind", kind, "outcome", done.GetOutcome(), "children", len(done.GetEnqueue()))
	return nil
}

// childKinds builds the args of the child jobs ogen's sweeps fan out into.
type childKinds struct {
	build map[string]func([]byte) (river.JobArgs, error)
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

func registerZernioLifecycle(reg *queue.Registry, cfg *config.Config, steps ZernioSteps, maintenance Maintenance, keys Keys, sender ZernioSender, children *childKinds) {
	worker := func() zernioDeps { return zernioDeps{ogen: steps, keys: keys, sender: sender, children: children} }
	queue.Register(reg, newZernioWorker[BootstrapZernioProfileArgs](worker()), func(m *jobsv1.BootstrapZernioProfileArgs) BootstrapZernioProfileArgs {
		return BootstrapZernioProfileArgs{TenantID: m.GetTenantId()}
	})
	queue.Register(reg, newZernioWorker[TeardownZernioProfileArgs](worker()), func(m *jobsv1.TeardownZernioProfileArgs) TeardownZernioProfileArgs {
		return TeardownZernioProfileArgs{TenantID: m.GetTenantId()}
	})
	queue.RegisterPeriodic(reg, newZernioWorker[DetectExpiringConnectionsArgs](worker()), cfg.ZernioHealthCheckInterval, DetectExpiringConnectionsArgs{})
	queue.RegisterInternal(reg, newZernioWorker[ExpiringConnectionsTenantArgs](worker()))
	children.build[ExpiringConnectionsTenantArgs{}.Kind()] = childOf[ExpiringConnectionsTenantArgs]()
	queue.RegisterPeriodic(reg, &sweepWorker[CleanupZernioConnectSessionsArgs]{rpc: maintenance}, connectSessionCleanupEvery, CleanupZernioConnectSessionsArgs{})
}

type zernioDeps struct {
	ogen     ZernioSteps
	keys     Keys
	sender   ZernioSender
	children *childKinds
}

func newZernioWorker[A river.JobArgs](d zernioDeps) *zernioWorker[A] {
	return &zernioWorker[A]{ogen: d.ogen, keys: d.keys, sender: d.sender, children: d.children}
}

// newZernioSender builds the sender for cfg.
func newZernioSender(cfg *config.Config) *zernio.Client {
	return zernio.New(cfg.ZernioBaseURL, cfg.ZernioHTTPTimeout)
}
