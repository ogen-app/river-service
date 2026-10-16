---
name: add-step-job
description: Add a job kind to river-service — the args type mirroring ogen's task, its registration (step-run worker, maintenance sweep or internal child), periodic schedule, config and tests. Use when a new background job lands in ogen and river-service has to run it.
tools: Read, Edit, Write, Glob, Grep, Bash
---

# Add Step Job Skill

river-service holds no domain logic. Every kind is one of:

| Shape | Worker | ogen side |
|---|---|---|
| **Step-run job** — calls something outside ogen | `newStepWorker[A](d, timeout)` (`internal/jobs/steps.go`) | the kind's steps in `JobStepService` |
| **Maintenance sweep** — touches only ogen's tables | `&sweepWorker[A]{rpc: maintenance}` | an entry in `MaintenanceTasks` |
| **Internal child** — fanned out by a sweep | `queue.RegisterInternal` + `d.children.build[kind]` | a step kind ogen emits in `StepDone.Enqueue` |

The ogen half is described by ogen's `add-river-job` skill. Do that first.

## Step 1: The args type

In the family's file under `internal/jobs/` (`publishing.go`,
`ingestion.go`, `analysis.go`, …):

```go
// <Kind>Args mirrors ogen's <Kind>Task field for field; ogen decodes the
// same JSON.
type <Kind>Args struct {
	PostID string `json:"post_id"`
}

func (<Kind>Args) Kind() string { return "<kind>" }

// InsertOpts match ogen's: queue, attempts and uniqueness.
func (<Kind>Args) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueDefault, MaxAttempts: 5}
}
```

- JSON names and `river:"unique"` tags must equal ogen's task exactly.
- Uniqueness over waiting work uses `waitingStates`; periodic markers use
  `sweepOpts()` (active states only, one attempt).
- The queue decides which Railway service runs it: `default`/`analysis` on
  `river-service`, `audio`/`image`/`ingest` on `river-service-heavy`.

## Step 2: Register it

```go
queue.Register(reg, newStepWorker[<Kind>Args](d, <timeout>), func(m *jobsv1.<Kind>Args) <Kind>Args {
	return <Kind>Args{PostID: m.GetPostId()}
})
```

`queue.Register` also teaches `JobService.Enqueue` to convert the proto
message. Periodic: `queue.RegisterPeriodic(reg, worker, cfg.<Cadence>, <Kind>Args{})`
with a cadence env var named as in ogen (zero turns it off). Call the family's
register function from `jobs.go`.

**Timeouts.** One attempt covers the whole step loop. A deploy cancels any
attempt still running at the soft stop (240 s, inside the 300 s drain), and
the job restarts without its step state. So keep each step short, and have
ogen record progress in its own rows after each one (submit records every
finished upload, audio every transcribed segment, ingestion its staged
chunks): the restarted attempt resumes from there instead of redoing the
work. The attempt timeout is only the cap for a call that hangs; it must stay
below `RESCUE_STUCK_JOBS_AFTER` (30 min). The default is 60 s.

## Step 3: Behaviour you get for free

- The loop asks ogen for each step, carries out HTTP/sidecar/transfer
  actions (`internal/actions`), sleeps on a `Wait` of up to 30 s (a longer
  one is a snooze), snoozes on a retry with a
  delay, and enqueues children and cancels matches on done.
- ogen busy (`RESOURCE_EXHAUSTED`) or without the kind yet (`UNIMPLEMENTED`)
  snoozes the job: no attempt used.
- Keys are leased from ogen per call; a key missing in ogen comes back as
  `credential_missing`, and ogen decides what that means.
- Tracing continues the enqueuing request's trace; a stop during a step
  snoozes the job so the next replica picks it up.

A new **upstream** (another API with a leased key) is an `actions.Upstream`
in `newActions` (`jobs.go`) plus its lease name in ogen's credential
allowlist. A new **sidecar** is an address in `config.go` and `newActions`.

## Step 4: Tests and docs

- Registration: extend `TestPublishingKindsRegister`-style counts in
  `steps_test.go`.
- Loop behaviour against a scripted ogen (`scriptedSteps`,
  `recordingActions`).
- DB tests (`TEST_DATABASE_DSN`, `make test-db`) for anything touching River.
- Add the kind to the README's family table and any new env var to the
  configuration table.

Run `make lint` and `go test -race ./...`.

## Deploy order

proto tag → ogen (steps or maintenance task) → river-service (this). Until
this service knows the kind, ogen's outbox holds its jobs.
