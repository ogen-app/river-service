# river-service

Runs Ogen's background jobs. It owns the [River](https://riverqueue.com) queue,
takes jobs from ogen over `jobs.v1.JobService`, and works the queues listed in
`RIVER_QUEUES`. It is internal-only: ogen reaches it over Railway's private
network.

```
ogen request ─▶ tx { business write + job_outbox row } ─▶ outbox relay
                                                            │ jobs.v1.JobService.Enqueue (gRPC)
                                                            ▼
                         river-service ─▶ River (schema river_service) ─▶ workers
                                                            │
                                  ogen job RPCs ◀───────────┘ (reads, writes, SSE, notifications)
```

ogen stays the system of record. river-service has no access to ogen's tables
or object storage: its database role owns only the `river_service` schema, and
everything a job reads or writes goes through ogen's internal job RPCs. The
design is in Linear CON-332; the deployment configuration is CON-377.

## Status

This is the walking skeleton (CON-332 phase 2.1). It migrates its schema,
serves `JobService`, drains on shutdown and traces jobs, but **no job kinds are
registered yet**: `Enqueue` answers `UNIMPLEMENTED` for every kind, and ogen keeps
those jobs in its outbox. Job families move over one at a time (phases 3–6) by
registering their kinds in `internal/jobs`.

## How it works

- **Enqueue.** ogen's relay calls `Enqueue` with the outbox row id as
  `idempotency_key`. The job and its key are written in one transaction, so a
  retried delivery returns the first job with `duplicate` set. A kind this build
  does not register (or one added to the contract after it was built) is refused
  with `UNIMPLEMENTED`, and the relay retries later.
- **Shutdown.** On SIGTERM the service stops answering `/readyz`, stops taking
  `Enqueue` calls, then stops River: running jobs get `SOFT_STOP_TIMEOUT` to
  finish, the rest are cancelled and rescheduled with a 5s snooze (no attempt
  used) so the replacement replica resumes them, and the stop gives up at
  `SHUTDOWN_TIMEOUT`. Telemetry is flushed last.
- **Scaling.** Replicas share the queue: River fetches with `SKIP LOCKED`, and
  only the elected leader schedules periodic sweeps. Replicas hold no state, so
  scaling is a change to the replica count.
- **Tracing.** Each job attempt's span is a child of the span that enqueued it
  (restored from the job's metadata) and links to it, so one trace id covers the
  ogen request, the job and the calls the job makes. Periodic sweeps start their
  own trace. Failures go to Sentry once retries are exhausted.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_DSN` | required | ogen's Postgres cluster, logging in as the `river_service` role |
| `DB_MAX_CONNS` | `10` | Connection pool size per replica. Keep replicas × this within the role's `CONNECTION LIMIT`. |
| `RIVER_SCHEMA` | `river_service` | Schema holding River's tables and the enqueue keys. Must exist and belong to the role. |
| `RIVER_QUEUES` | `default` | Queues this service works: `default`, `analysis`, `audio`, `image`, `ingest` |
| `JOB_WORKERS` / `ANALYSIS_JOB_WORKERS` / `AUDIO_JOB_WORKERS` / `IMAGE_JOB_WORKERS` / `INGEST_JOB_WORKERS` | `4` / `4` / `2` / `2` / `2` | Workers per queue on each replica |
| `SOFT_STOP_TIMEOUT` | `240s` | Time running jobs get to finish on shutdown before they are cancelled and rescheduled. Kept at least 15s below `SHUTDOWN_TIMEOUT`. |
| `SHUTDOWN_TIMEOUT` | `270s` | When the stop gives up. Must be below `RAILWAY_DEPLOYMENT_DRAINING_SECONDS`. |
| `RESCUE_STUCK_JOBS_AFTER` | `30m` | When a job left running by a dead replica is retried. Must exceed the longest attempt timeout. |
| `GRPC_ADDR` | `:9092` | Where `JobService` listens. Empty disables it (heavy service). |
| `GRPC_AUTH_TOKEN` | required with `GRPC_ADDR` | Bearer token ogen sends (ogen's `RIVER_SERVICE_AUTH_TOKEN`) |
| `HEALTH_ADDR` | `:8080` | HTTP `/healthz` (process up) and `/readyz` (serving and database reachable) |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `json` | Logging; `LOG_FORMAT=text` for local runs |
| `SENTRY_DSN` | empty | Turns on tracing and error reporting. Also `SENTRY_ENVIRONMENT`, `SENTRY_RELEASE`, `SENTRY_TRACES_SAMPLE_RATE` (`0.1`), `OTEL_SERVICE_NAME` (`river-service`). |

river-service never holds API keys, storage credentials, model ids or ogen's
KEK. Job families that need a vendor key lease it from ogen.

## Deploying on Railway

Two services run the same image:

| Service | `RIVER_QUEUES` | `GRPC_ADDR` | Public domain |
| --- | --- | --- | --- |
| `river-service` | `default,analysis` | `:9092` | none (private network only) |
| `river-service-heavy` | `audio,image,ingest` | empty | none |

Both share one River schema, so the light service's `JobService` enqueues jobs
for either service's queues. Periodic sweeps are scheduled by whichever replica
is leader.

**Deploy settings, on both services:**

- `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=300`. Railway sends SIGTERM once the new
  deployment is up and SIGKILL after this many seconds (default 0, which kills
  running jobs). It must exceed `SHUTDOWN_TIMEOUT`.
- `RAILWAY_DEPLOYMENT_OVERLAP_SECONDS≈30`, so the new replica is serving
  before the old one drains.
- Healthcheck path `/readyz` on `HEALTH_ADDR`; restart policy on failure.

**Postgres.** Create the role and schema once, as a superuser on ogen's
database:

```sql
CREATE ROLE river_service LOGIN PASSWORD '…' CONNECTION LIMIT 20;
CREATE SCHEMA river_service AUTHORIZATION river_service;
```

Grant the role nothing on ogen's schema. The service creates its tables inside
`river_service` on boot, one replica at a time under an advisory lock.

**Private network.** ogen reaches `JobService` at
`dns:///river-service.railway.internal:9092` (ogen's `RIVER_SERVICE_ADDR`), and
river-service will reach ogen's job RPCs at
`dns:///ogen.railway.internal:9091`. Define each token once as a Railway shared
variable and reference it from both services.

**Scaling.** Change the replica count. Watch queue depth and the age of the
oldest available job per queue, ogen's outbox relay lag, and ogen's job-RPC
limiter rejections; keep replicas × workers within ogen's limiter and vendor
quotas.

**Deploy order** for each job family: ogen ships its new job RPCs, then
river-service ships the workers, then ogen adds the kinds to
`OUTBOX_RELAY_KINDS`. A slip only delays jobs: unknown kinds stay in ogen's
outbox, and a job whose ogen RPC is missing snoozes.

**Rollback.** Roll back river-service to the previous image. Jobs of a kind the
older build doesn't know are refused at `Enqueue` and wait in ogen's outbox;
remove the kind from `OUTBOX_RELAY_KINDS` to route it back to ogen's in-process
queue while that path still exists.

## Development

```sh
make build        # binary
make test         # unit tests (database tests skip without TEST_DATABASE_DSN)
make test-db      # all tests against a throwaway Postgres container
make lint         # gofmt + go vet
make proto        # regenerate gen/ from the pinned contract
make proto PROTO_SOURCE=../proto/proto   # from a local proto checkout
```

Generated stubs in `gen/` are committed. `jobs.v1` is pinned to
`buf.build/ogen-app/proto` `PROTO_VERSION` in the Makefile.
