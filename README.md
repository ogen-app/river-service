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

Job families move over from ogen one at a time (CON-332 phases 3–6) by
registering their kinds in `internal/jobs`. A kind this build does not register
is refused by `Enqueue` with `UNIMPLEMENTED`, and ogen keeps it in its outbox.

| Family | Kinds | State |
| --- | --- | --- |
| Housekeeping | `cleanup_post_logs`, `cleanup_notifications`, `cleanup_login_security`, `cleanup_plugin_pairings`, `sweep_pending_uploads` | Scheduled here; each pass runs in ogen via `MaintenanceService.Run` |
| Email | `send_email`, `cleanup_email_logs` | ogen prepares and renders (`EmailJobService.PrepareEmail`), river-service sends through Resend with a leased key, ogen records the outcome (`RecordEmailOutcome`). The log sweep runs in ogen. |
| Harbor webhook | `notify_harbor_tenant_registered` | Runs here; the payload is just the tenant id, signed with a key leased from ogen |
| Zernio lifecycle | `bootstrap_zernio_profile`, `teardown_zernio_profile`, `detect_expiring_connections` (fans out into `detect_expiring_connections_tenant`), `cleanup_zernio_connect_sessions` | Step-run jobs (below): Zernio calls go out from here with a key leased from ogen, and ogen plans each one and applies the result (`JobStepService.Step`). The connect-session sweep runs in ogen. |
| Publishing | `submit_post_to_zernio`, `poll_zernio_status`, `cancel_zernio_job`, `withdraw_zernio_post`, `post_first_comment` | Step-run jobs. Submit streams each attachment from storage to Zernio (presign, then a media transfer). Polls snooze themselves until the post is published or fails. Withdrawing a post cancels its pending polls. |
| Publishing sweeps | `reconcile_scheduled_posts`, `detect_manual_publish_due`, `sweep_zernio_orphans` (fans out into `sweep_zernio_orphans_profile`), `refresh_zernio_analytics` (into `refresh_zernio_analytics_tenant`), `refresh_zernio_followers` (into `refresh_zernio_followers_tenant`) | Reconcile and manual-publish-due run in ogen through `MaintenanceService.Run`. The others fan out into one step-run job per tenant profile. |

### How a step-run job runs

Jobs that call outside services run on one generic worker (`internal/jobs/steps.go`):
1. It asks ogen for the next step (`JobStepService.Step`).
2. ogen answers with one of five things:
   - an HTTP call, either to a named API (`zernio`, `gemini`, `firecrawl`, sent with that API's leased key) or to an absolute URL fetched with no credentials through the SSRF guard
   - a sidecar gRPC call, unary or client-streamed from a stored file
   - a media transfer from storage to an upload URL
   - done, optionally with child jobs to enqueue
   - retry, now or after a delay
3. The worker carries it out (`internal/actions`) and passes the raw result to the next `Step`. A call to an API whose key isn't set in ogen is not made; the result says `credential_missing`, and ogen decides what that means.

ogen builds every request and decodes every response, so this service holds no domain logic: a new step-run kind is an args type plus one `newStepWorker` registration. A job cut short (a deploy, a crash) retries from the start, and every flow converges on a repeat.

A workspace teardown holds a short lease in ogen. While it's held, a restore of that workspace is refused, and each step renews the lease, so a stalled teardown frees the workspace within two minutes.


ogen keeps scheduling a sweep until it is listed in ogen's `OUTBOX_RELAY_KINDS`,
so deploy river-service first, then list the family there.

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
| `OGEN_GRPC_ADDR` / `OGEN_RPC_AUTH_TOKEN` | empty | ogen's job-RPC listener (e.g. `dns:///ogen.railway.internal:9093`) and the token it expects (ogen's `RIVER_RPC_AUTH_TOKEN`). Without it no job kinds are registered. |
| `NOTIFICATIONS_CLEANUP_EVERY` / `PENDING_UPLOAD_SWEEP_INTERVAL` | `24h` / `1h` | Cadence of those two sweeps; `0` turns one off. Post logs run hourly, login security daily and plugin pairings every 15 minutes. |
| `ZERNIO_BASE_URL` / `ZERNIO_HTTP_TIMEOUT` | `https://zernio.com/api/v1` / `15s` | Zernio's API and per-call timeout. The key is leased from ogen; with none set, ogen ends Zernio jobs without doing anything. |
| `GEMINI_BASE_URL` / `GEMINI_HTTP_TIMEOUT` | `https://generativelanguage.googleapis.com` / `60s` | Gemini's API (embeddings) and per-call timeout. Key leased from ogen. |
| `FIRECRAWL_BASE_URL` / `FIRECRAWL_HTTP_TIMEOUT` | `https://api.firecrawl.dev` / `90s` | Firecrawl's API (URL scrapes) and per-call timeout. Key leased from ogen. |
| `FETCH_TIMEOUT` | `30s` | Fetch of outside content (an image on a scraped page). These fetches never reach private, shared (100.64.0.0/10), loopback or link-local addresses. |
| `PDF_SERVICE_ADDR` / `DOCUMENTS_SERVICE_ADDR` / `IMAGE_SERVICE_ADDR` / `AUDIO_SERVICE_ADDR` | empty | Sidecar gRPC addresses, set on the heavy service. A sidecar left unset behaves as it does in ogen without it. |
| `SIDECAR_MAX_RECV_BYTES` | `134217728` (128 MiB) | Largest sidecar response accepted |
| `ZERNIO_HEALTH_CHECK_INTERVAL` | `6h` | Connection-health sweep cadence; `0` turns it off. |
| `RECONCILE_EVERY` / `MANUAL_PUBLISH_DUE_SWEEP_EVERY` | `5m` / `1h` | Cadence of the two publishing sweeps that run in ogen; `0` turns one off. |
| `ZERNIO_ORPHAN_SWEEP_INTERVAL` / `ZERNIO_ANALYTICS_REFRESH_INTERVAL` / `ZERNIO_FOLLOWER_REFRESH_INTERVAL` | `1h` / `1h` / `24h` | Cadence of the Zernio sweeps; `0` turns one off. Whether the orphan sweep withdraws anything (`ZERNIO_ORPHAN_SWEEP_LIVE`) stays in ogen. |
| `EMAIL_BASE_URL` / `EMAIL_HTTP_TIMEOUT` | `https://api.resend.com` / `15s` | Resend endpoint and per-call timeout. The API key is leased from ogen; with no key set in ogen, sends are logged as skipped. |
| `HARBOR_WEBHOOK_URL` | empty | Harbor's new-tenant webhook. Empty turns it off. The HMAC key (`X-Ogen-Signature`) is leased from ogen's `HARBOR_WEBHOOK_SECRET`; with none set the webhook goes out unsigned. |
| `HEALTH_ADDR` | `:$PORT`, else `:8080` | HTTP `/healthz` (process up) and `/readyz` (serving and database reachable). Unset, it follows `PORT`, which is where Railway sends healthchecks. |
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
- `RAILWAY_DEPLOYMENT_OVERLAP_SECONDS=30` (about 30 seconds is enough), so
  the new replica is serving before the old one drains.
- Healthcheck path `/readyz`. Railway probes the port in `PORT`, and the
  health listener uses `PORT` when `HEALTH_ADDR` is unset, so leave
  `HEALTH_ADDR` unset on Railway. Restart policy: on failure.

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
river-service reaches ogen's job RPCs at
`dns:///ogen.railway.internal:9093` (river-service's `OGEN_GRPC_ADDR`, ogen's
`JOB_RPC_ADDR`). Define each token once as a Railway shared
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
