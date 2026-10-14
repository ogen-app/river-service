// Package config loads river-service settings from the environment. Each field
// names its own variable with no global prefix, matching ogen and the sidecar
// services.
package config

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Queue names. Every job kind inserts into one of these, and a service works
// only the queues listed in RIVER_QUEUES.
const (
	QueueDefault  = "default"
	QueueAnalysis = "analysis"
	QueueAudio    = "audio"
	QueueImage    = "image"
	QueueIngest   = "ingest"
)

// knownQueues are the queues a service may be asked to work.
var knownQueues = []string{QueueDefault, QueueAnalysis, QueueAudio, QueueImage, QueueIngest}

// minCancelMargin is the least time the shutdown leaves between River
// cancelling running jobs and giving up on the stop, so cancelled jobs can
// record their reschedule before the process exits.
const minCancelMargin = 15 * time.Second

// Config holds the service settings.
type Config struct {
	// DatabaseDSN connects as the river_service role, which owns RiverSchema and
	// has no grants on ogen's tables.
	DatabaseDSN string `envconfig:"DATABASE_DSN" required:"true"`
	// DBMaxConns caps this replica's connection pool. Keep replicas × DBMaxConns
	// within the role's CONNECTION LIMIT.
	DBMaxConns int32 `envconfig:"DB_MAX_CONNS" default:"10"`
	// RiverSchema holds River's tables and the enqueue keys.
	RiverSchema string `envconfig:"RIVER_SCHEMA" default:"river_service"`

	// RiverQueues lists the queues this service works, e.g. "default,analysis"
	// on the light service and "audio,image,ingest" on the heavy one.
	RiverQueues []string `envconfig:"RIVER_QUEUES" default:"default"`
	// Per-queue workers on each replica. A queue not in RiverQueues ignores its
	// setting.
	DefaultWorkers  int `envconfig:"JOB_WORKERS" default:"4"`
	AnalysisWorkers int `envconfig:"ANALYSIS_JOB_WORKERS" default:"4"`
	AudioWorkers    int `envconfig:"AUDIO_JOB_WORKERS" default:"2"`
	ImageWorkers    int `envconfig:"IMAGE_JOB_WORKERS" default:"2"`
	IngestWorkers   int `envconfig:"INGEST_JOB_WORKERS" default:"2"`

	// On SIGTERM running jobs get SoftStopTimeout to finish; any still running
	// are then cancelled and rescheduled, and the stop gives up at
	// ShutdownTimeout. Keep ShutdownTimeout below Railway's
	// RAILWAY_DEPLOYMENT_DRAINING_SECONDS.
	SoftStopTimeout time.Duration `envconfig:"SOFT_STOP_TIMEOUT" default:"240s"`
	ShutdownTimeout time.Duration `envconfig:"SHUTDOWN_TIMEOUT" default:"270s"`
	// RescueStuckJobsAfter is how long a job may stay running before River
	// treats it as abandoned by a dead replica. It must exceed the longest
	// attempt timeout of any job.
	RescueStuckJobsAfter time.Duration `envconfig:"RESCUE_STUCK_JOBS_AFTER" default:"30m"`

	// GRPCAddr is where JobService listens. Empty disables it, as on the heavy
	// service, which only works queues.
	GRPCAddr string `envconfig:"GRPC_ADDR" default:":9092"`
	// GRPCAuthToken is the bearer token callers of JobService present (ogen's
	// RIVER_SERVICE_AUTH_TOKEN). Required when GRPCAddr is set.
	GRPCAuthToken string `envconfig:"GRPC_AUTH_TOKEN"`

	// OgenGRPCAddr is ogen's job-RPC listener, e.g.
	// dns:///ogen.railway.internal:9093. Jobs read and write ogen data only
	// through it. OgenRPCAuthToken is the bearer token it expects (ogen's
	// RIVER_RPC_AUTH_TOKEN). Required once any job kind is registered.
	OgenGRPCAddr     string `envconfig:"OGEN_GRPC_ADDR"`
	OgenRPCAuthToken string `envconfig:"OGEN_RPC_AUTH_TOKEN"`

	// Housekeeping sweep cadences, as ogen had them. Zero turns a sweep off.
	NotificationsCleanupEvery  time.Duration `envconfig:"NOTIFICATIONS_CLEANUP_EVERY" default:"24h"`
	PendingUploadSweepInterval time.Duration `envconfig:"PENDING_UPLOAD_SWEEP_INTERVAL" default:"1h"`

	// Resend endpoint and per-call timeout for send_email. The API key is
	// leased from ogen.
	EmailBaseURL     string        `envconfig:"EMAIL_BASE_URL" default:"https://api.resend.com"`
	EmailHTTPTimeout time.Duration `envconfig:"EMAIL_HTTP_TIMEOUT" default:"15s"`

	// Harbor's new-tenant webhook. An empty URL turns the notification off. Its
	// signing key is leased from ogen (harbor_webhook_secret).
	HarborWebhookURL string `envconfig:"HARBOR_WEBHOOK_URL"`

	// HealthAddr serves the HTTP liveness and readiness checks Railway probes.
	// Unset, it follows PORT (where Railway sends healthchecks), else :8080.
	HealthAddr string `envconfig:"HEALTH_ADDR"`
	Port       string `envconfig:"PORT"`

	LogLevel  string `envconfig:"LOG_LEVEL" default:"info"`
	LogFormat string `envconfig:"LOG_FORMAT" default:"json"`

	// Telemetry is off while SentryDSN is empty.
	SentryDSN         string  `envconfig:"SENTRY_DSN"`
	SentryEnvironment string  `envconfig:"SENTRY_ENVIRONMENT" default:"development"`
	SentryRelease     string  `envconfig:"SENTRY_RELEASE"`
	SentrySampleRate  float64 `envconfig:"SENTRY_TRACES_SAMPLE_RATE" default:"0.1"`
	OTelServiceName   string  `envconfig:"OTEL_SERVICE_NAME" default:"river-service"`
}

// Load reads and validates the environment.
func Load() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, err
	}
	cfg.RiverQueues = normalizeQueues(cfg.RiverQueues)
	if cfg.HealthAddr == "" {
		cfg.HealthAddr = ":" + cmp.Or(strings.TrimSpace(cfg.Port), "8080")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func normalizeQueues(in []string) []string {
	var out []string
	for _, q := range in {
		q = strings.ToLower(strings.TrimSpace(q))
		if q != "" && !slices.Contains(out, q) {
			out = append(out, q)
		}
	}
	return out
}

func (c *Config) validate() error {
	var errs []error
	for _, q := range c.RiverQueues {
		if !slices.Contains(knownQueues, q) {
			errs = append(errs, fmt.Errorf("RIVER_QUEUES: unknown queue %q (known: %s)", q, strings.Join(knownQueues, ", ")))
		}
	}
	if c.GRPCAddr != "" && c.GRPCAuthToken == "" {
		errs = append(errs, errors.New("GRPC_AUTH_TOKEN is required when GRPC_ADDR is set"))
	}
	if c.ShutdownTimeout <= minCancelMargin {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT must exceed %s", minCancelMargin))
	}
	if c.OgenGRPCAddr != "" && c.OgenRPCAuthToken == "" {
		errs = append(errs, errors.New("OGEN_RPC_AUTH_TOKEN is required when OGEN_GRPC_ADDR is set"))
	}
	if c.RiverSchema == "" {
		errs = append(errs, errors.New("RIVER_SCHEMA must not be empty"))
	}
	return errors.Join(errs...)
}

// EffectiveSoftStop is how long River lets running jobs finish before
// cancelling them, kept at least minCancelMargin below ShutdownTimeout so the
// cancellation happens while the stop is still being awaited.
func (c *Config) EffectiveSoftStop() time.Duration {
	ceiling := c.ShutdownTimeout - minCancelMargin
	if c.SoftStopTimeout <= 0 || c.SoftStopTimeout > ceiling {
		return ceiling
	}
	return c.SoftStopTimeout
}

// QueueWorkers returns the worker count of each queue this service works.
func (c *Config) QueueWorkers() map[string]int {
	all := map[string]int{
		QueueDefault:  c.DefaultWorkers,
		QueueAnalysis: c.AnalysisWorkers,
		QueueAudio:    c.AudioWorkers,
		QueueImage:    c.ImageWorkers,
		QueueIngest:   c.IngestWorkers,
	}
	out := make(map[string]int, len(c.RiverQueues))
	for _, q := range c.RiverQueues {
		out[q] = max(all[q], 1)
	}
	return out
}
