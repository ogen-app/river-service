// Package telemetry wires OpenTelemetry tracing and Sentry error reporting.
// Spans go to Sentry over OTLP, the same way ogen sends them, so a job's spans
// join the trace of the ogen request that enqueued it.
package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	sentry "github.com/getsentry/sentry-go"
	sentryotel "github.com/getsentry/sentry-go/otel"
	sentryotlp "github.com/getsentry/sentry-go/otel/otlp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/logging"
)

const flushTimeout = 2 * time.Second

// Propagator carries W3C trace context and baggage, which is what ogen's
// gRPC clients send.
func Propagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}

// Init turns tracing and error reporting on when SENTRY_DSN is set and returns
// a shutdown that flushes both. The propagator is installed either way, so job
// metadata keeps carrying trace context with telemetry off. A failure leaves
// telemetry off and never blocks boot.
func Init(ctx context.Context, cfg *config.Config) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(Propagator())
	noop := func(context.Context) error { return nil }
	if cfg.SentryDSN == "" {
		return noop, nil
	}

	if err := sentry.Init(sentry.ClientOptions{
		Dsn:         cfg.SentryDSN,
		Environment: cfg.SentryEnvironment,
		Release:     cfg.SentryRelease,
		// Spans come from OpenTelemetry; Sentry's own tracing stays off.
		EnableTracing: false,
		Integrations: func(in []sentry.Integration) []sentry.Integration {
			return append(in, sentryotel.NewOtelIntegration())
		},
	}); err != nil {
		return noop, err
	}

	exporter, err := sentryotlp.NewTraceExporter(ctx, cfg.SentryDSN)
	if err != nil {
		sentry.Flush(flushTimeout)
		return noop, err
	}

	res, rerr := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", cfg.OTelServiceName),
		attribute.String("service.version", cfg.SentryRelease),
		attribute.String("deployment.environment", cfg.SentryEnvironment),
	))
	if rerr != nil {
		slog.WarnContext(ctx, "telemetry: partial otel resource", logging.AttrComponent, "telemetry", logging.AttrError, rerr)
	}

	// Parent-based: a job continues ogen's sampling decision, so a kept trace
	// keeps its job spans and a dropped one drops them.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SentrySampleRate))),
	)
	otel.SetTracerProvider(tp)

	// Outbound HTTP (Zernio, Resend, Gemini, Firecrawl) gets client spans and
	// carries the trace onward.
	http.DefaultTransport = otelhttp.NewTransport(http.DefaultTransport)

	slog.InfoContext(ctx, "telemetry enabled", logging.AttrComponent, "telemetry",
		"service", cfg.OTelServiceName, "environment", cfg.SentryEnvironment, "sample_rate", cfg.SentrySampleRate)

	return func(ctx context.Context) error {
		err := tp.Shutdown(ctx)
		if !sentry.Flush(flushTimeout) && err == nil {
			return errors.New("sentry flush timed out")
		}
		return err
	}, nil
}

// CaptureError reports err to Sentry, linked to the trace on ctx. It is a
// no-op while telemetry is off.
func CaptureError(ctx context.Context, err error, attrs ...attribute.KeyValue) {
	if err == nil {
		return
	}
	hub := sentry.GetHubFromContext(ctx)
	if hub == nil {
		hub = sentry.CurrentHub()
	}
	client, scope := hub.Client(), hub.Scope()
	if client == nil {
		return
	}
	s := scope.Clone()
	if len(attrs) > 0 {
		c := make(sentry.Context, len(attrs))
		for _, a := range attrs {
			c[string(a.Key)] = a.Value.AsInterface()
		}
		s.SetContext("job", c)
	}
	client.CaptureException(err, &sentry.EventHint{OriginalException: err, Context: ctx}, s)
}
