// Package logging sets up river-service's structured logger on log/slog and
// adds the request, job and trace ids to every line logged with a context.
package logging

import (
	"context"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// Attribute keys shared across the service.
const (
	AttrComponent = "component"
	AttrError     = "err"
	AttrRequestID = "request_id"
)

type requestIDKey struct{}

// WithRequestID returns ctx carrying the id of the request that caused the
// work, so log lines correlate with the ogen request.
func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID returns the request id on ctx, if any.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// New builds the logger for level and format ("json" or "text"), installs it
// as the slog default and routes the stdlib log package through it.
func New(level, format string) *slog.Logger {
	logger := slog.New(contextHandler{newBaseHandler(os.Stderr, level, format)})
	slog.SetDefault(logger)
	log.SetFlags(0)
	log.SetOutput(slog.NewLogLogger(logger.Handler(), slog.LevelInfo).Writer())
	return logger
}

func newBaseHandler(w io.Writer, level, format string) slog.Handler {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}
	if strings.EqualFold(strings.TrimSpace(format), "text") {
		return slog.NewTextHandler(w, opts)
	}
	return slog.NewJSONHandler(w, opts)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// contextHandler adds the request id and the active trace and span ids to each
// record logged with a context.
type contextHandler struct{ slog.Handler }

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestID(ctx); id != "" {
		r.AddAttrs(slog.String(AttrRequestID, id))
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}
