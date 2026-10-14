package queue

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// jobMetadata is what a job row carries about where it came from: the trace
// context of the request that enqueued it and that request's id.
type jobMetadata struct {
	Trace     map[string]string `json:"trace,omitempty"`
	RequestID string            `json:"request_id,omitempty"`
}

// EncodeMetadata returns River job metadata for a job enqueued under ctx: its
// trace context and the request id that caused it.
func EncodeMetadata(ctx context.Context, requestID string) []byte {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	md := jobMetadata{RequestID: requestID}
	if len(carrier) > 0 {
		md.Trace = carrier
	}
	if md.Trace == nil && md.RequestID == "" {
		return nil
	}
	b, err := json.Marshal(md)
	if err != nil {
		return nil
	}
	return b
}

// restoreMetadata returns ctx carrying the enqueuing request's trace context
// as the remote parent, and that request's id. Malformed or empty metadata
// leaves ctx as it is.
func restoreMetadata(ctx context.Context, raw []byte) (context.Context, string) {
	if len(raw) == 0 {
		return ctx, ""
	}
	var md jobMetadata
	if err := json.Unmarshal(raw, &md); err != nil {
		return ctx, ""
	}
	if len(md.Trace) > 0 {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(md.Trace))
	}
	return ctx, md.RequestID
}
