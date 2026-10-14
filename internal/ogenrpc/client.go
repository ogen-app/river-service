// Package ogenrpc is river-service's client for ogen's job RPCs (jobrpc.v1).
// Jobs reach ogen's data, storage and credentials only through it.
package ogenrpc

import (
	"context"
	"fmt"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
	"github.com/ogen-app/river-service/internal/logging"
)

// requestIDHeader is the metadata key ogen reads the request id from.
const requestIDHeader = "x-request-id"

// roundRobin spreads calls across every ogen replica the DNS name resolves to.
const roundRobin = `{"loadBalancingConfig":[{"round_robin":{}}]}`

// Client calls ogen's job RPCs. The connection is lazy, so building it never
// waits on ogen.
type Client struct {
	conn        *grpc.ClientConn
	Maintenance jobrpcv1.MaintenanceServiceClient
	Credentials jobrpcv1.CredentialServiceClient
	Email       jobrpcv1.EmailJobServiceClient
	Steps       jobrpcv1.JobStepServiceClient
}

// New dials ogen at addr with the bearer token on every call. Calls carry the
// trace context and the job's request id.
func New(addr, token string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(roundRobin),
		grpc.WithChainUnaryInterceptor(authUnary(token)),
		grpc.WithChainStreamInterceptor(authStream(token)),
	)
	if err != nil {
		return nil, fmt.Errorf("ogenrpc: dial %q: %w", addr, err)
	}
	return &Client{
		conn:        conn,
		Maintenance: jobrpcv1.NewMaintenanceServiceClient(conn),
		Credentials: jobrpcv1.NewCredentialServiceClient(conn),
		Email:       jobrpcv1.NewEmailJobServiceClient(conn),
		Steps:       jobrpcv1.NewJobStepServiceClient(conn),
	}, nil
}

// Close releases the connection. Safe on a nil client.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	return c.conn.Close()
}

// Transient reports whether err from an ogen RPC means "try again later"
// rather than "this failed": ogen is at its job-RPC limit, unreachable, slow,
// or does not have the RPC yet (a newer river-service ahead of ogen). A job
// snoozes on these instead of using an attempt.
func Transient(err error) bool {
	switch status.Code(err) {
	case codes.ResourceExhausted, codes.Unavailable, codes.Unimplemented, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func outgoing(ctx context.Context, token string) context.Context {
	kv := []string{"authorization", "Bearer " + token}
	if id := logging.RequestID(ctx); id != "" {
		kv = append(kv, requestIDHeader, id)
	}
	return metadata.AppendToOutgoingContext(ctx, kv...)
}

func authUnary(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(outgoing(ctx, token), method, req, reply, cc, opts...)
	}
}

func authStream(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(outgoing(ctx, token), desc, cc, method, opts...)
	}
}
