// Package grpcauth gates the gRPC server behind a shared bearer token, for
// unary and streaming calls alike. Health checks stay open so the platform can
// probe the service.
package grpcauth

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const healthPrefix = "/grpc.health.v1.Health/"

// Unary returns a unary interceptor that rejects calls without the token.
func Unary(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := check(ctx, info.FullMethod, token); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// Stream returns a stream interceptor that rejects calls without the token.
func Stream(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := check(ss.Context(), info.FullMethod, token); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func check(ctx context.Context, method, token string) error {
	if strings.HasPrefix(method, healthPrefix) {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		got, ok := strings.CutPrefix(v, "Bearer ")
		if ok && subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "missing or invalid bearer token")
}
