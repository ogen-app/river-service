package grpcauth

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type stream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s stream) Context() context.Context { return s.ctx }

func TestInterceptors(t *testing.T) {
	const token = "s3cret"
	withAuth := func(v string) context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", v))
	}
	tests := []struct {
		name   string
		ctx    context.Context
		method string
		want   codes.Code
	}{
		{name: "valid token", ctx: withAuth("Bearer s3cret"), method: "/jobs.v1.JobService/Enqueue", want: codes.OK},
		{name: "wrong token", ctx: withAuth("Bearer nope"), method: "/jobs.v1.JobService/Enqueue", want: codes.Unauthenticated},
		{name: "no scheme", ctx: withAuth("s3cret"), method: "/jobs.v1.JobService/Enqueue", want: codes.Unauthenticated},
		{name: "no metadata", ctx: context.Background(), method: "/jobs.v1.JobService/Enqueue", want: codes.Unauthenticated},
		{name: "health stays open", ctx: context.Background(), method: "/grpc.health.v1.Health/Check", want: codes.OK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Unary(token)(tt.ctx, nil, &grpc.UnaryServerInfo{FullMethod: tt.method},
				func(context.Context, any) (any, error) { return nil, nil })
			if got := status.Code(err); got != tt.want {
				t.Errorf("unary: code %v, want %v", got, tt.want)
			}
			err = Stream(token)(nil, stream{ctx: tt.ctx}, &grpc.StreamServerInfo{FullMethod: tt.method},
				func(any, grpc.ServerStream) error { return nil })
			if got := status.Code(err); got != tt.want {
				t.Errorf("stream: code %v, want %v", got, tt.want)
			}
		})
	}
}
