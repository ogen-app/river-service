package actions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
	"github.com/ogen-app/river-service/internal/logging"
)

// chunkSize is the size of each streamed frame, well under the sidecars'
// per-message limit.
const chunkSize = 1 << 20

// defaultSidecarTimeout bounds a sidecar call ogen gave no deadline.
const defaultSidecarTimeout = 2 * time.Minute

// Sidecar is a connection to one sidecar service.
type Sidecar struct {
	conn *grpc.ClientConn
}

// DialSidecar connects to a sidecar at addr. The connection is lazy, so this
// never waits on the sidecar. maxRecvBytes bounds a response (parsed
// documents and thumbnails can be large).
func DialSidecar(addr string, maxRecvBytes int) (*Sidecar, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvBytes)),
	)
	if err != nil {
		return nil, fmt.Errorf("dial sidecar %q: %w", addr, err)
	}
	return &Sidecar{conn: conn}, nil
}

// Close releases the connection.
func (s *Sidecar) Close() error { return s.conn.Close() }

// rawCodec passes already-encoded protobuf messages through untouched: ogen
// encodes each request and decodes each response with the sidecar's own
// contract. Named "proto" so sidecars accept it as ordinary protobuf.
type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(v any) ([]byte, error) {
	f, ok := v.(*frame)
	if !ok {
		return nil, fmt.Errorf("raw codec: unexpected %T", v)
	}
	return f.b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	f, ok := v.(*frame)
	if !ok {
		return fmt.Errorf("raw codec: unexpected %T", v)
	}
	f.b = append([]byte(nil), data...)
	return nil
}

type frame struct{ b []byte }

// Sidecar makes one planned sidecar call: unary, or client-streamed from a
// stored file. The result carries the gRPC status code, the response
// message and the status message.
func (e *Executor) Sidecar(ctx context.Context, call *jobrpcv1.SidecarCall) *jobrpcv1.StepResult {
	sc, ok := e.sidecars[call.GetService()]
	if !ok || sc == nil {
		// Unimplemented tells ogen the sidecar isn't configured here, which it
		// treats as the feature being off, as it does locally.
		return &jobrpcv1.StepResult{Status: 12, Error: fmt.Sprintf("the %s sidecar is not configured on river-service", call.GetService())}
	}
	timeout := call.GetTimeout().AsDuration()
	if timeout <= 0 {
		timeout = defaultSidecarTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if id := logging.RequestID(ctx); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", id)
	}

	var resp frame
	var err error
	if s := call.GetStream(); s != nil {
		err = e.stream(ctx, sc, call.GetMethod(), s, &resp)
	} else {
		err = sc.conn.Invoke(ctx, call.GetMethod(), &frame{b: call.GetRequest()}, &resp, grpc.ForceCodec(rawCodec{}))
	}
	if err != nil {
		st := status.Convert(err)
		return &jobrpcv1.StepResult{Status: int32(st.Code()), Error: st.Message()}
	}
	return &jobrpcv1.StepResult{Status: 0, Body: resp.b}
}

// stream sends the header frame, then the stored file in chunk frames, then
// reads the single response.
func (e *Executor) stream(ctx context.Context, sc *Sidecar, method string, src *jobrpcv1.StreamSource, resp *frame) error {
	file, err := openSource(ctx, e.transfer, src.GetSourceUrl())
	if err != nil {
		return status.Error(14, err.Error()) // Unavailable: the file couldn't be read, worth a retry
	}
	defer func() { _ = file.Close() }()

	cs, err := sc.conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true}, method, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return err
	}
	if err := cs.SendMsg(&frame{b: src.GetHeader()}); err != nil {
		return recvErr(cs, err)
	}
	field := protowire.Number(src.GetChunkField())
	buf := make([]byte, chunkSize)
	for {
		n, rerr := io.ReadFull(file, buf)
		if n > 0 {
			msg := protowire.AppendBytes(protowire.AppendTag(nil, field, protowire.BytesType), buf[:n])
			if err := cs.SendMsg(&frame{b: msg}); err != nil {
				return recvErr(cs, err)
			}
		}
		if errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF) {
			break
		}
		if rerr != nil {
			return status.Error(14, "read source: "+rerr.Error())
		}
	}
	if err := cs.CloseSend(); err != nil {
		return err
	}
	return cs.RecvMsg(resp)
}

// recvErr surfaces the server's status when a send fails because the server
// already answered (e.g. it rejected the header).
func recvErr(cs grpc.ClientStream, sendErr error) error {
	if !errors.Is(sendErr, io.EOF) {
		return sendErr
	}
	var discard frame
	if err := cs.RecvMsg(&discard); err != nil {
		return err
	}
	return sendErr
}
