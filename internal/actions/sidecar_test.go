package actions

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/durationpb"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
)

// rawSidecar is a sidecar that sees exactly the bytes river-service sends:
// Echo answers a unary call with its request reversed, and Upload collects a
// client stream.
type rawSidecar struct {
	frames [][]byte
}

func (s *rawSidecar) desc() *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: "test.v1.Sidecar",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Echo",
			Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				var in frame
				if err := dec(&in); err != nil {
					return nil, err
				}
				if string(in.b) == "fail" {
					return nil, status.Error(codes.InvalidArgument, "unsupported file")
				}
				out := bytes.Clone(in.b)
				for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
					out[i], out[j] = out[j], out[i]
				}
				return &frame{b: out}, nil
			},
		}},
		Streams: []grpc.StreamDesc{{
			StreamName:    "Upload",
			ClientStreams: true,
			Handler: func(_ any, ss grpc.ServerStream) error {
				for {
					var f frame
					err := ss.RecvMsg(&f)
					if errors.Is(err, io.EOF) {
						return ss.SendMsg(&frame{b: []byte("ok")})
					}
					if err != nil {
						return err
					}
					s.frames = append(s.frames, f.b)
				}
			},
		}},
	}
}

func startSidecar(t *testing.T, s *rawSidecar) *Sidecar {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}))
	srv.RegisterService(s.desc(), s)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	sc, err := DialSidecar(lis.Addr().String(), 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	return sc
}

func TestSidecarUnary(t *testing.T) {
	e := New(Options{Sidecars: map[string]*Sidecar{"pdf": startSidecar(t, &rawSidecar{})}})
	res := e.Sidecar(t.Context(), &jobrpcv1.SidecarCall{Service: "pdf", Method: "/test.v1.Sidecar/Echo", Request: []byte("abc"), Timeout: durationpb.New(time.Second)})
	if res.GetStatus() != 0 || string(res.GetBody()) != "cba" {
		t.Fatalf("result = %v", res)
	}
	res = e.Sidecar(t.Context(), &jobrpcv1.SidecarCall{Service: "pdf", Method: "/test.v1.Sidecar/Echo", Request: []byte("fail")})
	if res.GetStatus() != int32(codes.InvalidArgument) || res.GetError() != "unsupported file" {
		t.Errorf("rejected call = %v", res)
	}
	if res := e.Sidecar(t.Context(), &jobrpcv1.SidecarCall{Service: "audio"}); res.GetStatus() != int32(codes.Unimplemented) {
		t.Errorf("unconfigured sidecar = %v, want Unimplemented", res)
	}
}

func TestSidecarStreamsAStoredFile(t *testing.T) {
	file := strings.Repeat("x", chunkSize+10) // two chunk frames
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(file)) }))
	t.Cleanup(src.Close)
	side := &rawSidecar{}
	e := New(Options{Sidecars: map[string]*Sidecar{"pdf": startSidecar(t, side)}})

	header := []byte{0x0a, 0x02, 'h', 'i'}
	res := e.Sidecar(t.Context(), &jobrpcv1.SidecarCall{Service: "pdf", Method: "/test.v1.Sidecar/Upload", Stream: &jobrpcv1.StreamSource{
		SourceUrl: src.URL, Header: header, ChunkField: 2,
	}})
	if res.GetStatus() != 0 || string(res.GetBody()) != "ok" {
		t.Fatalf("result = %v", res)
	}
	if len(side.frames) != 3 || !bytes.Equal(side.frames[0], header) {
		t.Fatalf("frames = %d, want header plus two chunks", len(side.frames))
	}
	var got []byte
	for _, f := range side.frames[1:] {
		num, typ, n := protowire.ConsumeTag(f)
		if num != 2 || typ != protowire.BytesType {
			t.Fatalf("chunk frame tag = %d/%d", num, typ)
		}
		b, _ := protowire.ConsumeBytes(f[n:])
		got = append(got, b...)
	}
	if string(got) != file {
		t.Errorf("reassembled %d bytes, want %d", len(got), len(file))
	}
}
