package actions

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
)

type keys map[string]string

func (k keys) Get(_ context.Context, name string) (string, error) { return k[name], nil }

func executor(baseURL string, k keys) *Executor {
	return New(Options{
		Keys: k,
		Upstreams: map[string]Upstream{
			"zernio": {BaseURL: baseURL + "/api/v1", Credential: "zernio_api_key", Header: "Authorization", Prefix: "Bearer ", Timeout: time.Second},
			"gemini": {BaseURL: baseURL, Credential: "gemini_api_key", Header: "x-goog-api-key"},
		},
		FetchTimeout: time.Second,
	})
}

func TestHTTPToANamedUpstream(t *testing.T) {
	var got *http.Request
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":1}`))
	}))
	t.Cleanup(srv.Close)

	res := executor(srv.URL, keys{"zernio_api_key": "zk"}).HTTP(t.Context(), &jobrpcv1.HttpCall{
		Upstream: "zernio", Method: "POST", Path: "/profiles", Body: []byte(`{"name":"x"}`),
		Query:   []*jobrpcv1.QueryParam{{Key: "profileId", Value: "p1"}},
		Headers: map[string]string{"X-Connect-Token": "ct"},
	})
	if res.GetStatus() != 201 || string(res.GetBody()) != `{"ok":1}` || res.GetError() != "" || res.GetContentType() != "application/json" {
		t.Fatalf("result = %v", res)
	}
	if got.URL.Path != "/api/v1/profiles" || got.URL.Query().Get("profileId") != "p1" || got.Header.Get("Authorization") != "Bearer zk" ||
		got.Header.Get("X-Connect-Token") != "ct" || body != `{"name":"x"}` {
		t.Errorf("request: %s %s %v body %s", got.Method, got.URL, got.Header, body)
	}

	res = executor(srv.URL, keys{"gemini_api_key": "gk"}).HTTP(t.Context(), &jobrpcv1.HttpCall{Upstream: "gemini", Method: "POST", Path: "/v1beta/models/m:batchEmbedContents"})
	if res.GetStatus() != 201 || got.Header.Get("x-goog-api-key") != "gk" {
		t.Errorf("gemini key header = %q", got.Header.Get("x-goog-api-key"))
	}
}

func TestHTTPWithNoKeyInOgen(t *testing.T) {
	res := executor("http://unused", keys{}).HTTP(t.Context(), &jobrpcv1.HttpCall{Upstream: "zernio", Method: "GET", Path: "/x"})
	if !res.GetCredentialMissing() || res.GetStatus() != 0 {
		t.Errorf("no key = %v, want credential_missing and no call", res)
	}
	if res := executor("http://unused", keys{}).HTTP(t.Context(), &jobrpcv1.HttpCall{Upstream: "mars"}); res.GetError() == "" {
		t.Errorf("unknown upstream = %v, want an error", res)
	}
}

func TestFetchRefusesInternalAddresses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("secret")) }))
	t.Cleanup(srv.Close)
	res := executor("http://unused", keys{}).HTTP(t.Context(), &jobrpcv1.HttpCall{Url: srv.URL})
	if res.GetStatus() != 0 || res.GetError() == "" || len(res.GetBody()) != 0 {
		t.Fatalf("fetch of a loopback URL = %v, want it refused", res)
	}
	if res := executor("http://unused", keys{}).HTTP(t.Context(), &jobrpcv1.HttpCall{Url: "file:///etc/passwd"}); res.GetError() == "" {
		t.Errorf("non-http URL = %v, want refused", res)
	}
	for _, ip := range []string{"127.0.0.1", "10.0.0.5", "100.64.1.1", "169.254.169.254", "::1", "fd00:ec2::254", "192.168.1.1"} {
		if !blocked(net.ParseIP(ip)) {
			t.Errorf("%s not blocked", ip)
		}
	}
	if blocked(net.ParseIP("93.184.216.34")) {
		t.Error("a public address is blocked")
	}
}

func TestTransferStreamsTheFile(t *testing.T) {
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("video-bytes")) }))
	t.Cleanup(src.Close)
	var gotBody, gotType string
	var gotLen int64
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody, gotType, gotLen = string(b), r.Header.Get("Content-Type"), r.ContentLength
	}))
	t.Cleanup(dst.Close)

	res := executor("http://unused", keys{}).Transfer(t.Context(), &jobrpcv1.MediaTransfer{SourceUrl: src.URL, UploadUrl: dst.URL, ContentType: "video/mp4", Size: 11})
	if res.GetStatus() != 200 || gotBody != "video-bytes" || gotType != "video/mp4" || gotLen != 11 {
		t.Fatalf("result %v; uploaded %q (%s, %d bytes)", res, gotBody, gotType, gotLen)
	}
	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	t.Cleanup(missing.Close)
	if res := executor("http://unused", keys{}).Transfer(t.Context(), &jobrpcv1.MediaTransfer{SourceUrl: missing.URL, UploadUrl: dst.URL}); res.GetError() == "" {
		t.Errorf("unreadable source = %v, want an error", res)
	}
}
