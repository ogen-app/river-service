package zernio

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
)

func TestDo(t *testing.T) {
	var got *http.Request
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"profile":{"_id":"p1"}}`))
	}))
	t.Cleanup(srv.Close)

	res := New(srv.URL+"/api/v1", time.Second).Do(t.Context(), "zk", &jobrpcv1.ZernioCall{
		Method: "POST", Path: "/profiles", Body: []byte(`{"name":"x"}`),
		Query:   []*jobrpcv1.QueryParam{{Key: "profileId", Value: "p1"}},
		Headers: map[string]string{"X-Connect-Token": "ct"},
	})
	if res.GetStatus() != 201 || string(res.GetBody()) != `{"profile":{"_id":"p1"}}` || res.GetError() != "" {
		t.Fatalf("result = %v", res)
	}
	if got.URL.Path != "/api/v1/profiles" || got.URL.Query().Get("profileId") != "p1" || got.Header.Get("Authorization") != "Bearer zk" ||
		got.Header.Get("X-Connect-Token") != "ct" || got.Header.Get("Content-Type") != "application/json" || body != `{"name":"x"}` {
		t.Errorf("request: %s %s %v body %s", got.Method, got.URL, got.Header, body)
	}

	if res := New("http://127.0.0.1:1", time.Second).Do(t.Context(), "zk", &jobrpcv1.ZernioCall{Method: "GET", Path: "/x"}); res.GetError() == "" || res.GetStatus() != 0 {
		t.Errorf("unreachable = %v, want an error and no status", res)
	}
}

func TestTransferStreamsTheFile(t *testing.T) {
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("video-bytes"))
	}))
	t.Cleanup(src.Close)
	var gotBody, gotType string
	var gotLen int64
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody, gotType, gotLen = string(b), r.Header.Get("Content-Type"), r.ContentLength
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dst.Close)

	res := New("http://unused", time.Second).Transfer(t.Context(), &jobrpcv1.MediaTransfer{
		SourceUrl: src.URL, UploadUrl: dst.URL, ContentType: "video/mp4", Size: 11,
	})
	if res.GetStatus() != 200 || gotBody != "video-bytes" || gotType != "video/mp4" || gotLen != 11 {
		t.Fatalf("result %v; uploaded %q (%s, %d bytes)", res, gotBody, gotType, gotLen)
	}

	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	t.Cleanup(missing.Close)
	if res := New("http://unused", time.Second).Transfer(t.Context(), &jobrpcv1.MediaTransfer{SourceUrl: missing.URL, UploadUrl: dst.URL}); res.GetError() == "" {
		t.Errorf("unreadable source = %v, want an error", res)
	}
}
