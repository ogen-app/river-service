package resend

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSend(t *testing.T) {
	var gotAuth, gotIdem string
	var gotBody sendRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotIdem = r.Header.Get("Authorization"), r.Header.Get("Idempotency-Key")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(`{"id":"msg_9"}`))
	}))
	t.Cleanup(srv.Close)

	id, err := New(srv.URL, time.Second).Send(t.Context(), "re_key", Message{
		From: "Ogen <hi@ogen.test>", To: "ann@example.com", Subject: "Hi", HTML: "<p>Hi</p>",
		Headers: map[string]string{"List-Unsubscribe": "<x>"}, IdempotencyKey: "welcome:u1",
	})
	if err != nil || id != "msg_9" {
		t.Fatalf("Send = %q, %v", id, err)
	}
	if gotAuth != "Bearer re_key" || gotIdem != "welcome:u1" || gotBody.To[0] != "ann@example.com" || gotBody.Headers["List-Unsubscribe"] != "<x>" {
		t.Errorf("request: auth %q, idem %q, body %+v", gotAuth, gotIdem, gotBody)
	}
}

func TestSendClassifiesFailures(t *testing.T) {
	tests := []struct {
		status    int
		transient bool
	}{
		{http.StatusTooManyRequests, true},
		{http.StatusServiceUnavailable, true},
		{http.StatusUnprocessableEntity, false},
		{http.StatusUnauthorized, false},
	}
	for _, tt := range tests {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tt.status)
			_, _ = w.Write([]byte(`{"message":"nope"}`))
		}))
		_, err := New(srv.URL, time.Second).Send(t.Context(), "k", Message{To: "a@b.test"})
		srv.Close()
		var re *Error
		if !errors.As(err, &re) || re.Status != tt.status || re.Transient != tt.transient || re.Message != "nope" {
			t.Errorf("status %d: %v, want transient %v", tt.status, err, tt.transient)
		}
	}

	_, err := New("http://127.0.0.1:1", time.Second).Send(t.Context(), "k", Message{To: "a@b.test"})
	var re *Error
	if !errors.As(err, &re) || !re.Transient {
		t.Errorf("unreachable: %v, want transient", err)
	}
}
