// Package resend sends one email through Resend's API.
package resend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// errorBodyLimit bounds how much of an error response is read.
const errorBodyLimit = 4 << 10

// Message is one email to send.
type Message struct {
	From           string
	ReplyTo        string
	To             string
	Subject        string
	HTML           string
	Text           string
	Headers        map[string]string
	IdempotencyKey string
}

// Error is a failed send. Transient errors (network, timeouts, 429, 5xx) are
// worth a retry; the rest (a rejected recipient, a bad request) are not.
type Error struct {
	Status    int
	Transient bool
	Message   string
}

func (e *Error) Error() string {
	if e.Status > 0 {
		return fmt.Sprintf("resend: status %d: %s", e.Status, e.Message)
	}
	return "resend: " + e.Message
}

// Client sends through one Resend endpoint.
type Client struct {
	baseURL string
	http    *http.Client
}

// New returns a client for baseURL (https://api.resend.com in production).
func New(baseURL string, timeout time.Duration) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: timeout}}
}

type sendRequest struct {
	From    string            `json:"from"`
	To      []string          `json:"to"`
	Subject string            `json:"subject"`
	HTML    string            `json:"html,omitempty"`
	Text    string            `json:"text,omitempty"`
	ReplyTo string            `json:"reply_to,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// Send delivers msg with apiKey and returns Resend's message id. Resend
// honours the Idempotency-Key header, so a retried send never delivers twice.
// Once Resend has accepted the message no error is returned, even if its id
// can't be read, because a retry would send again.
func (c *Client) Send(ctx context.Context, apiKey string, msg Message) (string, error) {
	buf, err := json.Marshal(sendRequest{
		From: msg.From, To: []string{msg.To}, Subject: msg.Subject, HTML: msg.HTML, Text: msg.Text,
		ReplyTo: msg.ReplyTo, Headers: msg.Headers,
	})
	if err != nil {
		return "", &Error{Message: "encode request: " + err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/emails", bytes.NewReader(buf))
	if err != nil {
		return "", &Error{Message: err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if msg.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", msg.IdempotencyKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", &Error{Transient: true, Message: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, errorBodyLimit))
		return "", &Error{
			Status:    resp.StatusCode,
			Transient: resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests,
			Message:   errorMessage(raw),
		}
	}
	var out struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, errorBodyLimit)).Decode(&out)
	return out.ID, nil
}

func errorMessage(raw []byte) string {
	var body struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &body) == nil && body.Message != "" {
		return body.Message
	}
	s := strings.TrimSpace(string(raw))
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}
