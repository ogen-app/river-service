// Package zernio sends the Zernio requests ogen plans. It knows nothing of
// Zernio's API beyond authentication: ogen builds every request and decodes
// every response, river-service only carries them, with a key leased from
// ogen.
package zernio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	jobrpcv1 "github.com/ogen-app/river-service/gen/jobrpc/v1"
)

// maxResponseBytes bounds a response handed back to ogen.
const maxResponseBytes = 16 << 20

// Client sends requests to one Zernio API base URL.
type Client struct {
	baseURL string
	api     *http.Client
	upload  *http.Client
}

// New returns a client for baseURL (https://zernio.com/api/v1 in production).
// timeout bounds each API call; a media upload is bounded by its own budget.
func New(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		api:     &http.Client{Timeout: timeout},
		upload:  &http.Client{},
	}
}

// Do sends one planned call with apiKey and returns Zernio's answer. A
// request that gets no response comes back as a result with Error set, so
// ogen sees it the way a direct call would.
func (c *Client) Do(ctx context.Context, apiKey string, call *jobrpcv1.ZernioCall) *jobrpcv1.ZernioResult {
	u := c.baseURL + call.GetPath()
	if len(call.GetQuery()) > 0 {
		q := url.Values{}
		for _, p := range call.GetQuery() {
			q.Add(p.GetKey(), p.GetValue())
		}
		u += "?" + q.Encode()
	}
	var body io.Reader
	if len(call.GetBody()) > 0 {
		body = bytes.NewReader(call.GetBody())
	}
	req, err := http.NewRequestWithContext(ctx, call.GetMethod(), u, body)
	if err != nil {
		return &jobrpcv1.ZernioResult{Error: err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range call.GetHeaders() {
		req.Header.Set(k, v)
	}
	return result(c.api.Do(req))
}

// minUploadBudget and uploadBytesPerSecond size an upload's deadline: a slow
// link still finishes a large video, a stalled one doesn't hang the job.
const (
	minUploadBudget      = 30 * time.Second
	uploadBytesPerSecond = 1 << 20
)

// Transfer streams a stored file from its download link to a Zernio upload
// URL. The bytes pass through river-service and nowhere else.
func (c *Client) Transfer(ctx context.Context, t *jobrpcv1.MediaTransfer) *jobrpcv1.ZernioResult {
	budget := minUploadBudget + time.Duration(t.GetSize()/uploadBytesPerSecond)*time.Second
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	get, err := http.NewRequestWithContext(ctx, http.MethodGet, t.GetSourceUrl(), nil)
	if err != nil {
		return &jobrpcv1.ZernioResult{Error: err.Error()}
	}
	src, err := c.upload.Do(get)
	if err != nil {
		return &jobrpcv1.ZernioResult{Error: "read source: " + err.Error()}
	}
	defer func() { _ = src.Body.Close() }()
	if src.StatusCode >= 300 {
		return &jobrpcv1.ZernioResult{Error: fmt.Sprintf("read source: status %d", src.StatusCode)}
	}

	put, err := http.NewRequestWithContext(ctx, http.MethodPut, t.GetUploadUrl(), src.Body)
	if err != nil {
		return &jobrpcv1.ZernioResult{Error: err.Error()}
	}
	if t.GetContentType() != "" {
		put.Header.Set("Content-Type", t.GetContentType())
	}
	if t.GetSize() > 0 {
		put.ContentLength = t.GetSize()
		put.Header.Set("Content-Length", strconv.FormatInt(t.GetSize(), 10))
	}
	return result(c.upload.Do(put))
}

func result(resp *http.Response, err error) *jobrpcv1.ZernioResult {
	if err != nil {
		return &jobrpcv1.ZernioResult{Error: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return &jobrpcv1.ZernioResult{Status: int32(resp.StatusCode), Error: "read response: " + err.Error()}
	}
	return &jobrpcv1.ZernioResult{Status: int32(resp.StatusCode), Body: b}
}
