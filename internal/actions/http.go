// Package actions carries out the actions ogen plans for a step-run job: HTTP
// calls to outside APIs, media transfers and sidecar calls. It knows nothing
// of those APIs beyond where they live and how they authenticate: ogen builds
// every request and decodes every response, and this package only carries
// them, with keys leased from ogen.
package actions

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
const maxResponseBytes = 32 << 20

// Keys returns a leased key, empty when it is not set in ogen.
type Keys interface {
	Get(ctx context.Context, name string) (string, error)
}

// Upstream is one outside API: where it lives, the lease name of its key, and
// how the key is sent.
type Upstream struct {
	BaseURL    string
	Credential string
	// Header and Prefix place the key, e.g. "Authorization" + "Bearer ".
	Header string
	Prefix string
	// Timeout bounds each call.
	Timeout time.Duration
}

// Executor carries out actions.
type Executor struct {
	keys      Keys
	upstreams map[string]Upstream
	api       *http.Client
	open      *http.Client
	transfer  *http.Client
	sidecars  map[string]*Sidecar
}

// Options configures an Executor.
type Options struct {
	Keys      Keys
	Upstreams map[string]Upstream
	// FetchTimeout bounds each fetch of an absolute URL.
	FetchTimeout time.Duration
	Sidecars     map[string]*Sidecar
}

// New returns an Executor.
func New(o Options) *Executor {
	return &Executor{
		keys:      o.Keys,
		upstreams: o.Upstreams,
		api:       &http.Client{},
		open:      guardedClient(o.FetchTimeout),
		transfer:  &http.Client{},
		sidecars:  o.Sidecars,
	}
}

// HTTP makes one planned HTTP call. A call to a named API goes to its base
// URL with its leased key; with no key set in ogen the call is not made and
// the result says so. A call with no upstream fetches its absolute URL with
// no credentials, through a client that refuses private and internal
// addresses, since such URLs come from outside content. A request that gets
// no response comes back with Error set, so ogen sees it the way a direct
// call would.
func (e *Executor) HTTP(ctx context.Context, call *jobrpcv1.HttpCall) *jobrpcv1.StepResult {
	if call.GetUpstream() == "" {
		return e.fetch(ctx, call)
	}
	up, ok := e.upstreams[call.GetUpstream()]
	if !ok {
		return &jobrpcv1.StepResult{Error: fmt.Sprintf("unknown upstream %q", call.GetUpstream())}
	}
	key, err := e.keys.Get(ctx, up.Credential)
	if err != nil {
		return &jobrpcv1.StepResult{Error: "lease " + up.Credential + ": " + err.Error()}
	}
	if key == "" {
		return &jobrpcv1.StepResult{CredentialMissing: true, Error: up.Credential + " is not set in ogen"}
	}

	u := strings.TrimRight(up.BaseURL, "/") + call.GetPath()
	if q := query(call.GetQuery()); q != "" {
		u += "?" + q
	}
	if up.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, up.Timeout)
		defer cancel()
	}
	req, err := newRequest(ctx, call, u)
	if err != nil {
		return &jobrpcv1.StepResult{Error: err.Error()}
	}
	req.Header.Set(up.Header, up.Prefix+key)
	return result(e.api.Do(req))
}

func (e *Executor) fetch(ctx context.Context, call *jobrpcv1.HttpCall) *jobrpcv1.StepResult {
	u, err := url.Parse(call.GetUrl())
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return &jobrpcv1.StepResult{Error: "fetch: not an http(s) URL"}
	}
	req, err := newRequest(ctx, call, u.String())
	if err != nil {
		return &jobrpcv1.StepResult{Error: err.Error()}
	}
	return result(e.open.Do(req))
}

func newRequest(ctx context.Context, call *jobrpcv1.HttpCall, u string) (*http.Request, error) {
	var body io.Reader
	if len(call.GetBody()) > 0 {
		body = bytes.NewReader(call.GetBody())
	}
	method := call.GetMethod()
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range call.GetHeaders() {
		req.Header.Set(k, v)
	}
	return req, nil
}

func query(params []*jobrpcv1.QueryParam) string {
	if len(params) == 0 {
		return ""
	}
	q := url.Values{}
	for _, p := range params {
		q.Add(p.GetKey(), p.GetValue())
	}
	return q.Encode()
}

// minUploadBudget and uploadBytesPerSecond size an upload's deadline: a slow
// link still finishes a large video, a stalled one doesn't hang the job.
const (
	minUploadBudget      = 30 * time.Second
	uploadBytesPerSecond = 1 << 20
)

// Transfer streams a stored file from its download link to an upload URL.
// The bytes pass through river-service and nowhere else.
func (e *Executor) Transfer(ctx context.Context, t *jobrpcv1.MediaTransfer) *jobrpcv1.StepResult {
	budget := minUploadBudget + time.Duration(t.GetSize()/uploadBytesPerSecond)*time.Second
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	src, err := openSource(ctx, e.transfer, t.GetSourceUrl())
	if err != nil {
		return &jobrpcv1.StepResult{Error: err.Error()}
	}
	defer func() { _ = src.Close() }()

	put, err := http.NewRequestWithContext(ctx, http.MethodPut, t.GetUploadUrl(), src)
	if err != nil {
		return &jobrpcv1.StepResult{Error: err.Error()}
	}
	if t.GetContentType() != "" {
		put.Header.Set("Content-Type", t.GetContentType())
	}
	if t.GetSize() > 0 {
		put.ContentLength = t.GetSize()
		put.Header.Set("Content-Length", strconv.FormatInt(t.GetSize(), 10))
	}
	return result(e.transfer.Do(put))
}

// openSource opens a stored file's download link for reading.
func openSource(ctx context.Context, c *http.Client, sourceURL string) (io.ReadCloser, error) {
	get, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(get)
	if err != nil {
		return nil, fmt.Errorf("read source: %w", err)
	}
	if resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("read source: status %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func result(resp *http.Response, err error) *jobrpcv1.StepResult {
	if err != nil {
		return &jobrpcv1.StepResult{Error: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return &jobrpcv1.StepResult{Status: int32(resp.StatusCode), Error: "read response: " + err.Error()}
	}
	return &jobrpcv1.StepResult{Status: int32(resp.StatusCode), Body: b}
}
