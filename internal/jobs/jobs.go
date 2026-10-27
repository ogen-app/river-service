// Package jobs assembles the job kinds this build runs. Each family registers
// its kinds here as it moves over from ogen; a kind not registered is refused
// by JobService with UNIMPLEMENTED, and ogen keeps it in its outbox.
//
// ogen schedules and runs a family until it is listed in ogen's
// OUTBOX_RELAY_KINDS. Deploy river-service with the family first, then add it
// there.
package jobs

import (
	"log/slog"

	"github.com/ogen-app/river-service/internal/actions"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/credentials"
	"github.com/ogen-app/river-service/internal/logging"
	"github.com/ogen-app/river-service/internal/ogenrpc"
	"github.com/ogen-app/river-service/internal/queue"
	"github.com/ogen-app/river-service/internal/resend"
)

// Registry returns every job kind this build can run. Without a connection to
// ogen there is nothing a job could do, so nothing is registered.
func Registry(cfg *config.Config, ogen *ogenrpc.Client) *queue.Registry {
	reg := queue.NewRegistry()
	if ogen == nil {
		return reg
	}
	registerHousekeeping(reg, cfg, ogen.Maintenance)
	keys := credentials.NewCache(ogen.Credentials)
	registerEmail(reg, ogen.Email, ogen.Maintenance, keys, resend.New(cfg.EmailBaseURL, cfg.EmailHTTPTimeout))
	registerHarbor(reg, cfg.HarborWebhookURL, keys)
	steps := stepDeps{ogen: ogen.Steps, actions: newActions(cfg, keys), children: newChildKinds()}
	registerZernioLifecycle(reg, cfg, steps, ogen.Maintenance)
	registerPublishing(reg, steps)
	registerPublishingSweeps(reg, cfg, steps, ogen.Maintenance)
	registerIngestion(reg, steps)
	registerAnalysis(reg, cfg, steps, ogen.Maintenance)
	registerBrandDistill(reg, steps, ogen.Maintenance)
	registerWeeklyReport(reg, cfg, steps)
	registerBilling(reg, cfg, steps, ogen.Maintenance)
	return reg
}

// newActions builds the executor for the outside calls ogen plans: the named
// APIs with their leased keys, and the sidecars this service is configured
// with.
func newActions(cfg *config.Config, keys actions.Keys) *actions.Executor {
	sidecars := map[string]*actions.Sidecar{}
	for name, addr := range map[string]string{
		"pdf":       cfg.PDFServiceAddr,
		"documents": cfg.DocumentsServiceAddr,
		"image":     cfg.ImageServiceAddr,
		"audio":     cfg.AudioServiceAddr,
	} {
		if addr == "" {
			continue
		}
		sc, err := actions.DialSidecar(addr, cfg.SidecarMaxRecvBytes)
		if err != nil {
			slog.Error("sidecar unavailable", logging.AttrComponent, "boot", "sidecar", name, logging.AttrError, err)
			continue
		}
		sidecars[name] = sc
	}
	return actions.New(actions.Options{
		Keys: keys,
		Upstreams: map[string]actions.Upstream{
			upstreamZernio:    {BaseURL: cfg.ZernioBaseURL, Credential: credentials.Zernio, Header: "Authorization", Prefix: "Bearer ", Timeout: cfg.ZernioHTTPTimeout},
			upstreamGemini:    {BaseURL: cfg.GeminiBaseURL, Credential: credentials.Gemini, Header: "x-goog-api-key", Timeout: cfg.GeminiHTTPTimeout},
			upstreamFirecrawl: {BaseURL: cfg.FirecrawlBaseURL, Credential: credentials.Firecrawl, Header: "Authorization", Prefix: "Bearer ", Timeout: cfg.FirecrawlHTTPTimeout},
			upstreamLemonSqueezy: {
				BaseURL: cfg.LemonSqueezyBaseURL, Credential: credentials.LemonSqueezy, Header: "Authorization", Prefix: "Bearer ", Timeout: cfg.LemonSqueezyHTTPTimeout,
			},
		},
		FetchTimeout: cfg.FetchTimeout,
		Sidecars:     sidecars,
	})
}

// The named APIs ogen's planned calls go to.
const (
	upstreamZernio       = "zernio"
	upstreamGemini       = "gemini"
	upstreamFirecrawl    = "firecrawl"
	upstreamLemonSqueezy = "lemonsqueezy"
)
