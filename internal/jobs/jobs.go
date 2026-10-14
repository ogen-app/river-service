// Package jobs assembles the job kinds this build runs. Each family registers
// its kinds here as it moves over from ogen; a kind not registered is refused
// by JobService with UNIMPLEMENTED, and ogen keeps it in its outbox.
//
// ogen schedules and runs a family until it is listed in ogen's
// OUTBOX_RELAY_KINDS. Deploy river-service with the family first, then add it
// there.
package jobs

import (
	"github.com/riverqueue/river"

	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/credentials"
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
	children := &childKinds{build: map[string]func([]byte) (river.JobArgs, error){}}
	registerZernioLifecycle(reg, cfg, ogen.Zernio, ogen.Maintenance, keys, newZernioSender(cfg), children)
	return reg
}
