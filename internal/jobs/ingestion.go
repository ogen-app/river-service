package jobs

import (
	"time"

	"github.com/riverqueue/river"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/queue"
)

// --- the ingestion family --------------------------------------------------

// parseTimeout bounds one attempt of a parse-and-embed ingest, as in ogen.
const parseTimeout = 16 * time.Minute

// ProcessPdfArgs parses and embeds an uploaded PDF.
type ProcessPdfArgs struct {
	AssetID      string `json:"asset_id"`
	TenantID     string `json:"tenant_id"`
	OriginalName string `json:"original_name"`
	MimeType     string `json:"mime_type"`
}

func (ProcessPdfArgs) Kind() string { return "process_pdf" }
func (ProcessPdfArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueIngest, MaxAttempts: 5}
}

// ProcessDocumentArgs parses and embeds an uploaded office or text document.
type ProcessDocumentArgs struct {
	AssetID      string `json:"asset_id"`
	TenantID     string `json:"tenant_id"`
	OriginalName string `json:"original_name"`
	MimeType     string `json:"mime_type"`
	StorageKey   string `json:"storage_key"`
}

func (ProcessDocumentArgs) Kind() string { return "process_document" }
func (ProcessDocumentArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueIngest, MaxAttempts: 5}
}

func registerIngestion(reg *queue.Registry, d stepDeps) {
	queue.Register(reg, newStepWorker[ProcessPdfArgs](d, parseTimeout), func(m *jobsv1.ProcessPdfArgs) ProcessPdfArgs {
		return ProcessPdfArgs{AssetID: m.GetAssetId(), TenantID: m.GetTenantId(), OriginalName: m.GetOriginalName(), MimeType: m.GetMimeType()}
	})
	queue.Register(reg, newStepWorker[ProcessDocumentArgs](d, parseTimeout), func(m *jobsv1.ProcessDocumentArgs) ProcessDocumentArgs {
		return ProcessDocumentArgs{
			AssetID: m.GetAssetId(), TenantID: m.GetTenantId(), OriginalName: m.GetOriginalName(),
			MimeType: m.GetMimeType(), StorageKey: m.GetStorageKey(),
		}
	})
}
