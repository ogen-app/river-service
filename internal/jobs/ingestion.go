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

// urlTimeout bounds one attempt of a URL ingest: the scrape, the page's
// images and the embeds.
const urlTimeout = 10 * time.Minute

// ProcessUrlArgs scrapes, mirrors and embeds a web page.
type ProcessUrlArgs struct {
	AssetID   string `json:"asset_id"`
	TenantID  string `json:"tenant_id"`
	SourceURL string `json:"source_url"`
	Refresh   bool   `json:"refresh"`
}

func (ProcessUrlArgs) Kind() string { return "process_url" }
func (ProcessUrlArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueIngest, MaxAttempts: 5}
}

// Attempt bounds of the image and audio jobs. Both checkpoint their progress
// in ogen (the vision pass; each transcribed segment), so an attempt cut
// short resumes where it stopped. Audio stays below RESCUE_STUCK_JOBS_AFTER's
// default although a long recording takes several attempts.
const (
	imageTimeout   = 10 * time.Minute
	reembedTimeout = 5 * time.Minute
	audioTimeout   = 25 * time.Minute
)

// ProcessImageArgs extracts and embeds an uploaded image.
type ProcessImageArgs struct {
	AssetID      string `json:"asset_id"`
	TenantID     string `json:"tenant_id"`
	OriginalName string `json:"original_name"`
	MimeType     string `json:"mime_type"`
	StorageKey   string `json:"storage_key"`
	RunKey       string `json:"run_key"`
	PinnedModel  string `json:"pinned_model,omitempty"`
}

func (ProcessImageArgs) Kind() string { return "process_image" }
func (ProcessImageArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueImage, MaxAttempts: 5}
}

// ReembedImageArgs re-embeds an image asset's text after an edit.
type ReembedImageArgs struct {
	AssetID  string `json:"asset_id"`
	TenantID string `json:"tenant_id"`
}

func (ReembedImageArgs) Kind() string { return "reembed_image" }
func (ReembedImageArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueImage, MaxAttempts: 5}
}

// ProcessAudioArgs transcribes and embeds an uploaded recording.
type ProcessAudioArgs struct {
	AssetID      string `json:"asset_id"`
	TenantID     string `json:"tenant_id"`
	OriginalName string `json:"original_name"`
	MimeType     string `json:"mime_type"`
	StorageKey   string `json:"storage_key"`
	RunKey       string `json:"run_key"`
	PinnedModel  string `json:"pinned_model,omitempty"`
}

func (ProcessAudioArgs) Kind() string { return "process_audio" }
func (ProcessAudioArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: config.QueueAudio, MaxAttempts: 10}
}

func registerIngestion(reg *queue.Registry, d stepDeps) {
	queue.Register(reg, newStepWorker[ProcessImageArgs](d, imageTimeout), func(m *jobsv1.ProcessImageArgs) ProcessImageArgs {
		return ProcessImageArgs{
			AssetID: m.GetAssetId(), TenantID: m.GetTenantId(), OriginalName: m.GetOriginalName(), MimeType: m.GetMimeType(),
			StorageKey: m.GetStorageKey(), RunKey: m.GetRunKey(), PinnedModel: m.GetPinnedModel(),
		}
	})
	queue.Register(reg, newStepWorker[ReembedImageArgs](d, reembedTimeout), func(m *jobsv1.ReembedImageArgs) ReembedImageArgs {
		return ReembedImageArgs{AssetID: m.GetAssetId(), TenantID: m.GetTenantId()}
	})
	queue.Register(reg, newStepWorker[ProcessAudioArgs](d, audioTimeout), func(m *jobsv1.ProcessAudioArgs) ProcessAudioArgs {
		return ProcessAudioArgs{
			AssetID: m.GetAssetId(), TenantID: m.GetTenantId(), OriginalName: m.GetOriginalName(), MimeType: m.GetMimeType(),
			StorageKey: m.GetStorageKey(), RunKey: m.GetRunKey(), PinnedModel: m.GetPinnedModel(),
		}
	})
	queue.Register(reg, newStepWorker[ProcessUrlArgs](d, urlTimeout), func(m *jobsv1.ProcessUrlArgs) ProcessUrlArgs {
		return ProcessUrlArgs{AssetID: m.GetAssetId(), TenantID: m.GetTenantId(), SourceURL: m.GetSourceUrl(), Refresh: m.GetRefresh()}
	})
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
