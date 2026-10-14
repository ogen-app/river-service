package jobs

import (
	"time"

	"github.com/riverqueue/river"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/queue"
)

// --- the publishing family -------------------------------------------------

// zernioCallTimeout bounds one attempt of a publishing job that makes a single
// Zernio call, as in ogen.
const zernioCallTimeout = 20 * time.Second

// submitTimeout bounds one submit attempt, which streams the post's media to
// Zernio; it stays below RESCUE_STUCK_JOBS_AFTER's default.
const submitTimeout = 25 * time.Minute

// SubmitPostToZernioArgs submits a scheduled post to Zernio.
type SubmitPostToZernioArgs struct {
	PostID string `json:"post_id"`
}

func (SubmitPostToZernioArgs) Kind() string                 { return "submit_post_to_zernio" }
func (SubmitPostToZernioArgs) InsertOpts() river.InsertOpts { return river.InsertOpts{MaxAttempts: 5} }

// PollZernioStatusArgs checks a submitted post's publish status. It snoozes
// itself until the post is published or fails.
type PollZernioStatusArgs struct {
	PostID          string `json:"post_id"`
	PublisherPostID string `json:"publisher_post_id,omitempty"`
}

func (PollZernioStatusArgs) Kind() string { return "poll_zernio_status" }

// InsertOpts: one live poll per submission however often it is enqueued.
func (PollZernioStatusArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 3, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: waitingStates}}
}

// CancelZernioJobArgs takes a post back out of Zernio's queue.
type CancelZernioJobArgs struct {
	PostID string `json:"post_id"`
	Target string `json:"target"`
	Actor  string `json:"actor"`
}

func (CancelZernioJobArgs) Kind() string                 { return "cancel_zernio_job" }
func (CancelZernioJobArgs) InsertOpts() river.InsertOpts { return river.InsertOpts{MaxAttempts: 3} }

// WithdrawZernioPostArgs deletes a Zernio post no Ogen post holds any more.
type WithdrawZernioPostArgs struct {
	PublisherPostID string `json:"publisher_post_id"`
	PostID          string `json:"post_id"`
	TenantID        string `json:"tenant_id"`
	Reason          string `json:"reason"`
	Actor           string `json:"actor"`
}

func (WithdrawZernioPostArgs) Kind() string                 { return "withdraw_zernio_post" }
func (WithdrawZernioPostArgs) InsertOpts() river.InsertOpts { return river.InsertOpts{MaxAttempts: 8} }

// PostFirstCommentArgs posts a published post's delayed first comment.
type PostFirstCommentArgs struct {
	PostID          string `json:"post_id"`
	PublisherPostID string `json:"publisher_post_id"`
}

func (PostFirstCommentArgs) Kind() string { return "post_first_comment" }

// InsertOpts: one live job per comment.
func (PostFirstCommentArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5, UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: waitingStates}}
}

func registerPublishing(reg *queue.Registry, d stepDeps) {
	queue.Register(reg, newStepWorker[SubmitPostToZernioArgs](d, submitTimeout), func(m *jobsv1.SubmitPostToZernioArgs) SubmitPostToZernioArgs {
		return SubmitPostToZernioArgs{PostID: m.GetPostId()}
	})
	queue.Register(reg, newStepWorker[PollZernioStatusArgs](d, zernioCallTimeout), func(m *jobsv1.PollZernioStatusArgs) PollZernioStatusArgs {
		return PollZernioStatusArgs{PostID: m.GetPostId(), PublisherPostID: m.GetPublisherPostId()}
	})
	queue.Register(reg, newStepWorker[CancelZernioJobArgs](d, zernioCallTimeout), func(m *jobsv1.CancelZernioJobArgs) CancelZernioJobArgs {
		return CancelZernioJobArgs{PostID: m.GetPostId(), Target: m.GetTarget(), Actor: m.GetActor()}
	})
	queue.Register(reg, newStepWorker[WithdrawZernioPostArgs](d, zernioCallTimeout), func(m *jobsv1.WithdrawZernioPostArgs) WithdrawZernioPostArgs {
		return WithdrawZernioPostArgs{
			PublisherPostID: m.GetPublisherPostId(), PostID: m.GetPostId(), TenantID: m.GetTenantId(),
			Reason: m.GetReason(), Actor: m.GetActor(),
		}
	})
	queue.Register(reg, newStepWorker[PostFirstCommentArgs](d, zernioCallTimeout), func(m *jobsv1.PostFirstCommentArgs) PostFirstCommentArgs {
		return PostFirstCommentArgs{PostID: m.GetPostId(), PublisherPostID: m.GetPublisherPostId()}
	})
}
