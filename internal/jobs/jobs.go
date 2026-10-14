// Package jobs assembles the job kinds this build runs. Each family registers
// its kinds here as it moves over from ogen; a kind not registered is refused
// by JobService with UNIMPLEMENTED, and ogen keeps it in its outbox.
package jobs

import "github.com/ogen-app/river-service/internal/queue"

// Registry returns every job kind this build can run.
func Registry() *queue.Registry {
	return queue.NewRegistry()
}
