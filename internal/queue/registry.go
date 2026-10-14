// Package queue runs river-service's River client: the job kinds this build
// can work, the middleware every job runs through, and River's schema.
package queue

import (
	"time"

	"github.com/riverqueue/river"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
)

// argsOneof is the EnqueueRequest oneof that names the job kind.
const argsOneof = "args"

// Registry holds the job kinds this build can run: how to turn each kind's
// Enqueue args into River job args, and the worker that runs them. Every
// service registers every kind; RIVER_QUEUES decides which queues a service
// works, so the light service can still enqueue a kind the heavy one runs.
type Registry struct {
	workers  *river.Workers
	convert  map[string]func(proto.Message) river.JobArgs
	periodic []*river.PeriodicJob
	kinds    int
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{workers: river.NewWorkers(), convert: map[string]func(proto.Message) river.JobArgs{}}
}

// RegisterPeriodic adds a sweep river-service schedules itself every interval.
// It has no Enqueue args: nothing enqueues it. Like ogen's sweeps it runs once
// on start, and a zero interval leaves it out (River would spin on it).
func RegisterPeriodic[A river.JobArgs](r *Registry, worker river.Worker[A], every time.Duration, args A) {
	river.AddWorker(r.workers, worker)
	r.kinds++
	if every <= 0 {
		return
	}
	r.periodic = append(r.periodic, river.NewPeriodicJob(river.PeriodicInterval(every),
		func() (river.JobArgs, *river.InsertOpts) { return args, nil },
		&river.PeriodicJobOpts{RunOnStart: true}))
}

// PeriodicJobs is the schedule of every registered sweep.
func (r *Registry) PeriodicJobs() []*river.PeriodicJob { return r.periodic }

// Register adds a job kind. The River kind (A's Kind()) must equal the name of
// the EnqueueRequest args field M is sent in, e.g. "send_email" for
// SendEmailArgs; Register panics on a mismatch or a duplicate, which a test of
// the full registry catches at build time.
func Register[M proto.Message, A river.JobArgs](r *Registry, worker river.Worker[A], convert func(M) A) {
	var zero A
	kind := zero.Kind()
	if !isArgsField(kind) {
		panic("queue: kind " + kind + " is not an EnqueueRequest args field")
	}
	if _, dup := r.convert[kind]; dup {
		panic("queue: kind " + kind + " registered twice")
	}
	river.AddWorker(r.workers, worker)
	r.kinds++
	r.convert[kind] = func(m proto.Message) river.JobArgs { return convert(m.(M)) }
}

// Len is the number of registered kinds, sweeps included.
func (r *Registry) Len() int { return r.kinds }

// Workers is the River workers bundle of every registered kind.
func (r *Registry) Workers() *river.Workers { return r.workers }

// Args returns the River job args for req and the kind it names. kind is empty
// when no args field is set, which is how a kind added to the contract after
// this build was compiled arrives. ok is false when the kind is not
// registered.
func (r *Registry) Args(req *jobsv1.EnqueueRequest) (args river.JobArgs, kind string, ok bool) {
	msg := req.ProtoReflect()
	field := msg.WhichOneof(msg.Descriptor().Oneofs().ByName(argsOneof))
	if field == nil {
		return nil, "", false
	}
	kind = string(field.Name())
	convert, ok := r.convert[kind]
	if !ok {
		return nil, kind, false
	}
	return convert(msg.Get(field).Message().Interface()), kind, true
}

func isArgsField(kind string) bool {
	oneof := (&jobsv1.EnqueueRequest{}).ProtoReflect().Descriptor().Oneofs().ByName(argsOneof)
	return oneof.Fields().ByName(protoreflect.Name(kind)) != nil
}
