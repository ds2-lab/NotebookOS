package tiresias

import (
	"github.com/scusemua/distributed-notebook/common/types"
	"math"
	"time"
)

const (
	Running  JobStatus = "RUNNING"
	Pending  JobStatus = "PENDING"
	Complete JobStatus = "COMPLETE" // Corresponds to "END" in the original Tiresias implementation.
	Erred    JobStatus = "ERRED"    // Corresponds to "ERROR" in the original Tiresias implementation.
)

// JobStatus defines the various states that a Tiresias Job can be in.
type JobStatus string

func (s JobStatus) String() string {
	return string(s)
}

// Job encapsulates a Tiresias job.
//
// It is a notebook cell execution in the context of this system, but it includes the various metrics
// used by Tiresias when scheduling jobs.
type Job struct {
	// JobId uniquely identifies the job.
	//
	// The JobId comes from the "msg_id" field of the "execute_request" Jupyter message's header.
	JobId string

	// KernelId is the ID of the kernel responsible for executing this Job.
	KernelId string

	// Status is the status of the job.
	Status JobStatus

	StartTime     time.Time     // StartTime is the time at which the Job started running.
	LastStartTime time.Time     // LastStartTime is the time at which the Job was last scheduled.
	LastCheckTime time.Time     // LastCheckTime is the time at which the Job's progress was last checked.
	SubmitTime    time.Time     // SubmitTime is the time at which the Job was submitted by a user.
	PendingTime   time.Duration // PendingTime is how long the Job has been waiting to be scheduled.

	// TotalExecutedTime is the cumulative amount of time that the Job has spent executing
	// across all the times that the Job has been scheduled.
	TotalExecutedTime time.Duration

	// ExecutedTime is the time the Job has been running since it was last started/resumed.
	// ExecutedTime is used for deciding the priority queue of the job.
	// ExecutedTime may be zeroed by LastPendingTime.
	ExecutedTime time.Duration

	// LastPendingTime is the amount of time that the Job spent waiting to be scheduled the last time
	// that the Job was in the Pending state (i.e., Status).
	LastPendingTime time.Duration

	// ResourceSpec is the amount of resources required by the Job.
	ResourceSpec types.Spec

	// CurrentQueueId is the ID of the queue that the Job is currently in.
	CurrentQueueId int

	// Promote is the number of times that the Job has been promoted.
	Promote int

	// Overhead is...
	Overhead int

	// Preempt is the number of times that the Job has been preempted.
	Preempt int

	// Resume is the number of times that the Job has been resumed.
	Resume int
}

// NewJob creates a new Job struct and returns a pointer to it.
func NewJob(kernelId string, msgId string, resourceSpec types.Spec) *Job {
	return &Job{
		KernelId:        kernelId,
		JobId:           msgId,
		ResourceSpec:    resourceSpec,
		Status:          Pending,
		StartTime:       time.UnixMilli(math.MaxInt32),
		LastStartTime:   time.Time{},
		LastCheckTime:   time.Now(),
		SubmitTime:      time.Now(),
		ExecutedTime:    0,
		PendingTime:     0,
		LastPendingTime: 0,
		Promote:         0,
		Overhead:        0,
	}
}

func (j *Job) IsRunning() bool {
	return j.Status == Running
}

func (j *Job) IsPending() bool {
	return j.Status == Pending
}

func (j *Job) IsComplete() bool {
	return j.Status == Complete
}

func (j *Job) NumGPUs() int {
	return int(j.ResourceSpec.GPU())
}
