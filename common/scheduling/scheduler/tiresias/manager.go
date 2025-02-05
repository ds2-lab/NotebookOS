package tiresias

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/types"
	"slices"
	"sync"
	"time"
)

// JobManager manages Tiresias jobs.
type JobManager struct {
	log            logger.Logger
	RunningJobs    []*Job // RunningJobs contains Job instances that are actively running.
	PendingJobs    []*Job // PendingJobs contains Job instances that are waiting to be scheduled.
	RunnableJobs   []*Job // RunnableJobs contains Job instances that can be run, but are not yet running.
	CompletedJobs  []*Job // CompletedJobs contains Job instances that have completed successfully.
	MigratableJobs []*Job // MigratableJobs contains Job instances that are eligible for migration.

	// Queues are the different Job queues managed by the JobManager.
	Queues []map[string]*Job

	// QueueLimits defines the GPU time limits for each queue.
	QueueLimits []time.Duration

	// TotalNumJobs is the total number of Job instances currently under the management of the JobManager.
	TotalNumJobs int

	// NumCompletedJobs is the number of Job instances that have completed.
	NumCompletedJobs int

	// StarvationLimit corresponds to the 'solve_starvation' parameter from the original Tiresias implementation.
	StarvationLimit time.Duration

	mu sync.Mutex
}

func NewJobManager(numQueues int, starvationLimit time.Duration) *JobManager {
	manager := &JobManager{
		RunningJobs:      make([]*Job, 0),
		PendingJobs:      make([]*Job, 0),
		RunnableJobs:     make([]*Job, 0),
		CompletedJobs:    make([]*Job, 0),
		MigratableJobs:   make([]*Job, 0),
		TotalNumJobs:     0,
		NumCompletedJobs: 0,
		Queues:           make([]map[string]*Job, 0, numQueues),
		StarvationLimit:  starvationLimit,
		QueueLimits:      make([]time.Duration, 0, numQueues),
	}

	initialQueueLimit := time.Second * 3600
	queueLimit := initialQueueLimit

	// Initialize the job queues.
	for i := 0; i < numQueues; i++ {
		jobQueue := make(map[string]*Job)
		manager.Queues = append(manager.Queues, jobQueue)
		manager.QueueLimits = append(manager.QueueLimits, queueLimit)

		// Initially, queue limit is 'initialQueueLimit'.
		// Then, it is 2 * initialQueueLimit.
		// Then, it is 3 * initialQueueLimit.
		// Then, it is 4 * initialQueueLimit.
		// And so on and so forth.
		queueLimit = queueLimit * time.Duration(i+2)
	}

	config.InitLogger(&manager.log, manager)

	return manager
}

func (m *JobManager) NumQueues() int {
	return len(m.Queues)
}

func (m *JobManager) RegisterJob(jMsg *messaging.JupyterMessage, kernelId string, spec types.Spec) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if jMsg.JupyterMessageType() != messaging.ShellExecuteRequest {
		m.log.Error("Job registration attempted with message %s of invalid type: \"%s\"",
			jMsg.JupyterMessageId(), jMsg.JupyterMessageType())
		return
	}

	job := NewJob(kernelId, jMsg.JupyterMessageId(), spec)

	// New jobs are placed into the Runnable Jobs list with status PENDING.
	m.RunnableJobs = append(m.RunningJobs, job)

	// All new jobs start in Queue 0.
	m.Queues[0][job.JobId] = job
}

// isLastQueue returns true if the given queue ID corresponds to the "last" Job queue.
func (m *JobManager) isLastQueue(queueId int) bool {
	return queueId == (len(m.Queues) - 1)
}

// dequeueJob removes the specified Job from its current queue and returns the queue ID.
//
// dequeueJob is not thread safe.
func (m *JobManager) dequeueJob(job *Job) int {
	delete(m.Queues[job.CurrentQueueId], job.JobId)
	prevQueueId := job.CurrentQueueId
	job.CurrentQueueId = -1

	return prevQueueId
}

// enqueueJob is not thread safe.
func (m *JobManager) enqueueJob(job *Job, queueId int) error {
	if job.CurrentQueueId != -1 {
		return fmt.Errorf("job is already enqueued in queue %d", job.CurrentQueueId)
	}

	m.Queues[queueId][job.JobId] = job
	job.CurrentQueueId = queueId
	return nil
}

// updatePendingJob is not thread safe.
func (m *JobManager) updatePendingJob(job *Job) (err error) {
	if !job.IsPending() {
		return
	}

	m.log.Debug("Updating pending job '%s' for kernel '%s'", job.JobId, job.KernelId)

	timeElapsed := time.Since(job.LastCheckTime)
	job.LastCheckTime = time.Now()
	job.PendingTime = job.PendingTime + timeElapsed // This is the total pending time.

	// If the job has not yet started, then the job will already be in Queue 0, so we don't need to push it back.
	if job.ExecutedTime > 0 {
		job.LastPendingTime = job.LastPendingTime + timeElapsed
	}

	// For jobs in Queue 0, we do not push them back, and they must be scheduled.
	if m.StarvationLimit > 0 && job.CurrentQueueId > 0 && job.TotalExecutedTime > 0 && job.ExecutedTime > 0 {
		err = m.tryPromoteJobToQueue0(job)
	}

	return
}

// tryPromoteJobToQueue0 checks if the specified Job is eligible for promotion.
// If so, then the Job is promoted to Queue 0.
func (m *JobManager) tryPromoteJobToQueue0(job *Job) (err error) {
	if job.LastPendingTime <= job.ExecutedTime*m.StarvationLimit {
		m.log.Debug("Job %s for kernel %s is NOT eligible for promotion to Queue 0. LastPendingTime=%v, ExecutedTime=%v",
			job.JobId, job.KernelId, job.LastPendingTime, job.ExecutedTime)
		return
	}

	job.ExecutedTime = 0
	job.LastPendingTime = 0

	// Remove the job from its current queue.
	m.dequeueJob(job)

	// Enqueue the job in Queue #0.
	err = m.enqueueJob(job, 0)

	if err == nil {
		job.Promote += 1
	}

	return
}

// updateRunningJob is not thread safe.
func (m *JobManager) updateRunningJob(job *Job) error {
	if !job.IsRunning() {
		return fmt.Errorf("job is not running, job has status '%s'", job.Status.String())
	}

	timeElapsed := time.Since(job.LastCheckTime)

	job.LastCheckTime = time.Now()
	job.TotalExecutedTime = job.TotalExecutedTime + timeElapsed
	job.ExecutedTime = job.ExecutedTime + timeElapsed

	m.log.Debug("Updating running job '%s' for kernel '%s'", job.JobId, job.KernelId)

	gpuTime := job.ExecutedTime * time.Duration(job.ResourceSpec.GPU())

	// If the job is not currently in the last queue and its GPU time exceeds the limit for its current queue,
	// then move the job to the next queue.
	if !m.isLastQueue(job.CurrentQueueId) && gpuTime > m.QueueLimits[job.CurrentQueueId] {
		prevQueueId := m.dequeueJob(job)
		err := m.enqueueJob(job, prevQueueId+1)
		if err != nil {
			m.log.Error("Could not enqueue job %s (for kernel %s) in queue %d: %v",
				job.JobId, job.KernelId, prevQueueId+1, err)
			return err
		}

		m.log.Debug("Demoted job %s (for kernel %s) from queue %d to queue %d.",
			job.JobId, job.KernelId, prevQueueId, job.CurrentQueueId)
	}

	return nil
}

// removeFromRunnable is not thread safe.
//
// removeFromRunnable returns true if the specified Job was removed from the RunnableJobs slice.
func (m *JobManager) removeFromRunnable(target *Job) bool {
	for i, job := range m.RunnableJobs {
		if job.JobId == target.JobId {
			m.RunnableJobs = append(m.RunnableJobs[:i], m.RunnableJobs[i+1:]...)
			return true
		}
	}

	return false
}

func (m *JobManager) updateJobs() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var removeFromRunnable []*Job
	var errs []error

	m.log.Debug("Updating jobs. Number of RunnableJobs: %d", len(m.RunnableJobs))

	for _, job := range m.RunnableJobs {
		if job.IsRunning() {
			err := m.updateRunningJob(job)
			if err != nil {
				m.log.Error("Error while updating running job %s (for kernel %s): %v", job.JobId, job.KernelId, err)
				if errs == nil {
					errs = make([]error, 0, 1)
				}

				errs = append(errs, err)
			}
		} else if job.IsPending() {
			err := m.updatePendingJob(job)
			if err != nil {
				m.log.Error("Error while updating running job %s (for kernel %s): %v", job.JobId, job.KernelId, err)
				if errs == nil {
					errs = make([]error, 0, 1)
				}

				errs = append(errs, err)
			}
		} else if job.IsComplete() {
			if removeFromRunnable == nil {
				removeFromRunnable = make([]*Job, 0, 1)
			}

			removeFromRunnable = append(removeFromRunnable, job)
		}
	}

	for _, job := range removeFromRunnable {
		m.removeFromRunnable(job)
	}

	if errs != nil && len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (m *JobManager) canRunJob(job *Job) bool {
	// TODO: Implement me
	// if CLUSTER.free_gpu >= job['num_gpu']:
	panic("Not implemented")
}

func (m *JobManager) scheduleJobs() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	jobsToPreempt := make([]*Job, 0)
	jobsToSchedule := make([]*Job, 0)

	for queueIndex, jobQueue := range m.Queues {
		for jobId, job := range jobQueue {
			if m.canRunJob(job) {
				// We can run the job.
				m.log.Debug("Will schedule job %s [queue=%d] for kernel %s", jobId, queueIndex, job.KernelId)
				jobsToSchedule = append(jobsToSchedule, job)

				// TODO: Add this logic?
				// CLUSTER.free_gpu = int(CLUSTER.free_gpu - job['num_gpu'])
			} else if job.IsRunning() {
				// We cannot run the job. If it's currently running, then we need to preempt it.
				m.log.Debug("Will preempt job %s [queue=%d] for kernel %s", jobId, queueIndex, job.KernelId)
				jobsToPreempt = append(jobsToPreempt, job)
			}
		}
	}

	//for _, job := range m.RunnableJobs {
	//
	//}

	for _, job := range jobsToPreempt {
		m.preemptJob(job)
	}

	for _, job := range jobsToSchedule {
		m.scheduleJob(job)
	}

	m.sortRunnableJobs()

	for _, job := range m.RunnableJobs {
		if !job.IsRunning() {
			continue
		}

		// TODO: Attempt to place the job on a node.
		panic("Not implemented.")
	}

	errs := make([]error, 0)

	for queueIndex, jobQueue := range m.Queues {
		pendingJobs := make([]*Job, 0)

		for _, job := range jobQueue {
			if job.IsPending() {
				pendingJobs = append(pendingJobs, job)
			}
		}

		// "Sort based on the job start time."
		for _, job := range pendingJobs {
			m.dequeueJob(job)
		}

		for _, job := range pendingJobs {
			err := m.enqueueJob(job, queueIndex)
			if err != nil {
				m.log.Error("Failed to enqueue job %s for kernel %s: %v", job.JobId, job.KernelId, err)
				errs = append(errs, err)
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (m *JobManager) scheduleJob(job *Job) {
	job.Status = Running
	job.Resume += 1

	if job.StartTime.IsZero() {
		m.log.Debug("Scheduled job %s for kernel %s for the first time.", job.JobId, job.KernelId)
		job.StartTime = time.Now()
	}

	// TODO: Implement the logic to actually schedule the job.
	panic("Not implemented")
}

func (m *JobManager) sortRunnableJobs() {
	slices.SortFunc(m.RunnableJobs, func(a, b *Job) int {
		// Return a negative number when a < b, a positive number when a > b, and 0 when a == b.

		aGpus := int(a.ResourceSpec.GPU())
		bGpus := int(b.ResourceSpec.GPU())

		return aGpus - bGpus
	})
}

// preemptJob is not thread safe.
func (m *JobManager) preemptJob(job *Job) {
	job.Status = Pending
	job.Preempt += 1

	// TODO: Implement the logic to actually preempt the job.
	panic("Not implemented")
}

// Commented out
//
// This corresponds to lines 2986 - 3004 of scheduler.py from ElasticNotebook.
// I don't think we need that part though, as they seem to just use it to figure out
// when to run simulator to for next event to be processed.
//
// That is, they figure out if the next queue jump occurs before/after the next event,
// and if it is before, then they will run simulator until the queue jump occurs, rather
// than until the next jump occurs.
//
//func (m *JobManager) performPromotionsAndDemotions() error {
//	m.mu.Lock()
//	defer m.mu.Unlock()
//
//	nextJump := time.UnixMilli(math.MaxInt32)
//	for _, job := range m.RunnableJobs {
//		if job.IsRunning() {
//			queueId := job.CurrentQueueId
//			if !m.isLastQueue(queueId) {
//				jumpTimeFloat := math.Ceil(float64((m.QueueLimits[queueId] - job.ExecutedTime) / time.Duration(job.NumGPUs())))
//				jumpTime := time.Now().Add(time.Duration(jumpTimeFloat))
//
//				if jumpTime.Before(nextJump) {
//					nextJump = jumpTime
//				}
//			}
//			continue
//		}
//
//		if job.IsPending() {
//			// When will pending jobs push back to Q0?
//			if m.StarvationLimit > 0 && job.CurrentQueueId > 0 && job.ExecutedTime > 0 {
//				timeDifference := (job.ExecutedTime * m.StarvationLimit) - job.LastPendingTime
//				if timeDifference > 0 {
//					jumpTime := time.Now().Add(timeDifference)
//
//					if jumpTime.Before(nextJump) {
//						nextJump = jumpTime
//					}
//				}
//			}
//		}
//	}
//
//	// TODO: Implement me.
//	panic("Not implemented")
//
//	return nil
//}

func (m *JobManager) Perform2DLAS() {
	err := m.updateJobs()
	if err != nil {
		m.log.Error("Encountered one or more errors while updating jobs: %v", err)
	}

	err = m.scheduleJobs()
	if err != nil {
		m.log.Error("Encountered one or more errors while scheduling jobs: %v", err)
	}
}
