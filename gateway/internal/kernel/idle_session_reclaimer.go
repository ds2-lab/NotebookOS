package kernel

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"sync/atomic"
	"time"
)

type IdleReclaimFunction func(kernel scheduling.Kernel, inSeparateGoroutine bool, noop bool) error

type IdleSessionReclaimer struct {
	// Kernels is a map from kernel and session IDs to Kernels.
	// There may be duplicate values (i.e., multiple sessions mapping to the same kernel).
	kernels hashmap.HashMap[string, scheduling.Kernel]

	// interval is the interval of real-life clock time that must elapse before a Session
	// is considered idle and is eligible for reclamation of IdleSessionReclamationEnabled is set to true.
	//
	// If interval is set to 0, then idle session reclamation is disabled, regardless of the
	// value of the IdleSessionReclamationEnabled flag.
	interval time.Duration

	// closed is used to signal/indicate that the IdleSessionReclaimer should stop.
	closed atomic.Int32

	// running indicates whether the IdleSessionReclaimer is running.
	running atomic.Int32

	idleReclaim IdleReclaimFunction

	numReplicasPerKernel int

	log logger.Logger
}

func NewIdleSessionReclaimer(kernels hashmap.HashMap[string, scheduling.Kernel], interval time.Duration,
	numReplicasPerKernel int, idleReclaim IdleReclaimFunction) *IdleSessionReclaimer {

	reclaimer := &IdleSessionReclaimer{
		kernels:              kernels,
		interval:             interval,
		numReplicasPerKernel: numReplicasPerKernel,
		idleReclaim:          idleReclaim,
	}

	config.InitLogger(&reclaimer.log, reclaimer)

	return reclaimer
}

func (r *IdleSessionReclaimer) Close() {
	r.closed.Store(1)
}

func (r *IdleSessionReclaimer) Start() {
	go r.run()
}

func (r *IdleSessionReclaimer) reclaimKernel(kernel scheduling.Kernel, iteration int64) error {
	reclamationStartTime := time.Now()

	err := r.idleReclaim(kernel, false, false)

	if err != nil {
		r.log.Error("Error while removing replicas of idle kernel \"%s\" [iter=%d]: %v",
			kernel.ID(), iteration, err)
		return err
	}

	r.log.Debug(
		utils.LightPurpleStyle.Render(
			"Successfully removed all %d replica(s) of idle kernel \"%s\" in %v. [iter=%d]"),
		r.numReplicasPerKernel, kernel.ID(), time.Since(reclamationStartTime), iteration)
	r.log.Debug("Status of idle-reclaimed kernel \"%s\": %v [iter=%d]",
		kernel.ID(), kernel.Status(), iteration)
	r.log.Debug("Aggregate busy status of idle-reclaimed kernel \"%s\": %v [iter=%d]",
		kernel.ID(), kernel.AggregateBusyStatus(), iteration)

	return nil
}

// reclamationWorker is intended to be executed in a separate goroutine.
//
// reclamationWorker repeatedly polls the workQueue chan until it is empty.
func (r *IdleSessionReclaimer) reclamationWorker(workQueue chan scheduling.Kernel, iteration int64) {
	for {
		select {
		case kernel := <-workQueue:
			{
				err := r.reclaimKernel(kernel, iteration)

				if err != nil {
					r.log.Error("Failed to idle-reclaim kernel \"%s\" during idle iteration %d because: %v",
						kernel.ID(), iteration, err)
				}
			}
		default:
			{
				return
			}
		}
	}
}

func (r *IdleSessionReclaimer) reclaimKernels(kernelsToReclaim []scheduling.Kernel, iteration int64) {
	numKernelsToReclaim := len(kernelsToReclaim)

	r.log.Debug("Identified %d idle kernel(s) to reclaim. [iteration=%d]",
		numKernelsToReclaim, iteration)

	if numKernelsToReclaim == 0 {
		return
	}

	if numKernelsToReclaim == 1 {
		kernel := kernelsToReclaim[0]

		r.log.Debug("Using 1 worker to remove a single idle kernel (kernel \"%s\") in idle iteration %d.",
			kernel.ID(), iteration)

		go func() {
			err := r.reclaimKernel(kernel, iteration)

			if err != nil {
				r.log.Error("Failed to idle-reclaim kernel \"%s\" during idle iteration %d because: %v",
					kernel.ID(), iteration, err)
			}
		}()

		return
	}

	// We'll use multiple goroutines if there are 2 or more Kernels to remove.
	workQueue := make(chan scheduling.Kernel, numKernelsToReclaim)
	for _, kernel := range kernelsToReclaim {
		workQueue <- kernel
	}

	var nWorkers int
	if numKernelsToReclaim <= 8 {
		nWorkers = numKernelsToReclaim / 2
	} else {
		nWorkers = numKernelsToReclaim / 4
	}

	r.log.Debug("Spawning %d workers to remove %d idle kernels in idle iteration %d.",
		nWorkers, numKernelsToReclaim, iteration)

	for i := 0; i < nWorkers; i++ {
		go r.reclamationWorker(workQueue, iteration)
	}
}

// run runs a loop and searches for scheduling.Kernel instances that are idle.
func (r *IdleSessionReclaimer) run() {
	if !r.running.CompareAndSwap(0, 1) {
		r.log.Warn("Idle session reclaimer is already running.")
		return
	}

	defer r.running.CompareAndSwap(1, 0)

	// Validate that we're supposed to run in the first place.
	if r.interval <= 0 {
		r.log.Warn("Idle session reclamation is NOT enabled. Exiting.")
		return
	}

	// Run every 1/60th of the reclamation interval, with the limit being once a second.
	// So, if sessions are reclaimed after 30 minutes of being idle, then this goroutine
	// runs every 30 seconds (1,800 seconds / 60 = 30 seconds).
	frequency := r.interval / 60
	if frequency < time.Second {
		frequency = time.Second
	}

	r.log.Debug("Idle Session Reclaimer initialized with frequency=%v and reclamation_interval=%v",
		frequency, r.interval)

	var iteration atomic.Int64
	iteration.Store(1)

	// Keep running until the Cluster Gateway is stopped.
	for r.closed.Load() == 0 {
		startTime := time.Now()

		kernelsToReclaim := r.identifyIdleSessions()

		// Make sure we haven't closed before we start doing this.
		if r.closed.Load() > 0 {
			return
		}

		if len(kernelsToReclaim) > 0 {
			r.reclaimKernels(kernelsToReclaim, iteration.Load())
		}

		// Sleep until we're supposed to run again.
		//
		// If the amount of time we spent checking if the Kernels are idle and reclaiming the idle Kernels
		// is greater than our frequency interval, then we will skip sleeping.
		//
		// Otherwise, we'll sleep for our frequencyInterval - timeElapsed.
		//
		// For example, if we're supposed to run every 5,000ms, and we spent 125ms on checking + reclaiming, then
		// we'll sleep for 5,000ms - 125ms = 4,875ms.
		//
		// If, as another example, we spent 6,500ms checking + reclaiming, then we'll just skip the sleep and
		// immediately check again.
		//
		// If we just check and find no idle Kernels, then the loop will be very quick. If we have to reclaim any
		// idle Kernels, though, then it could take a lot longer. That's why we have this check.
		timeElapsed := time.Since(startTime)
		timeRemaining := frequency - timeElapsed
		if timeRemaining > 0 {
			time.Sleep(timeRemaining)
		}

		iteration.Add(1)
	}
}

// identifyIdleSessions is used by the idle session reclamation goroutine to identify idle sessions.
func (r *IdleSessionReclaimer) identifyIdleSessions() []scheduling.Kernel {
	var kernelsToReclaim []scheduling.Kernel

	// Keep track of which Kernels we've seen, as there may be duplicates in the Kernels map.
	kernelsSeen := make(map[string]scheduling.Kernel)

	// This locks the Kernels map, so we'll just copy all the Kernels to this other map while
	// also removing any duplicates.
	r.kernels.Range(func(kernelId string, kernel scheduling.Kernel) (contd bool) {
		// If we've already seen this kernel, then skip it.
		// The Kernels map may have duplicate values, such as when multiple sessions map to the same kernel.
		if _, loaded := kernelsSeen[kernel.ID()]; loaded {
			return true
		}

		// Record that we've now seen this kernel.
		kernelsSeen[kernel.ID()] = kernel
		return true
	})

	// Now we can iterate without locking the Kernels map.
	for kernelId, kernel := range kernelsSeen {
		// If the kernel is already de-scheduled -- if its replicas are not scheduled -- then skip over it.
		if kernel.Status() != jupyter.KernelStatusRunning || kernel.IsIdleReclaimed() || !kernel.ReplicasAreScheduled() {
			continue
		}

		// If the kernel's containers are actively being scheduled right now, then we shouldn't reclaim it.
		// Likewise, if they're actively being removed right now -- or if the kernel is being shut down right
		// now -- then we shouldn't reclaim it.
		_, removalAttempt := kernel.ReplicaContainersAreBeingRemoved()
		if kernel.ReplicaContainersAreBeingScheduled() || removalAttempt != nil || kernel.IsShuttingDown() {
			continue
		}

		// If the kernel is actively training (as in, it is literally executing code, or the client has submitted
		// code to be executed but the kernel has not necessarily started executing code yet), then we should not
		// reclaim this kernel.
		if kernel.HasActiveTraining() {
			continue
		}

		// Check if the kernel is idle and, if it is, then add it to the slice of Kernels to be reclaimed.
		timeElapsedSinceLastTrainingSubmitted := time.Since(kernel.LastTrainingSubmittedAt())
		timeElapsedSinceLastTrainingBegan := time.Since(kernel.LastTrainingStartedAt())
		timeElapsedSinceLastTrainingEnded := time.Since(kernel.LastTrainingEndedAt())
		timeElapsedSinceContainersCreated := time.Since(kernel.ReplicaContainersStartedAt())

		// May want to use this to dynamically adjust the interval required for a kernel to be considered idle.
		multiplier := 1.0

		// If the kernel has never trained before, then we increase the interval by a factor of 1.5.
		if kernel.NumCompletedTrainings() == 0 {
			multiplier = 1.5
		}

		// Compute how long it has been since the kernel last submitted a training event, last began training,
		// and last completed training. If the idle interval has elapsed for all of these times, then the
		// session is eligible for idle reclamation.

		if timeElapsedSinceLastTrainingSubmitted < time.Duration(float64(r.interval)*multiplier) {
			// Skip this container
			continue
		}

		if timeElapsedSinceLastTrainingBegan < time.Duration(float64(r.interval)*multiplier) {
			// Skip this container
			continue
		}

		if timeElapsedSinceLastTrainingEnded < time.Duration(float64(r.interval)*multiplier) {
			// Skip this container
			continue
		}

		if timeElapsedSinceContainersCreated < time.Duration(float64(r.interval)*multiplier) {
			// Skip this container
			continue
		}

		r.log.Debug(
			utils.LightPurpleStyle.Render("Kernel \"%s\" last submitted a training event %v ago, last began training %v ago, "+
				"and last finished training %v ago, and its replica container(s) were created %v ago, so kernel \"%s\" is now eligible for idle reclamation."),
			kernelId, timeElapsedSinceLastTrainingSubmitted, timeElapsedSinceLastTrainingBegan,
			timeElapsedSinceLastTrainingEnded, timeElapsedSinceContainersCreated, kernel.ID())

		if kernelsToReclaim == nil {
			kernelsToReclaim = make([]scheduling.Kernel, 0, 1)
		}

		kernelsToReclaim = append(kernelsToReclaim, kernel)

		continue
	}

	return kernelsToReclaim
}
