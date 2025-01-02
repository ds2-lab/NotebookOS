package policy

import (
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"time"
)

var (
	ErrInvalidIdleSessionTimeoutInterval = errors.New("invalid idle session timeout interval specified")
)

// noIdleSessionReclamationPolicy defines a scheduling.IdleSessionReclamationPolicy for which the reclamation
// of idle sessions is entirely disabled.
type noIdleSessionReclamationPolicy struct {
	opts *scheduling.SchedulerOptions
}

func (n noIdleSessionReclamationPolicy) IdleSessionReclamationEnabled() bool {
	return false
}

func (n noIdleSessionReclamationPolicy) ReclaimedSessionsMustReplayAllCells() bool {
	return false
}

func (n noIdleSessionReclamationPolicy) IdleSessionReclamationInterval() time.Duration {
	return time.Hour * 87600 // 10 years
}

// adobeSenseiReclamationPolicy defines a scheduling.IdleSessionReclamationPolicy modeled after
// [Adobe Sensei's session reclamation policy], which (by default) has session timeout configured to 30 minutes.
//
// [Adobe Sensei's session reclamation policy]: https://helpx.adobe.com/adobe-connect/installconfigure/configuring-session-timeout-value.html
type adobeSenseiReclamationPolicy struct {
	opts *scheduling.SchedulerOptions
}

func (n adobeSenseiReclamationPolicy) IdleSessionReclamationEnabled() bool {
	return true
}

func (n adobeSenseiReclamationPolicy) ReclaimedSessionsMustReplayAllCells() bool {
	return true
}

func (n adobeSenseiReclamationPolicy) IdleSessionReclamationInterval() time.Duration {
	// https://helpx.adobe.com/adobe-connect/installconfigure/configuring-session-timeout-value.html
	return time.Minute * 30
}

// googleColabReclamationPolicy defines a scheduling.IdleSessionReclamationPolicy modeled after
// [Google Colab's session reclamation policy], which (by default) has session timeout configured to 90 minutes.
//
// [Google Colab's session reclamation policy]: https://cloud.google.com/colab/docs/idle-shutdown
type googleColabReclamationPolicy struct {
	opts *scheduling.SchedulerOptions
}

func (n googleColabReclamationPolicy) IdleSessionReclamationEnabled() bool {
	return true
}

func (n googleColabReclamationPolicy) ReclaimedSessionsMustReplayAllCells() bool {
	return true
}

func (n googleColabReclamationPolicy) IdleSessionReclamationInterval() time.Duration {
	// https://cloud.google.com/colab/docs/idle-shutdown
	return time.Minute * 180
}

type customColabReclamationPolicy struct {
	opts *scheduling.SchedulerOptions

	timeoutInterval time.Duration

	replayAllCells bool
}

func newCustomColabReclamationPolicy(opts *scheduling.SchedulerOptions) (*customColabReclamationPolicy, error) {
	policy := &customColabReclamationPolicy{
		opts:            opts,
		replayAllCells:  opts.CustomIdleSessionReclamationOptions.ReplayAllCells,
		timeoutInterval: time.Second * time.Duration(opts.CustomIdleSessionReclamationOptions.TimeoutIntervalSec),
	}

	if opts.CustomIdleSessionReclamationOptions.TimeoutIntervalSec <= 0 {
		return nil, fmt.Errorf("%w: %v", ErrInvalidIdleSessionTimeoutInterval, opts.CustomIdleSessionReclamationOptions.TimeoutIntervalSec)
	}

	return policy, nil
}

func (n customColabReclamationPolicy) IdleSessionReclamationEnabled() bool {
	return true
}

func (n customColabReclamationPolicy) ReclaimedSessionsMustReplayAllCells() bool {
	panic("Not implemented")
}

func (n customColabReclamationPolicy) IdleSessionReclamationInterval() time.Duration {
	panic("Not implemented")
}
