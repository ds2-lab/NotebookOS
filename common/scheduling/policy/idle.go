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

// NoIdleSessionReclamationPolicy defines a scheduling.IdleSessionReclamationPolicy for which the reclamation
// of idle sessions is entirely disabled.
type NoIdleSessionReclamationPolicy struct {
	Opts *scheduling.SchedulerOptions
}

func (n NoIdleSessionReclamationPolicy) IdleSessionReclamationEnabled() bool {
	return false
}

func (n NoIdleSessionReclamationPolicy) ReclaimedSessionsMustReplayAllCells() bool {
	return false
}

func (n NoIdleSessionReclamationPolicy) IdleSessionReclamationInterval() time.Duration {
	return time.Hour * 87600 // 10 years
}

// AdobeSenseiReclamationPolicy defines a scheduling.IdleSessionReclamationPolicy modeled after
// [Adobe Sensei's session reclamation policy], which (by default) has session timeout configured to 30 minutes.
//
// [Adobe Sensei's session reclamation policy]: https://helpx.adobe.com/adobe-connect/installconfigure/configuring-session-timeout-value.html
type AdobeSenseiReclamationPolicy struct {
	Opts *scheduling.SchedulerOptions
}

func (n AdobeSenseiReclamationPolicy) IdleSessionReclamationEnabled() bool {
	return true
}

func (n AdobeSenseiReclamationPolicy) ReclaimedSessionsMustReplayAllCells() bool {
	return true
}

func (n AdobeSenseiReclamationPolicy) IdleSessionReclamationInterval() time.Duration {
	// https://helpx.adobe.com/adobe-connect/installconfigure/configuring-session-timeout-value.html
	return time.Minute * 30
}

// GoogleColabReclamationPolicy defines a scheduling.IdleSessionReclamationPolicy modeled after
// [Google Colab's session reclamation policy], which (by default) has session timeout configured to 90 minutes.
//
// [Google Colab's session reclamation policy]: https://cloud.google.com/colab/docs/idle-shutdown
type GoogleColabReclamationPolicy struct {
	Opts *scheduling.SchedulerOptions
}

func (n GoogleColabReclamationPolicy) IdleSessionReclamationEnabled() bool {
	return true
}

func (n GoogleColabReclamationPolicy) ReclaimedSessionsMustReplayAllCells() bool {
	return true
}

func (n GoogleColabReclamationPolicy) IdleSessionReclamationInterval() time.Duration {
	// https://cloud.google.com/colab/docs/idle-shutdown
	return time.Minute * 180
}

type CustomColabReclamationPolicy struct {
	Opts *scheduling.SchedulerOptions

	timeoutInterval time.Duration

	replayAllCells bool
}

func NewCustomColabReclamationPolicy(opts *scheduling.SchedulerOptions) (*CustomColabReclamationPolicy, error) {
	policy := &CustomColabReclamationPolicy{
		Opts:            opts,
		replayAllCells:  opts.CustomIdleSessionReclamationOptions.ReplayAllCells,
		timeoutInterval: time.Second * time.Duration(opts.CustomIdleSessionReclamationOptions.TimeoutIntervalSec),
	}

	if opts.CustomIdleSessionReclamationOptions.TimeoutIntervalSec <= 0 {
		return nil, fmt.Errorf("%w: %v", ErrInvalidIdleSessionTimeoutInterval, opts.CustomIdleSessionReclamationOptions.TimeoutIntervalSec)
	}

	return policy, nil
}

func (n CustomColabReclamationPolicy) IdleSessionReclamationEnabled() bool {
	return true
}

func (n CustomColabReclamationPolicy) ReclaimedSessionsMustReplayAllCells() bool {
	panic("Not implemented")
}

func (n CustomColabReclamationPolicy) IdleSessionReclamationInterval() time.Duration {
	panic("Not implemented")
}
