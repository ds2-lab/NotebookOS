package execution

import "errors"

var (
	ErrExecutionFailedAllYielded = errors.New("an execution failed; all replicas proposed 'YIELD'")
	ErrProposalAlreadyReceived   = errors.New("we already received a Proposal from that replica")
)
