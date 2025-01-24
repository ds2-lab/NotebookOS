package execution

import "errors"

var (
	ErrExecutionFailedAllYielded = errors.New("an execution failed; all replicas proposed 'YIELD'")
	ErrProposalAlreadyReceived   = errors.New("we already received a Proposal from that replica")
	ErrInvalidMessage            = errors.New("execution registration must occur when an 'execute_request' or a 'yield_request'")
	ErrDuplicateExecution        = errors.New("execution already exists for given 'execute_request' or 'yield_request' message")
	ErrUnknownActiveExecution    = errors.New("no active execution found with specified message id")
)
