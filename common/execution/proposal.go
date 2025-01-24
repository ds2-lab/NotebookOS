package execution

import "encoding/json"

var (
	// LeadProposal is issued by a kernel replica when it would like to execute the user-submitted code.
	LeadProposal ProposalKey = "LEAD"

	// YieldProposal is issued by a kernel replica when it would like to defer the execution of
	// the user-submitted code to another kernel replica.
	YieldProposal ProposalKey = "YIELD"
)

type ProposalKey string

func (pk ProposalKey) String() string {
	return string(pk)
}

// Proposal encapsulates a "YIELD" or "LEAD" proposal issued by a kernel replica
// during the Raft-based "primary replica selection" protocol.
type Proposal struct {
	// Key defines what type of Proposal this is -- either a YieldProposal or a LeadProposal.
	Key ProposalKey `json:"key"`

	// Reason provides an explanation for why the Key was proposed.
	Reason string `json:"reason"`
}

// NewProposal creates a new Proposal struct and returns a pointer to it.
func NewProposal(key ProposalKey, reason string) *Proposal {
	return &Proposal{
		Key:    key,
		Reason: reason,
	}
}

// IsYield returns true if the target Proposal is a YieldProposal.
func (p *Proposal) IsYield() bool {
	return p.Key == YieldProposal
}

// IsLead returns true if the target Proposal is a LeadProposal.
func (p *Proposal) IsLead() bool {
	return p.Key == LeadProposal
}

func (p *Proposal) String() string {
	m, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}

	return string(m)
}
