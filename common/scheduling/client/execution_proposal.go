package client

import (
	"encoding/json"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

// Proposal encapsulates a "YIELD" or "LEAD" proposal issued by a kernel replica
// during the Raft-based "primary replica selection" protocol.
type Proposal struct {
	// Key defines what type of Proposal this is -- either a YieldProposal or a LeadProposal.
	Key scheduling.ProposalKey `json:"key"`

	// Reason provides an explanation for why the Key was proposed.
	Reason string `json:"reason"`
}

// NewProposal creates a new Proposal struct and returns a pointer to it.
func NewProposal(key scheduling.ProposalKey, reason string) *Proposal {
	return &Proposal{
		Key:    key,
		Reason: reason,
	}
}

func (p *Proposal) GetKey() scheduling.ProposalKey {
	return p.Key
}

func (p *Proposal) GetReason() string {
	return p.Reason
}

// IsYield returns true if the target Proposal is a YieldProposal.
func (p *Proposal) IsYield() bool {
	return p.Key == scheduling.YieldProposal
}

// IsLead returns true if the target Proposal is a LeadProposal.
func (p *Proposal) IsLead() bool {
	return p.Key == scheduling.LeadProposal
}

func (p *Proposal) String() string {
	m, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}

	return string(m)
}
