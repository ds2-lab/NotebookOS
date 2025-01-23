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

type Proposal struct {
	Key    ProposalKey `json:"key"`
	Reason string      `json:"reason"`
}

// NewProposal creates a new Proposal struct and returns a pointer to it.
func NewProposal(key ProposalKey, reason string) *Proposal {
	return &Proposal{
		Key:    key,
		Reason: reason,
	}
}

func (p *Proposal) GetKey() ProposalKey {
	return p.Key
}

func (p *Proposal) GetReason() string {
	return p.Reason
}

func (p *Proposal) IsYield() bool {
	return p.Key == YieldProposal
}

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
