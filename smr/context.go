package smr

import (
	"context"
	"time"
)

type proposalContext struct {
	context.Context
	Id       string
	Proposal []byte
	Cancel   context.CancelFunc
}

func ProposalContext(id string, proposal []byte, timeout time.Duration) *proposalContext {
	context, cancel := context.WithTimeout(context.Background(), timeout)
	return &proposalContext{
		Context:  context,
		Id:       id,
		Proposal: proposal,
		Cancel:   cancel,
	}
}

func (ctx *proposalContext) Reset(timeout time.Duration) *proposalContext {
	context, cancel := context.WithTimeout(context.Background(), timeout)
	ctx.Context = context
	ctx.Cancel = cancel
	return ctx
}
