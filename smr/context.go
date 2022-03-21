package smr

import (
	"context"
	"time"
)

type proposalContext struct {
	context.Context
	Id          string
	Proposal    []byte
	Cancel      context.CancelFunc
	chCallbacks chan func()
}

func ProposalContext(id string, proposal []byte, timeout time.Duration) *proposalContext {
	context, cancel := context.WithTimeout(context.Background(), timeout)
	return &proposalContext{
		Context:     context,
		Id:          id,
		Proposal:    proposal,
		Cancel:      cancel,
		chCallbacks: make(chan func()),
	}
}

func (ctx *proposalContext) Reset(timeout time.Duration) *proposalContext {
	context, cancel := context.WithTimeout(context.Background(), timeout)
	ctx.Context = context
	ctx.Cancel = cancel
	return ctx
}

func (ctx *proposalContext) Trigger(cb func()) {
	ctx.chCallbacks <- cb
}

func (ctx *proposalContext) Callbacks() <-chan func() {
	return ctx.chCallbacks
}
