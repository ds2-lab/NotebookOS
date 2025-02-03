package smr

import (
	"context"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type smrContext interface {
	context.Context

	// ID returns the context ID.
	ID() string

	// Reset resets the context with a new timeout.
	Reset(timeout time.Duration) smrContext

	// Cancel cancels the context.
	Cancel()
}

type SMRContext struct {
	context.Context
	cancel context.CancelFunc
	id     string
}

func (ctx *SMRContext) ID() string {
	return ctx.id
}

func (ctx *SMRContext) Reset(timeout time.Duration) smrContext {
	context, cancel := context.WithTimeout(context.Background(), timeout)
	ctx.Context = context
	ctx.cancel = cancel
	return ctx
}

func (ctx *SMRContext) Cancel() {
	ctx.cancel()
}

type proposalContext struct {
	SMRContext
	Proposal []byte
}

func NewProposalContext(id string, proposal []byte, timeout time.Duration) *proposalContext {
	context, cancel := context.WithTimeout(context.Background(), timeout)
	return &proposalContext{
		SMRContext: SMRContext{
			Context: context,
			id:      id,
			cancel:  cancel,
		},
		Proposal: proposal,
	}
}

type confChangeContext struct {
	SMRContext
	*raftpb.ConfChange
}

func NewConfChangeContext(id string, cc *raftpb.ConfChange, timeout time.Duration) *confChangeContext {
	context, cancel := context.WithTimeout(context.Background(), timeout)
	return &confChangeContext{
		SMRContext: SMRContext{
			Context: context,
			id:      id,
			cancel:  cancel,
		},
		ConfChange: cc,
	}
}

func (ctx *confChangeContext) ID() string {
	return ctx.SMRContext.id
}

func (ctx *confChangeContext) Reset(timeout time.Duration) smrContext {
	return ctx.SMRContext.Reset(timeout)
}
