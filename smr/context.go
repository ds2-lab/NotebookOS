package smr

import (
	"context"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	defaultTimeout = time.Second * 60
)

type SmrContext interface {
	context.Context

	// ID returns the context ID.
	ID() string

	// Reset resets the context with a new timeout.
	Reset(timeout time.Duration) SmrContext

	// ResetWithPreviousTimeout resets the context with the same timeout it had originally/previously.
	ResetWithPreviousTimeout() SmrContext

	// Cancel cancels the context.
	Cancel()
}

type SMRContext struct {
	context.Context
	id     string
	cancel context.CancelFunc

	Timeout time.Duration
}

func (ctx *SMRContext) ID() string {
	return ctx.id
}

func (ctx *SMRContext) Reset(timeout time.Duration) SmrContext {
	_ctx, cancel := context.WithTimeout(context.Background(), timeout)
	ctx.Context = _ctx
	ctx.cancel = cancel
	return ctx
}

func (ctx *SMRContext) ResetWithPreviousTimeout() SmrContext {
	if ctx.Timeout == 0 {
		ctx.Timeout = defaultTimeout
	}

	_ctx, cancel := context.WithTimeout(context.Background(), ctx.Timeout)
	ctx.Context = _ctx
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
	_ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return &proposalContext{
		SMRContext: SMRContext{
			Context: _ctx,
			Timeout: timeout,
			id:      id,
			cancel:  cancel,
		},
		Proposal: proposal,
	}
}

type ConfChangeContext struct {
	SMRContext
	*raftpb.ConfChange
}

func NewConfChangeContext(id string, cc *raftpb.ConfChange, timeout time.Duration) *ConfChangeContext {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return &ConfChangeContext{
		SMRContext: SMRContext{
			Context: ctx,
			Timeout: timeout,
			id:      id,
			cancel:  cancel,
		},
		ConfChange: cc,
	}
}

func (ctx *ConfChangeContext) ID() string {
	return ctx.SMRContext.id
}

func (ctx *ConfChangeContext) Reset(timeout time.Duration) SmrContext {
	return ctx.SMRContext.Reset(timeout)
}

func (ctx *ConfChangeContext) ResetWithPreviousTimeout() SmrContext {
	if ctx.Timeout == 0 {
		ctx.Timeout = defaultTimeout
	}

	return ctx.SMRContext.Reset(ctx.Timeout)
}
