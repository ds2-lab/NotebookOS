package types

import "context"

type Contextable interface {
	Context() context.Context
	SetContext(context.Context)
}
