package core

import "fmt"

type CoreOptions struct {
	NumReplicas        int     `name:"num-replicas" description:"Number of kernel replicas."`
	MaxSubscribedRatio float64 `name:"max-subscribed-ratio" description:"Maximum subscribed ratio."`
}

func (co CoreOptions) String() string {
	return fmt.Sprintf("NumReplicas: %d, MaxSubscribedRatio: %.4f", co.NumReplicas, co.MaxSubscribedRatio)
}
