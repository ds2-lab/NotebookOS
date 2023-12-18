package core

type CoreOptions struct {
	NumReplicas        int     `name:"num-replicas" description:"Number of kernel replicas."`
	MaxSubscribedRatio float64 `name:"max-subscribed-ratio" description:"Maximum subscribed ratio."`
}
