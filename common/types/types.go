package types

import "context"

const (
	DockerMode     DeploymentMode = "docker"     // Running via Docker on a single system rather than using the Kubernetes-based deployment.
	KubernetesMode DeploymentMode = "kubernetes" // Running via Kubernetes rather than Docker.

	DockerNode string = "DockerNode"

	DockerContainerIdTBD string = "TBD"
)

type DeploymentMode string

type Contextable interface {
	Context() context.Context
	SetContext(context.Context)
}
