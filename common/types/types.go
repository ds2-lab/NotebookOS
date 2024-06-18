package types

import "context"

const (
	DockerMode     DeploymentMode = "docker"     // Running via Docker on a single system rather than using the Kubernetes-based deployment.
	KubernetesMode DeploymentMode = "kubernetes" // Running via Kubernetes rather than Docker.
	LocalMode      DeploymentMode = "local"      // "Local mode" is a sort of debug mode where we're running locally rather than in Kubernetes. This will later be replaced by Docker mode, which was the original way of deploying this system.

	DockerNode string = "DockerNode" // Node name used for "docker" mode deployments.
	LocalNode  string = "LocalNode"  // Node name used for "local" mode deployments.

	DockerContainerIdTBD string = "TBD"
)

type DeploymentMode string

type Contextable interface {
	Context() context.Context
	SetContext(context.Context)
}
