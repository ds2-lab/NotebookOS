package types

import "context"

const (
	DockerComposeMode DeploymentMode = "docker-compose" // Running via Docker Compose on a single system rather than using the Kubernetes-based deployment.
	DockerSwarmMode   DeploymentMode = "docker-swarm"   // Running via Docker Swarm on one or more machines.
	KubernetesMode    DeploymentMode = "kubernetes"     // Running via Kubernetes rather than Docker.
	LocalMode         DeploymentMode = "local"          // "Local mode" is a sort of debug mode where we're running locally rather than in Kubernetes. This will later be replaced by Docker mode, which was the original way of deploying this system.

	VirtualDockerNode string = "VirtualDockerNode" // Node name used for "docker compose" deployments.
	DockerNode        string = "DockerNode"        // Node name used for "docker swarm" mode deployments.
	KubernetesNode    string = "KubernetesNode"    // Node name used for "kubernetes" mode deployments.
	LocalNode         string = "LocalNode"         // Node name used for "local" mode deployments.

	DockerContainerIdTBD string = "TBD"
)

type DeploymentMode string

func (d DeploymentMode) IsDockerMode() bool {
	return d == DockerComposeMode || d == DockerSwarmMode
}

func (d DeploymentMode) String() string {
	return string(d)
}

type Contextable interface {
	Context() context.Context
	SetContext(context.Context)
}
