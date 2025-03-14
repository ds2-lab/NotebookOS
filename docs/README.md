# Project Architecture - Docker Swarm

The following diagram reflects the current system architecture when the system is deployed atop a Docker Swarm cluster.

To see the architecture of the `Global Scheduler` component specifically, please refer to the architecture diagram 
shown in the `README.md` located in `<root_directory>/gateway/README.md`.

![Docker Swarm System Architecture Diagram](docker_swarm_arch.png)

# Project Architecture - Kubernetes

The following diagram reflects the current system architecture when the system is deployed atop a Kubernetes cluster.

Note that the Kubernetes-based deployment mode has (temporarily) fallen out of favor and is not as up-to-date as the
Docker Swarm deployment mode.

![Kubernetes System Architecture Diagram](k8s_arch.png)

# Socket Connections

The following diagram displays how all the components are connected together via ZMQ sockets.

![Socket Connection Diagram](socket_connections.png)