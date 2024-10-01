# `distributed-notebook` Helm Chart

This directory contains a [Helm](https://helm.sh/) chart for the `distributed-kernel` application.

## What is a Helm chart?

A [Helm](https://helm.sh/) chart contains all the required resources to deploy an application to a Kubernetes cluster, including YAML configuration files for deployments, services, secrets, and config maps that define the target state of the application.

### Helm Chart Overview and Organization

The `./Chart.yaml` file contains metadata for the Helm chart.

The `./charts/` directory contains additional components that must be installed in the application environment.

The `./templates/` directory contains all of the `YAML` artifacts that will be deployed into/onto the Kubernetes cluster.

The `./templates/NOTES.txt` file contains the output that will be shown if the deployment of the Helm chart is successful.

The `./templates/_helpers.tpl` file is a _template file_, which will help pull metadata or run functions for the deployment. It is highly extensible.

The `./templates/tests/` directory contains tests for the deployed artifacts. These tests are the executed last by Helm; as such, they are used by Helm to validate that the deployment of the chart was successful.

The `./values.yaml` file specifies default values to be used within the templates. These default values enable the templates to be consistent while retaining their dynamic, configurable nature.

# Installation

For now, these instructions are written specifically for use with a [kind](https://kind.sigs.k8s.io/) Kubernetes cluster. That being said, they should largely apply to other types of Kubernetes clusters as well.

## Creating the `kind` Kubernetes Cluster

To create the `kind` Kubernetes cluster, execute the following command:

```sh
kind create cluster --config deploy/kind/kind-config.yaml
```

You can use a different `kind` cluster config file if desired; however, it will not be guaranteed to work if you use settings that differ from those in the provided configuration file.

## Cluster Configuration

The `distributed-notebook` project uses a State Machine Replication (SMR) protocol to provide data availability and fault tolerance. This protocol assigns an `SMR Node ID` to each distributed kernel replica. These IDs are expected to begin with 1 rather than 0. The distributed kernel replicas for each user session are deployed as a Kubernetes `StatefulSet` resource. Per the [Kubernetes documentation](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/): for a `StatefulSet` with _N_ replicas, each `Pod` in the `StatefulSet` will be assigned an integer ordinal that is unique over the `StatefulSet`. By default, pods will be assigned ordinals from 0 up through _N_-1.

We use each distributed kernel `Pod`'s ordinal as its SMR node ID. But because SMR node IDs are expected to begin with 1, we need to enable a special feature within Kubernetes that adjusts the starting index for the distributed kernel `StatefulSet` resources. For instructions on how to do this, please refer to the Kubernetes documentation available [here](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#start-ordinal). Specifically, the `StatefulSetStartOrdinal` feature gate must be enabled.

## Chart Installation Instructions

### Local Path Provisioner

To install this Helm chart, we must first install a third-party provisioner that supports dynamic provisioning of `local` volumes.

We are using [Local Path Provisioner](https://github.com/rancher/local-path-provisioner/tree/master). To install this on your Kubernetes cluster, execute the following:

```sh
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.26/deploy/local-path-storage.yaml
```

### Metrics Server

**This is now installed automatically.**

Next, we install the `metrics-server`. With kind, we either need to enable `--kubelet-insecure-tls`, or we can configure our kind cluster differently. See [this post](https://www.zeng.dev/post/2023-kubeadm-enable-kubelet-serving-certs/) for how to configure.

For now, we will use the `--kubelet-insecure-tls` solution with the included `.yaml` file (found at `deploy/helm/distributed-notebook/metrics-server.yaml`):

```sh
kubectl apply -f distributed-notebook/depend/metrics-server.yaml
```

### Load Balancer 

``` sh
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.13.7/config/manifests/metallb-native.yaml
``` 

Execute:
``` sh
docker network inspect -f '{{.IPAM.Config}}' kind
```

or if you're using podman 4.0 or higher in rootful mode with the netavark network backend, use the following command instead:
``` sh
podman network inspect -f '{{range .Subnets}}{{if eq (len .Subnet.IP) 4}}{{.Subnet}}{{end}}{{end}}' kind
```

The output will contain a cidr such as 172.18.0.0/16. Modify the `deploy/helm/distributed-notebook/metallb-config.yaml` file accordingly, then execute:
``` sh
kubectl apply -f distributed-notebook/metallb-config.yaml
```

### OpenKruise and the CloneSet Resource

**This is now installed automatically.**

We also need the `CloneSet` resource provided by OpenKruise. To install this on your Kubernetes cluster, execute the following commands:

```sh
# Firstly add openkruise charts repository if you haven't do this.
helm repo add openkruise https://openkruise.github.io/charts/

# [Optional]
helm repo update

# Install the latest version.
helm install kruise openkruise/kruise --version 1.5.2
```

### The `DistributedNotebook` Helm Chart

Next, to install the Helm chart, execute the following command:

```sh
helm install distributed-notebook ./distributed-notebook/ --create-namespace
```

# Uninstalling

To uninstall the Helm chart, execute the following command:

```sh
helm uninstall distributed-notebook
```

If you'd like to install [Local Path Provisioner](https://github.com/rancher/local-path-provisioner/tree/master), then please refer to the uninstallation instructions available [here](https://github.com/rancher/local-path-provisioner/tree/master).
