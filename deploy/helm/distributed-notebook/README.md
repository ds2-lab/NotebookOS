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

To install this Helm chart, we must first install a third-party provisioner that supports dynamic provisioning of `local` volumes. 

We are using [Local Path Provisioner](https://github.com/rancher/local-path-provisioner/tree/master). To install this on your Kubernetes cluster, execute the following:
``` sh
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.26/deploy/local-path-storage.yaml
```

Next, to install the Helm chart, execute the following command:
``` sh
helm install distributed-notebook ./distributed-notebook/ --create-namespace
```

# Uninstalling

To uninstall the Helm chart, execute the following command:
``` sh
helm uninstall distributed-notebook
```

If you'd like to install [Local Path Provisioner](https://github.com/rancher/local-path-provisioner/tree/master), then please refer to the uninstallation instructions available [here](https://github.com/rancher/local-path-provisioner/tree/master).