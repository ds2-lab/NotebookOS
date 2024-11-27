# The `scheduling` Package

This package contains the core scheduling logic for the Distributed Notebook Cluster.

The dependency graph of the various sub-packages is shown below. This graph was generated using [goda](https://github.com/loov/goda) using the following command:
``` sh
goda graph -cluster -short "github.com/scusemua/distributed-notebook/common/scheduling..." | dot -Tsvg -o graph_scheduling_dependency.svg
```

![dependency graph of the 'scheduling' package](https://raw.githubusercontent.com/Scusemua/distributed-notebook/refs/heads/main/common/scheduling/docs/graph_scheduling_dependency.svg?token=GHSAT0AAAAAACW4ENFHHY75JLHUSYLU637UZZX234A)

## `scheduling` Package Overview

The `scheduling` package itself essentially serves as a `domain` or `shared` package. It contains a number of 
commonly-used interfaces that are referenced throughout the codebase, both in the sub-packages of `scheduling` as 
well as in other parts of the codebase.

### `client`

The `client` subpackage notably contains the Jupyter `KernelReplicaClient` and `DistributedKernelClient` structs.

### cluster 

TBD.

### entity

TBD.

### index

TBD.

### placer

TBD.

### resource

TBD.

### scheduler

TBD.