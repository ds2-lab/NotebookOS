# Scheduling

This package contains the core scheduling logic for the Distributed Notebook Cluster.

The dependency graph of the various sub-packages is shown below. This graph was generated using [goda](https://github.com/loov/goda) using the following command:
``` sh
goda graph -cluster -short "github.com/scusemua/distributed-notebook/common/scheduling..." | dot -Tsvg -o graph_scheduling_dependency.svg
```

![dependency graph of the 'scheduling' package](graph_scheduling_dependency.svg)