# Device Plugin for Virtual GPU Resources "oGPU"

This directory contains the code for a Kubernetes [device plugin](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/) for _virtual GPUs_. Virtual GPUs are used to implement a notion of oversubscription when scheduling GPU-enabled Pods onto GPU-enabled nodes. 

Consider a scenario in which there is a fixed oversubscription limit of 3. Assume that the nodes each have 8 _actual_ GPUs available. The number of virtual GPUs will then be 3 $\times$ 8 = 24 _virtual_ GPUs.

You may also configure an additional multiplier. This multiplier may be useful in scenarios where you are replicating some entity that consumes GPU resources. If each entity has 3 replicas, then you may set this additional multiplier to 3, creating a total of 3 $\times$ 8 $\times$ 3 = 72 _virtual_ GPUs per node (where each node has 8 _actual_ GPUs).

## References:
- [`squat/generic-device-plugin`](https://github.com/squat/generic-device-plugin/tree/main)
- [`intel/intel-device-plugins-for-kubernetes`](https://github.com/intel/intel-device-plugins-for-kubernetes/tree/main)
- [`kubevirt/device-plugin-manager`](https://github.com/kubevirt/device-plugin-manager/tree/master)
- [`ROCm/k8s-device-plugin`](https://github.com/ROCm/k8s-device-plugin/tree/master)
- [`GoogleCloudPlatform/container-engine-accelerators/cmd/nvidia_gpu`](https://github.com/GoogleCloudPlatform/container-engine-accelerators/tree/master/cmd/nvidia_gpu)