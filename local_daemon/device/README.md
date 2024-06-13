# The `device` Package 

This directory contains the `device` package, which encapsulates an implementation of [Kubernetes `DevicePlugin` interface](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins) that lives within the Local Daemon.

Useful references for implementation the [`DevicePlugin`]() interface:
- [**GPU Manager**](https://github.dev/tkestack/gpu-manager/)
- [**Intel Device Plugins for Kubernetes**](https://github.com/intel/intel-device-plugins-for-kubernetes)
- [**Official `DevicePlugin` Documentation**](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins)