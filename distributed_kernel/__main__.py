from ipykernel.kernelapp import IPKernelApp
from kernel import DistributedKernel

IPKernelApp.launch_instance(kernel_class=DistributedKernel)
