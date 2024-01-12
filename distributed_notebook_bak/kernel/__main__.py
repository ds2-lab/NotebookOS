from ipykernel.kernelapp import IPKernelApp
from . import DistributedKernel

IPKernelApp.launch_instance(kernel_class=DistributedKernel)
