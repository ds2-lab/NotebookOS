from ipykernel.kernelapp import IPKernelApp
from . import DistributedKernel

import faulthandler

def main():
    print("Driver process has started running.", flush = True)
    faulthandler.enable()
    IPKernelApp.launch_instance(kernel_class=DistributedKernel)

if __name__ == "__main__":
    main()