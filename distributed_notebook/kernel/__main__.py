from ipykernel.kernelapp import IPKernelApp
from . import DistributedKernel

from distributed_notebook.logs import ColoredLogFormatter

import logging

# Create the custom formatter
colored_log_formatter: ColoredLogFormatter = ColoredLogFormatter()

# Create a StreamHandler and set the custom formatter
handler = logging.StreamHandler()
handler.setFormatter(colored_log_formatter)

# Get the root logger and add the handler
root_logger = logging.getLogger()
# root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(handler)

import faulthandler

def main():
    print("DistributedKernel: main()", flush = True)
    faulthandler.enable()
    IPKernelApp.launch_instance(kernel_class=DistributedKernel)

if __name__ == "__main__":
    main()