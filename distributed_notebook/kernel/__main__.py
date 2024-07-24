from ipykernel.kernelapp import IPKernelApp
from . import DistributedKernel

import faulthandler
import sys 

def tracefunc(frame, event, arg, indent=[0]):
      if event == "call":
          indent[0] += 2
          print("-" * indent[0] + "> call function", frame.f_code.co_name, flush = True)
      elif event == "return":
          print("<" + "-" * indent[0], "exit function", frame.f_code.co_name, flush = True)
          indent[0] -= 2
      return tracefunc

def main():
    # sys.setprofile(tracefunc)
    print("Driver process has started running.", flush = True)
    faulthandler.enable()
    IPKernelApp.launch_instance(kernel_class=DistributedKernel)

if __name__ == "__main__":
    main()