from ipykernel.kernelapp import IPKernelApp
from . import DistributedKernel

from multiprocessing import Process

import sys 
import os 

def proc():
    # sys.stdout = open(str(os.getpid()) + ".out", "wb", buffering = 0)
    # sys.stderr = open(str(os.getpid()) + "_error.out", "wb", buffering=0)
    print("Driver process has started running.", flush = True)
    IPKernelApp.launch_instance(kernel_class=DistributedKernel)
    
# def main():
    # process: Process = Process(target = proc)
    # process.daemon = True 
    # process.start()
    # process.join()

    # proc_pid = process.pid

    # print("Exit code: ", process.exitcode)
    # print("PID: ", proc_pid)

    # with open(str(proc_pid) + ".out", "r") as fh:
    #     stdout_lines: list[str] = fh.readlines()
    
    # with open(str(proc_pid) + "_error.out", "r") as fh:
    #     stderr_lines: list[str] = fh.readlines()
    
    # for line in stdout_lines:
    #     print(line)

    # print("\n\n\n\n\n")

    # for line in stderr_lines:
    #     print(line)

    # print("Exiting.")

if __name__ == "__main__":
    # main() 
    proc()