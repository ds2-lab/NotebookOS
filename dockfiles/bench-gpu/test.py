import numpy as np
import cupy as gnp

# to measure exec time
from timeit import default_timer as timer

# normal function to run on cpu
def func(a, b):
    return a.dot(b)

if __name__=="__main__":
    n = 10000
    cpu = False
    iter = 100

    if cpu:
        a = np.random.random_sample((n,n))
        b = np.random.random_sample((n,n))

    ga = gnp.random.random_sample((n,n))
    gb = gnp.random.random_sample((n,n))
    gnp.cuda.Stream.null.synchronize()
    print("Initialized")

    if cpu:
        start = timer()
        ret = func(a, b)
        print("without GPU:", timer()-start)

    start = timer()
    for i in range(iter):
        gret = func(ga, gb)
    gnp.cuda.Stream.null.synchronize()
    print("with GPU:", timer()-start)