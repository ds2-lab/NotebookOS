# distributed_notebook

## Demo

    python3 -m distributed_notebook.demo distributed_notebook/demo/script/script.py distributed_notebook/demo/script/script2.py

## Jupyter Kernel

``distributed_kernel`` is a Jupyter kernel that can execute distributedly. 

### Build

Follow https://github.com/scusemua/gopy and execute:

~~~bash
$ python3 -m pip install pybindgen
$ go install golang.org/x/tools/cmd/goimports@latest
$ go install github.com/scusemua/gopy@go-python-master
$ make 
~~~

Gopy compatibility:

- `Mac Intel`: **PREVIOUSLY OK, NOW UNKNOWN**
- `Mac ARM`: **PREVIOUSLY OK, NOW UNKNOWN**
- `Linux AMD`: **OK**
- `Windows 10`: **BUILD ISSUES**
- `Other`: **UNKNOWN**

### Preparation

Start jupyter container.

    cd dockfiles/jupyter
    docker-compose up -d  # docker compose up -d for new docker version on Mac.

I have prepared built SMR for mac running on ARM. To build SMR for different arch, we may need to build a local "scusemua/gopy" image first.

    # In other folder.
    git clone https://github.com/scusemua/gopy.git
    docker build -t scusemua/gopy .
    # Go back to this folder.

    # For mac using ARM
    make build-smr-linux-arm64

    # For other using x86
    make build-smr-linux-amd64

### Installation

Login to container

    docker-compose exec Python /bin/bash  # "docker compose" again for new docker version on Mac.

Install Pytorch

    # For mac using ARM
    python3 -m pip install torch==1.10.0 torchvision==0.11.0 -f https://torch.kmtea.eu/whl/stable.html

    # For x86
    python3 -m pip install torch torchvision

Install ``distributed_kernel`` from PyPI::

    cd distributed-notebook
    ./install_kernel.sh

### Using the Distributed Kernel

**Notebook**: Go to http://localhost:8888/ , log in. The *New* menu in the notebook should show an option for an distributed notebook.

**Console frontends**: To use it with the console frontends, add ``--kernel distributed`` to
their command line arguments.

### Try Demo

In http://localhost:8888/, open training-test.ipynb, training-test2.ipynb, training-test3.ipynb. If any of the opened notebooks shows "loading" in browser, refresh that notebook page. Now it is ready to play.

# Ben Note:
I will fully update this README at some point. For now, I rebuild the SMR code on Linux AMD64 via:
``` sh
sudo env PATH=$PATH make build-linux-amd64
```
from the SMR directory. Note that you need to have certain environment variables set correctly. I've written some code to try to resolve these automatically, by the `PYTHON3_LIB_PATH` resolves to a slightly incorrect directory (for my Conda-based installation of Python), so I hard-coded it for now. 

These environment variables are set in `/home/bcarver2/go/distributed-notebook/distributed_notebook/env.mk`.

Resolving them on Mac/OSx/Darwin-based systems is different than Linux/AMD64. For now, they're set to AMD64 in that `env.mk` file. Swap the uncommented lines with the commented lines if you want to use a Mac/Darwin-based system.

Again, the `PYTHON3_LIB_PATH` is hard-coded right now, at least for Linux/AMD64 systems. It needs to be the directory that has the `libpython3.11.so`, `libpython3.11.so.1.0`, and `libpython3.so` files in it. For me, this is `~/miniconda3/lib/`. 

After running `sudo env PATH=$PATH make build-linux-amd64` in the `smr/` directory, I go back to the root project directory and execute:
``` sh
sudo make build-smr-linux-amd64
```

The command in the `smr/` directory does something along the lines of regenerate the Python/Golang/C bindings. The command from the root directory compiles the generated Go code. Something like that.

## Common Errors:

### glibc Issues
```
./gateway: /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.34' not found (required by ./gateway)
```

If you see the above error, then try rebuilding *all* Docker images locally, including the base image. Specifically, rebuild these images (in roughly this order):
- `dockfiles/base-image`
- `dockfiles/cpu-python3-amd64` (or `dockfiles/cpu-python3-arm64`, depending on your CPU architecture)
- `dockfiles/gateway`
- `dockfiles/local_daemon`

Once rebuilt, you can redeploy the application and see if that issue is resolved. If the error persists, then you may have an incompatible glibc version installed locally (compared to the glibc version found within the Docker images).

You can see the version number of your locally-installed glibc via `ldd --version`. You can see the version installed within the Docker containers by running one of the containers and executing that command:
``` sh
docker run -it --entrypoint /bin/bash <container id>
ldd --version
```

At the time of writing this, the glibc version found in the Gateway and Local Daemon Docker containers is: `ldd (Debian GLIBC 2.36-9+deb12u7) 2.36`

The version found within the Jupyter Docker container is: `ldd (Ubuntu GLIBC 2.35-0ubuntu3.8) 2.35`

### Kernel Replcia Containers/Pods Crashing with No Error Messages in Logs (Exit Code 134)

If the container/pod exits with error code 134, then this indicates that the application terminated with the 'Aborted' signal (SIGABRT) -- at least in the case of Docker containers. This indicates that there was a critical error during the application's execution. 

It seems common that, if such an error occurs within the Golang code of kernel, then it will often exit without flushing everything to `stdout`/`stderr` (whic is weird, because I thought `stderr` was typically unbuffered?). In any case, if you're observing crashes with nothing helpful in the logs, then common issues may be that the kernels are failing to connect to HDFS or each other (i.e., network connectivity issues of some kind). There may also be some other kind of issue within the Golang code that is causing the kernel to crash.

Ensure that the HDFS hostname being passed to the kernels is (a) correct, and (b) usable from within the container/pod. For example, if you're running HDFS on your host VM and the kernels in Docker containers on that VM, then they can probably connect via something like `172.0.17.1:9000` if the network mode of the container is bridge (`--network 'bridge'`, or you deployed the cluster using `docker compose`, which creates a default network). If you use such a hostname without the proper network configuration, then the HDFS connection will fail.

### `dial unix /var/run/docker.sock: connect: permission denied`

If you get a `panic` in the Cluster Gateway container with that as the reason, then there are several possibilities.

One possibility is that the permissions on your system's Docker socket (`/var/run/docker.sock`) are too restrictive. This can be changed via `chmod 666 /var/run/docker.sock`, though note that this is a significant security risk.

Another possibility is that the `docker` group within the container does not have the same ID as the `docker` group on your host system. To address this, first rebuild the "base" Docker image and then rebuild the Cluster Gateway's Docker image. (Note that this likely won't work if you already built these images locally. That is, this may happen if you use the images available on Dockerhub, but if you built them locally already, then this likely won't address the issue.)

The base image is located at `dockfiles/base-image` while the Cluster Gateway's image is located at `dockfiles/gateway`.

See [this](https://stackoverflow.com/a/41574919/5937661) StackOverflow post for details.