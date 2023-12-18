# distributed_notebook

## Demo

    python3 -m distributed_notebook.demo distributed_notebook/demo/script/script.py distributed_notebook/demo/script/script2.py

## Jupyter Kernel

``distributed_kernel`` is a Jupyter kernel that can execute distributedly. 

### Build

Follow https://github.com/zhangjyr/gopy and execute:

~~~bash
$ python3 -m pip install pybindgen
$ go install golang.org/x/tools/cmd/goimports@latest
$ go install github.com/zhangjyr/gopy@v0.4.1
$ make 
~~~

Gopy compatibility:

- Mac Intel: **OK**
- Mac ARM: **OK**
- Other: **UNKNOWN**

### Preparation

Start jupyter container.

    cd dockfiles/jupyter
    docker-compose up -d  # docker compose up -d for new docker version on Mac.

I have prepared built SMR for mac running on ARM. To build SMR for different arch, we may need to build a local "zhangjyr/gopy" image first.

    # In other folder.
    git clone https://github.com/zhangjyr/gopy.git
    docker build -t zhangjyr/gopy .
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