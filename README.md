# distributed-notebook

## Demo

    python3 -m distributed-notebook.demo distributed-notebook/demo/script/script.py distributed-notebook/demo/script/script2.py

## Jupyter Kernel

``distributed_kernel`` is a Jupyter kernel that can execute distributedly. 

### Installation

To install ``distributed_kernel`` from PyPI::

    pip3 install .
    python3 -m distributed-notebook.kernel.install

### Using the Distributed Kernel

**Notebook**: The *New* menu in the notebook should show an option for an distributed notebook.

**Console frontends**: To use it with the console frontends, add ``--kernel distributed`` to
their command line arguments.

## Jupyter docker

## ARM

python3 -m pip install torch==1.10.0 torchvision==0.11.0 -f https://torch.kmtea.eu/whl/stable.html