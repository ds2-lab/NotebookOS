# distributed-notebook

## Jupyter Kernel

``distributed_kernel`` is a Jupyter kernel that can execute distributedly. 

### Installation

To install ``distributed_kernel`` from PyPI::

    pip install distributed_kernel
    python -m distributed_kernel.install

### Using the Distributed Kernel

**Notebook**: The *New* menu in the notebook should show an option for an distributed notebook.

**Console frontends**: To use it with the console frontends, add ``--kernel distributed`` to
their command line arguments.

## Jupyter docker

## ARM

pip3 install torch torchvision -f https://torch.kmtea.eu/whl/stable.html