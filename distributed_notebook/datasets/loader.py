from distributed_notebook.datasets.base import Dataset
from distributed_notebook.datasets.cifar10 import Cifar10, CIFAR10

value_error_contents = "cannot load dataset because dataset description does not contain a '%s' key"

def load_dataset(dataset_description: dict[str, str|bool|int])->Dataset:
    if 'name' not in dataset_description:
        raise ValueError(f"dataset description does not contain a '_name' field: {dataset_description}")

    dataset_name:str = dataset_description['name']

    if dataset_name == Cifar10:
        return CIFAR10(**dataset_description)

    raise ValueError(f"unknown or unsupported dataset \"{dataset_name}\"")