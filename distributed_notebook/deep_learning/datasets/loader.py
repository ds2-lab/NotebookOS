from typing import Optional, Type

from distributed_notebook.deep_learning.datasets import ALL_DATASET_CLASSES
from distributed_notebook.deep_learning.datasets.custom_dataset import CustomDataset

value_error_contents = "cannot load dataset because dataset description does not contain a '%s' key"

def load_dataset(dataset_description: dict[str, str|bool|int])->CustomDataset:
    if 'name' not in dataset_description:
        raise ValueError(f"dataset description does not contain a '_name' field: {dataset_description}")

    dataset_name:str = dataset_description['name']

    cls: Optional[Type] = None
    for dataset_class in ALL_DATASET_CLASSES:
        assert issubclass(dataset_class, CustomDataset)

        if dataset_name == dataset_class.dataset_name():
            cls = dataset_class
            break

    if cls is None:
        raise ValueError(f"unknown or unsupported dataset \"{dataset_name}\"")

    assert issubclass(cls, CustomDataset)

    print(f'Passing the following keyword arguments to constructor of dataset "{dataset_name}":', flush = True)
    for k, v in dataset_description.items():
        print(f'\t"{k}": {v}', flush = True)

    return cls(**dataset_description)