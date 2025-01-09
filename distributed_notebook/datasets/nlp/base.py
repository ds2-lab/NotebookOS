from abc import ABC

from distributed_notebook.datasets.base import CustomDataset

class NLPDataset(CustomDataset, ABC):
    def __init__(self, name:str = "", root_dir: str = "", shuffle: bool = True, num_workers: int = 2, **kwargs):
        assert name is not None and name != ""
        assert root_dir is not None and root_dir != ""

        super().__init__(
            name = name,
            root_dir = root_dir,
            shuffle = shuffle,
            num_workers = num_workers,
        )

    @property
    def requires_tokenization(self)->bool:
        return True