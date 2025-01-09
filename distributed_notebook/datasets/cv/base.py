from abc import ABC

from distributed_notebook.datasets.custom_dataset import CustomDataset

class CVDataset(CustomDataset, ABC):
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
        return False

    @property
    def tokenization_start(self)->float:
        return -1.0

    @property
    def tokenization_end(self)->float:
        return -1.0

    @property
    def tokenization_duration_sec(self)->float:
        return -1.0