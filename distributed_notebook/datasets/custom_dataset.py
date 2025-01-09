from abc import ABC, abstractmethod

import os

from torchvision.datasets.utils import check_integrity

class CustomDataset(ABC):
    def __init__(self, name:str = "", root_dir: str = "", shuffle: bool = True, num_workers: int = 2):
        self._name = name
        self._root_dir = root_dir
        self._shuffle = shuffle
        self._num_workers = num_workers

    def _check_if_downloaded(self, filenames: list, base_folder: str) -> bool:
        """
        Check if the dataset is already downloaded.

        :return: True if the dataset is already downloaded, otherwise False
        """
        for filename, md5 in filenames:
            fpath = os.path.join(self._root_dir, base_folder, filename)
            if not check_integrity(fpath, md5):
                return False
        return True

    @property
    @abstractmethod
    def dataset_already_downloaded(self)->bool:
        return False

    @property
    @abstractmethod
    def download_start(self)->float:
        return -1.0

    @property
    @abstractmethod
    def download_end(self)->float:
        return -1.0

    @property
    @abstractmethod
    def download_duration_sec(self)->float:
        return -1.0

    @property
    @abstractmethod
    def tokenization_start(self)->float:
        return -1.0

    @property
    @abstractmethod
    def tokenization_end(self)->float:
        return -1.0

    @property
    @abstractmethod
    def tokenization_duration_sec(self)->float:
        return -1.0

    @property
    @abstractmethod
    def requires_tokenization(self)->bool:
        return False

    @property
    @abstractmethod
    def description(self)->dict[str, str|int|bool]:
        return {
            "name": self._name,
            "root_dir": self._root_dir,
            "shuffle": self._shuffle,
            "num_workers": self._num_workers
        }

    @property
    def num_workers(self)->int:
        return self._num_workers

    @property
    def shuffle(self)->bool:
        return self._shuffle

    @property
    def name(self)->str:
        return self._name

    @property
    def root_directory(self)->str:
        return self._root_dir

    @property
    @abstractmethod
    def train_dataset(self):
        pass

    @property
    @abstractmethod
    def train_loader(self):
        pass

    @property
    @abstractmethod
    def test_dataset(self):
        pass

    @property
    @abstractmethod
    def test_loader(self):
        pass