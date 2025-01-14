from abc import ABC, abstractmethod

import os
import logging

from distributed_notebook.logs import ColoredLogFormatter

from torchvision.datasets.utils import check_integrity

class CustomDataset(ABC):
    def __init__(self, root_dir: str = "", shuffle: bool = True, num_workers: int = 2):
        self._root_dir = root_dir
        self._shuffle = shuffle
        self._num_workers = num_workers

        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

    @staticmethod
    @abstractmethod
    def category() -> str:
        pass

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
            "name": self.name,
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
    @abstractmethod
    def name(self)->str:
        pass

    @property
    def root_directory(self)->str:
        return self._root_dir

    @property
    @abstractmethod
    def train_loader(self):
        pass

    @property
    @abstractmethod
    def test_loader(self):
        pass