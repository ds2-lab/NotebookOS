import time

import os

from typing import List

from torch.utils.data import DataLoader
from torchaudio import datasets

from distributed_notebook.datasets.custom_dataset import CustomDataset

class LibriSpeech(CustomDataset):
    _train_splits: List[str] = ["train-clean-100", "train-clean-360", "train-other-500"]
    _test_splits: List[str] = ["test-clean", "test-other"]

    def __init__(
            self,
            root_dir: str = os.path.expanduser("~/.cache"),
            batch_size: int = 256,
            shuffle: bool = True,
            num_workers: int = 2,
            train_split: str = "",
            test_split: str = "",
            **kwargs
    ):
        if train_split is None or train_split == "":
            train_split = LibriSpeech._train_splits[0]

        if test_split is None or test_split == "":
            test_split = LibriSpeech._test_splits[0]

        assert train_split in LibriSpeech._train_splits[0]
        assert test_split in LibriSpeech._test_splits[0]

        super().__init__(name="LibriSpeech", root_dir=root_dir, shuffle=shuffle, num_workers=num_workers)

        self._train_split: str = train_split
        self._test_split: str  = test_split

        try:
            # Create the dataset without downloading it.
            # If there's a RuntimeError, then it hasn't been downloaded yet.
            self._train_dataset: datasets.LIBRISPEECH = datasets.LIBRISPEECH(root=root_dir, download=False, url=train_split)
            self._test_dataset: datasets.LIBRISPEECH = datasets.LIBRISPEECH(root=root_dir, download=False, url=test_split)

            print(f"The {self._name} dataset was already downloaded. Root directory: \"{root_dir}\"")

            # No RuntimeError. The dataset must have already been downloaded.
            self._dataset_already_downloaded: bool = True
        except RuntimeError:
            self._dataset_already_downloaded: bool = False

            print(f'Downloading LibriSpeech dataset to root directory "{root_dir}" now...')

            self._download_start = time.time()
            self._train_dataset: datasets.LIBRISPEECH = datasets.LIBRISPEECH(root=root_dir, download=True, url=train_split)
            self._test_dataset: datasets.LIBRISPEECH = datasets.LIBRISPEECH(root=root_dir, download=True, url=test_split)
            self._download_end = time.time()
            self._download_duration_sec = self._download_end - self._download_start

            print(f"The {self._name} dataset was downloaded to root directory \"{root_dir}\" in {self._download_duration_sec} seconds.")

        self._train_loader: DataLoader = DataLoader(self._train_dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers)
        self._test_loader: DataLoader = DataLoader(self._test_dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers)

    @property
    def download_duration_sec(self) -> float:
        return self._download_duration_sec

    @property
    def dataset_already_downloaded(self) -> bool:
        return self._dataset_already_downloaded

    @property
    def download_start_time(self) -> float:
        return self._download_start

    @property
    def download_end_time(self) -> float:
        return self._download_end

    @property
    def download_duration(self) -> float:
        return self._download_duration_sec

    @property
    def download_start(self) -> float:
        return self._download_start

    @property
    def download_end(self) -> float:
        return self._download_end

    @property
    def description(self) -> dict[str, str | int | bool]:
        return {
            "name": self._name,
            "root_dir": self._root_dir,
            "shuffle": self._shuffle,
            "num_workers": self._num_workers,
            "split": self._train_split,
        }

    @property
    def train_split(self) -> str:
        return self._train_split

    @property
    def test_split(self) -> str:
        return self._test_split

    @property
    def train_dataset(self):
        return self._train_dataset

    @property
    def train_loader(self):
        return self._train_loader

    @property
    def test_dataset(self):
        return self._test_dataset

    @property
    def test_loader(self):
        return self._test_loader

    @property
    def tokenization_start(self) -> float:
        return -1

    @property
    def tokenization_end(self) -> float:
        return -1

    @property
    def tokenization_duration_sec(self) -> float:
        return -1

    @property
    def requires_tokenization(self) -> bool:
        return False