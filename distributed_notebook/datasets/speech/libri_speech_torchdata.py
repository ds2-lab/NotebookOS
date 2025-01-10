import time

import os

from typing import List, Optional, Dict, Union

from torch.utils.data import DataLoader
from torchaudio import datasets

from distributed_notebook.datasets.custom_dataset import CustomDataset

class LibriSpeech(CustomDataset):
    train_clean_100:str = "train-clean-100"
    train_clean_360:str = "train-clean-360"
    train_clean_500:str = "train-other-500"

    test_clean:str = "test-clean"
    test_other:str = "test-other"

    _train_splits: List[str] = [
        "train-clean-100",
        "train-clean-360",
        "train-other-500"
    ]

    _test_splits: List[str] = ["test-clean", "test-other"]

    def __init__(
            self,
            root_dir: str = os.path.expanduser("~/.cache"),
            folder_in_archive: str = datasets.librispeech.FOLDER_IN_ARCHIVE,
            batch_size: int = 256,
            shuffle: bool = True,
            num_workers: int = 2,
            train_split: Optional[str] = None,
            test_split: Optional[str] = None
    ):
        assert folder_in_archive is not None

        if train_split is not None:
            assert train_split in LibriSpeech._train_splits[0]

        if test_split is not None:
            assert test_split in LibriSpeech._test_splits[0]

        if train_split is None and test_split is None:
            raise ValueError("At least one of the training split and test split should be non-null.")

        super().__init__(root_dir=root_dir, shuffle=shuffle, num_workers=num_workers)

        self._train_split: Optional[str] = train_split
        self._test_split: Optional[str] = test_split
        self._folder_in_archive: str = folder_in_archive

        try:
            # Create the dataset without downloading it.
            # If there's a RuntimeError, then it hasn't been downloaded yet.
            if train_split is not None:
                self._train_dataset: Optional[datasets.LIBRISPEECH] = datasets.LIBRISPEECH(
                    root=root_dir, download=False, url=train_split, folder_in_archive = folder_in_archive)
            else:
                self._train_dataset: Optional[datasets.LIBRISPEECH] = None

            if test_split is not None:
                self._test_dataset: Optional[datasets.LIBRISPEECH] = datasets.LIBRISPEECH(
                    root=root_dir, download=False, url=test_split, folder_in_archive = folder_in_archive)
            else:
                self._test_dataset: Optional[datasets.LIBRISPEECH] = None

            print(f"The {self.name} dataset was already downloaded. Root directory: \"{root_dir}\"")

            # No RuntimeError. The dataset must have already been downloaded.
            self._dataset_already_downloaded: bool = True
        except RuntimeError:
            self._dataset_already_downloaded: bool = False

            print(f'Downloading {self.name} dataset to root directory "{root_dir}" now...')

            self._download_start = time.time()

            if train_split is not None:
                self._train_dataset: Optional[datasets.LIBRISPEECH] = datasets.LIBRISPEECH(
                    root=root_dir, download=True, url=train_split, folder_in_archive = folder_in_archive)
            else:
                self._train_dataset: Optional[datasets.LIBRISPEECH] = None

            if test_split is not None:
                self._test_dataset: Optional[datasets.LIBRISPEECH] = datasets.LIBRISPEECH(
                    root=root_dir, download=True, url=test_split, folder_in_archive = folder_in_archive)
            else:
                self._test_dataset: Optional[datasets.LIBRISPEECH] = None

            self._download_end = time.time()
            self._download_duration_sec = self._download_end - self._download_start

            print(f"The {self.name} dataset was downloaded to root directory \"{root_dir}\" in {self._download_duration_sec} seconds.")

        if self._train_dataset is not None:
            self._train_loader: Optional[DataLoader] = DataLoader(self._train_dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers)
        else:
            self._train_loader: Optional[DataLoader] = None

        if self._test_dataset is not None:
            self._test_loader: Optional[DataLoader] = DataLoader(self._test_dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers)
        else:
            self._test_loader: Optional[DataLoader] = None

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
    def folder_in_archive(self) -> str:
        return self._folder_in_archive

    @property
    def description(self) -> Dict[str, Union[str, int, bool]]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["train_split"] = self._train_split
        desc["test_split"] = self._test_split
        desc["folder_in_archive"] = self._folder_in_archive

        return desc

    @property
    def train_split(self) -> Optional[str]:
        return self._train_split

    @property
    def test_split(self) -> Optional[str]:
        return self._test_split

    @property
    def train_dataset(self)->Optional[datasets.LIBRISPEECH]:
        return self._train_dataset

    @property
    def train_loader(self)->Optional[DataLoader]:
        return self._train_loader

    @property
    def test_dataset(self)->Optional[datasets.LIBRISPEECH]:
        return self._test_dataset

    @property
    def test_loader(self)->Optional[DataLoader]:
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

    @property
    def name(self)->str:
        return "LibriSpeech"