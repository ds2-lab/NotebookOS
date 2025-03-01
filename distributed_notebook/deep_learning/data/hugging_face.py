from abc import ABC, abstractmethod
import os
from typing import Dict, Union, Optional

from datasets import load_dataset, DownloadMode

import time

from distributed_notebook.deep_learning.data.custom_dataset import CustomDataset


class HuggingFaceDataset(CustomDataset, ABC):
    """
    HuggingFaceDatasets are a particular type of custom dataset that uses the HuggingFace 'data' module
    to download/manage data, rather than the data built into PyTorch.
    """

    def __init__(
            self,
            root_dir: str = "",
            model_name: Optional[str] = "",
            shuffle: bool = True,
            num_workers: int = 2,
            hugging_face_dataset_name: str = "",
            hugging_face_dataset_config_name: Optional[str] = None,
            batch_size: int = 1,
            **kwargs
    ):
        assert root_dir is not None and root_dir != ""
        assert hugging_face_dataset_name is not None and hugging_face_dataset_name != ""

        if model_name is not None:
            model_name = model_name.lower()

        self._model_name: Optional[str] = model_name

        super().__init__(
            root_dir=root_dir,
            shuffle=shuffle,
            num_workers=num_workers,
            batch_size=batch_size,
        )

        self._hugging_face_dataset_name: str = hugging_face_dataset_name
        self._dataset_already_downloaded: bool = os.path.exists(root_dir)

        # Download the dataset, or load it from the cache.
        self._download_start: float = time.time()
        self._dataset = load_dataset(
            path=self._hugging_face_dataset_name,
            name=hugging_face_dataset_config_name,
            download_mode=DownloadMode.REUSE_DATASET_IF_EXISTS
        )
        self._download_end: float = time.time()

        if not self._dataset_already_downloaded:
            self._download_duration_sec = self._download_end - self._download_start
            self.log.debug(f"The {self.name} dataset was downloaded to root directory \"{self._root_dir}\" from "
                  f"HuggingFace's servers in {self._download_duration_sec} seconds.")
        else:
            self.log.debug(f"The {self.name} dataset was already downloaded. Root directory: \"{self._root_dir}\"")

    @staticmethod
    @abstractmethod
    def huggingface_directory_name()->str:
        pass

    def remove_local_files(self):
        """
        Remove any local files on disk.
        """
        if self._dataset is not None:
            self.log.debug(f'Cleaning up cache files for "{self.name}" dataset.')

            st: float = time.time()
            self._dataset.cleanup_cache_files()
            self.log.debug(f'Successfully cleaned-up cache files for "{self.name}" '
                           f'dataset in {round(time.time() - st, 3):,} seconds.')

    @property
    def tokenization_start(self) -> float:
        if hasattr(self, "_tokenize_start"):
            return self._tokenize_start

        return -1

    @property
    def tokenization_end(self) -> float:
        if hasattr(self, "_tokenize_end"):
            return self._tokenize_end

        return -1

    @property
    def tokenization_duration_sec(self) -> float:
        if hasattr(self, "_tokenize_duration"):
            return self._tokenize_duration

        return -1

    @property
    def requires_tokenization(self) -> bool:
        return True

    @property
    def download_duration_sec(self) -> float:
        return self._download_duration_sec

    @property
    def dataset_already_downloaded(self) -> bool:
        return self._dataset_already_downloaded

    @dataset_already_downloaded.setter
    def dataset_already_downloaded(self, val: bool):
        self._dataset_already_downloaded = val

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
    def description(self) -> Dict[str, Union[str, int, bool]]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["model_name"] = self._model_name

        return desc
