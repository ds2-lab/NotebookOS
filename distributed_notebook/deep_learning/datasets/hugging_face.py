from abc import ABC
import os
from typing import Callable, Dict, Union, Optional

from datasets import load_dataset, DownloadMode

import time

from distributed_notebook.deep_learning.datasets.custom_dataset import CustomDataset

class HuggingFaceDataset(CustomDataset, ABC):
    """
    HuggingFaceDatasets are a particular type of custom dataset that uses the HuggingFace 'datasets' module
    to download/manage datasets, rather than the datasets built into PyTorch.
    """
    def __init__(
            self,
            root_dir: str = "",
            model_name: Optional[str] = "",
            shuffle: bool = True,
            num_workers: int = 2,
            hugging_face_dataset_name: str = "",
            hugging_face_dataset_config_name: Optional[str] = None,
            **kwargs
    ):
        assert root_dir is not None and root_dir != ""
        assert hugging_face_dataset_name is not None and hugging_face_dataset_name != ""

        if model_name is not None:
            model_name = model_name.lower()

        self._model_name: Optional[str] = model_name

        super().__init__(
            root_dir = root_dir,
            shuffle = shuffle,
            num_workers = num_workers,
        )

        self._hugging_face_dataset_name: str = hugging_face_dataset_name
        self._dataset_already_downloaded: bool = os.path.exists(root_dir)

        # Download the dataset, or load it from the cache.
        self._download_start: float = time.time()
        self._dataset = load_dataset(
            path = self._hugging_face_dataset_name,
            name = hugging_face_dataset_config_name,
            download_mode = DownloadMode.REUSE_DATASET_IF_EXISTS
        )
        self._download_end: float = time.time()

        if not self._dataset_already_downloaded:
            self._download_duration_sec: float = self._download_end - self._download_start
            print(f"The {self.name} dataset was downloaded to root directory \"{self._root_dir}\" in "
                  f"{self._download_duration_sec} seconds.")
        else:
            print(f"The {self.name} dataset was already downloaded. Root directory: \"{self._root_dir}\"")

    @property
    def tokenization_start(self)->float:
        if hasattr(self, "_tokenize_start"):
            return self._tokenize_start

        return -1

    @property
    def tokenization_end(self)->float:
        if hasattr(self, "_tokenize_end"):
            return self._tokenize_end

        return -1

    @property
    def tokenization_duration_sec(self)->float:
        if hasattr(self, "_tokenize_duration"):
            return self._tokenize_duration

        return -1

    @property
    def requires_tokenization(self)->bool:
        return True

    @property
    def download_duration_sec(self)->float:
        return self._download_duration_sec

    @property
    def dataset_already_downloaded(self)->bool:
        return self._dataset_already_downloaded

    @property
    def download_start_time(self)->float:
        return self._download_start

    @property
    def download_end_time(self)->float:
        return self._download_end

    @property
    def download_duration(self)->float:
        return self._download_duration_sec

    @property
    def download_start(self)->float:
        return self._download_start

    @property
    def download_end(self)->float:
        return self._download_end

    @property
    def description(self)->Dict[str, Union[str, int, bool]]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["model_name"] = self._model_name

        return desc