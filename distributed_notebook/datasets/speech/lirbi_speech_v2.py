from abc import ABC
import os
from typing import Callable, Dict, Union, Optional

from datasets import load_dataset, DownloadMode, load_from_disk

import time

from distributed_notebook.datasets.hugging_face import HuggingFaceDataset
from distributed_notebook.datasets.nlp.util import get_tokenizer, get_username
from distributed_notebook.datasets.custom_dataset import CustomDataset

class LibriSpeech(HuggingFaceDataset):
    root_directory: str = f"/home/{get_username()}/.cache/huggingface/datasets/glue/cola"

    # https://huggingface.co/datasets/nyu-mll/glue
    hugging_face_dataset_name: str = "glue"

    text_feature_column_name: str = "sentence"

    hugging_face_dataset_config_name: Optional[str] = "cola"

    def __init__(
            self,
            name:str = "",
            root_dir: str = "",
            model_name: str = "",
            shuffle: bool = True,
            num_workers: int = 2,
            hugging_face_dataset_name: str = "",
            hugging_face_dataset_config_name: Optional[str] = None,
            **kwargs
    ):
        assert name is not None and name != ""
        assert model_name is not None and model_name != ""
        assert root_dir is not None and root_dir != ""
        assert hugging_face_dataset_name is not None and hugging_face_dataset_name != ""

        model_name = model_name.lower()
        assert model_name == "bert" or model_name == "gpt-2" or model_name == "gpt2"

        super().__init__(
            name = name,
            root_dir = root_dir,
            shuffle = shuffle,
            num_workers = num_workers,
            hugging_face_dataset_name = hugging_face_dataset_name,
            hugging_face_dataset_config_name = hugging_face_dataset_config_name,
            model_name = model_name,
            **kwargs,
        )

        # Prepare the data loaders
        self._train_loader = self._dataset["train"]
        self._test_loader = self._dataset["validation"]

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
    def dataset_already_tokenized(self)->bool:
        return self._dataset_already_tokenized

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
    def train_loader(self):
        return self._train_loader

    @property
    def test_loader(self):
        return self._test_loader

    @property
    def description(self)->Dict[str, Union[str, int, bool]]:
        return {
            "name": self._name,
            "root_dir": self._root_dir,
            "shuffle": self._shuffle,
            "num_workers": self._num_workers,
            "model_name": self._model_name,
        }