from abc import ABC
import os
from typing import Callable

from datasets import load_dataset, DownloadMode, load_from_disk

import time

from distributed_notebook.datasets.nlp.util import get_tokenizer
from distributed_notebook.datasets.custom_dataset import CustomDataset

class NLPDataset(CustomDataset, ABC):
    def __init__(
            self,
            name:str = "",
            root_dir: str = "",
            model_name: str = "",
            shuffle: bool = True,
            num_workers: int = 2,
            hugging_face_dataset_name: str = "",
            text_feature_column_name: str = "text",
            postprocess_tokenized_dataset: Callable = None,
            max_token_length: int = 128,
            token_truncation: bool = True,
            token_padding: str = "max_length",
            **kwargs
    ):
        assert name is not None and name != ""
        assert model_name is not None and model_name != ""
        assert root_dir is not None and root_dir != ""
        assert hugging_face_dataset_name is not None and hugging_face_dataset_name != ""
        assert text_feature_column_name is not None and text_feature_column_name != ""
        assert postprocess_tokenized_dataset is not None

        super().__init__(
            name = name,
            root_dir = root_dir,
            shuffle = shuffle,
            num_workers = num_workers,
        )

        self._model_name: str = model_name
        self._dataset_dict_path: str = f"~/tokenized_datasets/cola/{self._model_name}"

        self._dataset_already_downloaded: bool = os.path.exists(root_dir)
        self._dataset_already_tokenized: bool = os.path.exists(self._dataset_dict_path)

        # Download the dataset, or load it from the cache.
        self._download_start: float = time.time()
        self._dataset = load_dataset(path = hugging_face_dataset_name, download_mode = DownloadMode.REUSE_DATASET_IF_EXISTS)
        self._download_end: float = time.time()

        if not self._dataset_already_downloaded:
            self._download_duration_sec: float = self._download_end - self._download_start
            print(f"The {name} dataset was downloaded to root directory \"{self._root_dir}\" in "
                  f"{self._download_duration_sec} seconds.")
        else:
            print(f"The {name} dataset was already downloaded. Root directory: \"{self._root_dir}\"")

        if not self._dataset_already_tokenized:
            self._tokenize_start: float = time.time()

            self.tokenizer = get_tokenizer(model_name)

            # Tokenization
            def tokenize_function(example):
                return self.tokenizer(
                    example[text_feature_column_name],
                    truncation=token_truncation,
                    padding=token_padding,
                    max_length=max_token_length
                )

            self._tokenized_datasets = self._dataset.map(tokenize_function, batched=True)
            self._tokenized_datasets = postprocess_tokenized_dataset(self._tokenized_datasets)
            self._tokenized_datasets.set_format("torch")

            os.makedirs(self._dataset_dict_path, 0o750, exist_ok=True)

            print(f'Finished tokenizing the {name} dataset in {time.time() - self._tokenize_start} seconds. '
                  f'Writing tokenized dataset to directory "{self._dataset_dict_path}".')

            write_start: float = time.time()

            self._tokenized_datasets.save_to_disk(dataset_dict_path = self._dataset_dict_path)

            self._tokenize_end: float = time.time()
            self._tokenize_duration: float = self._tokenize_end - self._tokenize_start

            print(f'Wrote the tokenized {name} dataset to directory "{self._dataset_dict_path}" in '
                  f'{time.time() - write_start} seconds. '
                  f'Total time elapsed: {self._tokenize_duration} seconds.')
        else:
            # TODO: Read the cached tokenized dataset.
            print(f'The {name} dataset was already tokenized. '
                  f'Loading cached, tokenized {name} dataset from directory "{self._dataset_dict_path}" now...')

            _read_start: float = time.time()
            self._tokenized_datasets = load_from_disk(self._dataset_dict_path)

            print(f'Read cached, tokenized {name} dataset from directory "{self._dataset_dict_path}" '
                  f'in {time.time() - _read_start} seconds.')

        # Prepare the data loaders
        self._train_loader = self._tokenized_datasets["train"]
        self._test_loader = self._tokenized_datasets["validation"]

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
    def train_loader(self):
        return self._train_loader

    @property
    def test_loader(self):
        return self._test_loader