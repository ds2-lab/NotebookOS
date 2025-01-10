from abc import ABC
import os
from typing import Callable, Dict, Union, Optional

from datasets import load_dataset, DownloadMode, load_from_disk

import time

from distributed_notebook.datasets.hugging_face import HuggingFaceDataset
from distributed_notebook.datasets.nlp.util import get_tokenizer

class NLPDataset(HuggingFaceDataset, ABC):
    """
    NLPDataset is a particular type of HuggingFaceDataset in which the data is tokenized after being downloaded.

    The tokenized data is cached locally (on disk).
    """
    def __init__(
            self,
            name:str = "",
            root_dir: str = "",
            model_name: str = "",
            shuffle: bool = True,
            num_workers: int = 2,
            hugging_face_dataset_name: str = "",
            hugging_face_dataset_config_name: Optional[str] = None,
            text_feature_column_name: str = "text",
            postprocess_tokenized_dataset: Callable = None,
            max_token_length: int = 128,
            token_truncation: bool = True,
            token_padding: str = "max_length",
            tokenized_dataset_directory: str = "",
            **kwargs
    ):
        assert name is not None and name != ""
        assert model_name is not None and model_name != ""
        assert root_dir is not None and root_dir != ""
        assert hugging_face_dataset_name is not None and hugging_face_dataset_name != ""
        assert text_feature_column_name is not None and text_feature_column_name != ""
        assert tokenized_dataset_directory is not None and tokenized_dataset_directory != ""
        assert postprocess_tokenized_dataset is not None

        model_name = model_name.lower()
        assert model_name == "bert" or model_name == "gpt-2" or model_name == "gpt2"

        super().__init__(
            name = name,
            root_dir = root_dir,
            shuffle = shuffle,
            num_workers = num_workers,
            hugging_face_dataset_name = hugging_face_dataset_name,
            hugging_face_dataset_config_name = hugging_face_dataset_config_name,
            text_feature_column_name = text_feature_column_name,
            postprocess_tokenized_dataset = postprocess_tokenized_dataset,
            max_token_length = max_token_length,
            tokenized_dataset_directory = tokenized_dataset_directory,
            model_name = model_name,
            **kwargs,
        )

        self._dataset_dict_path: str = tokenized_dataset_directory
        self._dataset_already_tokenized: bool = os.path.exists(self._dataset_dict_path)

        if not self._dataset_already_tokenized:
            print(f'Tokenizing the {name} dataset now. Will cache tokenized data in directory "{self._root_dir}"')
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