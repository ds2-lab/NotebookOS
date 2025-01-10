import os
import time
from abc import ABC
from typing import Callable, Dict, Union, Optional

import torch.utils.data
from datasets import load_from_disk
from sklearn.model_selection import train_test_split

from torch.utils.data import DataLoader, Dataset, TensorDataset, SequentialSampler, RandomSampler

from distributed_notebook.datasets.hugging_face import HuggingFaceDataset
from distributed_notebook.datasets.nlp.util import get_tokenizer


class TextDataset(Dataset):
    def __init__(self, encodings):
        self.encodings = encodings

    def __getitem__(self, idx):
        return {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}

    def __len__(self):
        return len(self.encodings)

class NLPDataset(HuggingFaceDataset, ABC):
    """
    NLPDataset is a particular type of HuggingFaceDataset in which the data is tokenized after being downloaded.

    The tokenized data is cached locally (on disk).
    """

    def __init__(
            self,
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
            batch_size = 16,
            **kwargs
    ):
        assert model_name is not None and model_name != ""
        assert root_dir is not None and root_dir != ""
        assert hugging_face_dataset_name is not None and hugging_face_dataset_name != ""
        assert text_feature_column_name is not None and text_feature_column_name != ""
        assert tokenized_dataset_directory is not None and tokenized_dataset_directory != ""
        assert postprocess_tokenized_dataset is not None

        model_name = model_name.lower()
        assert model_name == "bert" or model_name == "gpt-2" or model_name == "gpt2"

        super().__init__(
            root_dir=root_dir,
            shuffle=shuffle,
            num_workers=num_workers,
            batch_size = batch_size,
            hugging_face_dataset_name=hugging_face_dataset_name,
            hugging_face_dataset_config_name=hugging_face_dataset_config_name,
            model_name=model_name,
            **kwargs,
        )

        self._max_token_length: int = max_token_length
        self._dataset_dict_path: str = tokenized_dataset_directory
        self._dataset_already_tokenized: bool = os.path.exists(self._dataset_dict_path)

        if not self._dataset_already_tokenized:
            print(f'Tokenizing the {self.name} dataset now. Will cache tokenized data in directory "{self._root_dir}"')
            self._tokenize_start: float = time.time()

            self.tokenizer = get_tokenizer(model_name)

            # Tokenization
            def tokenize_function(example):
                return self.tokenizer(
                    example[text_feature_column_name],
                    add_special_tokens=True,
                    truncation=token_truncation,
                    padding=token_padding,
                    max_length=max_token_length
                )

            self._tokenized_datasets = self._dataset.map(tokenize_function, batched=True)
            self._tokenized_datasets = postprocess_tokenized_dataset(self._tokenized_datasets)
            self._tokenized_datasets.set_format("torch")

            os.makedirs(self._dataset_dict_path, 0o750, exist_ok=True)

            print(f'Finished tokenizing the {self.name} dataset in {time.time() - self._tokenize_start} seconds. '
                  f'Writing tokenized dataset to directory "{self._dataset_dict_path}".')

            write_start: float = time.time()

            self._tokenized_datasets.save_to_disk(dataset_dict_path=self._dataset_dict_path)

            self._tokenize_end: float = time.time()
            self._tokenize_duration: float = self._tokenize_end - self._tokenize_start

            print(f'Wrote the tokenized {self.name} dataset to directory "{self._dataset_dict_path}" in '
                  f'{time.time() - write_start} seconds. '
                  f'Total time elapsed: {self._tokenize_duration} seconds.')
        else:
            # TODO: Read the cached tokenized dataset.
            print(f'The {self.name} dataset was already tokenized. '
                  f'Loading cached, tokenized {self.name} dataset from directory "{self._dataset_dict_path}" now...')

            _read_start: float = time.time()
            self._tokenized_datasets = load_from_disk(self._dataset_dict_path)

            print(f'Read cached, tokenized {self.name} dataset from directory "{self._dataset_dict_path}" '
                  f'in {time.time() - _read_start} seconds.')

        # Create the DataLoader for our training set
        train_data = TensorDataset(
            self._tokenized_datasets['train']['input_ids'],
            self._tokenized_datasets['train']['attention_mask'],
            self._tokenized_datasets['train']['labels']
        )
        train_sampler = RandomSampler(train_data)
        self._train_loader = DataLoader(train_data, sampler=train_sampler, batch_size=32)

        # Create the DataLoader for our validation set
        validation_data = TensorDataset(
            self._tokenized_datasets['validation']['input_ids'],
            self._tokenized_datasets['validation']['attention_mask'],
            self._tokenized_datasets['validation']['labels']
        )
        validation_sampler = SequentialSampler(validation_data)
        self._test_loader = DataLoader(validation_data, sampler=validation_sampler, batch_size=32)

        # Prepare the data loaders
        # self._train_dataset = TextDataset(self._tokenized_datasets["train"])
        # self._test_dataset = TextDataset(self._tokenized_datasets["validation"])
        #
        # self._train_loader = DataLoader(self._train_dataset, batch_size = batch_size)
        # self._test_loader = DataLoader(self._test_dataset, batch_size = batch_size)

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

    @property
    def dataset_already_tokenized(self) -> bool:
        return self._dataset_already_tokenized

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
    def train_loader(self):
        return self._train_loader

    @property
    def test_loader(self):
        return self._test_loader

    @property
    def description(self) -> Dict[str, Union[str, int, bool]]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["model_name"] = self._model_name

        return desc
