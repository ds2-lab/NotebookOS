import os
import tarfile

import boto3
from abc import ABC, abstractmethod
from typing import Callable, Dict, Union, Optional, Any

import datasets
import time
import torch.utils.data
from torch.utils.data import Dataset, TensorDataset, SequentialSampler, RandomSampler, DataLoader

from distributed_notebook.deep_learning.data.hugging_face import HuggingFaceDataset
from distributed_notebook.deep_learning.data.loader import WrappedLoader
from distributed_notebook.deep_learning.data.nlp.util import get_tokenizer

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
            postprocess_tokenized_dataset: Callable[[Any, str], Any] = None,
            max_token_length: int = 128,
            token_truncation: bool = True,
            token_padding: str = "max_length",
            tokenized_dataset_directory: str = "",
            batch_size=16,
            aws_region: str = "us-east-1",
            s3_bucket_name:str = "distributed-notebook-public",
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

        self._recorded_tokenization_overhead: bool = False

        super().__init__(
            root_dir=root_dir,
            shuffle=shuffle,
            num_workers=num_workers,
            batch_size=batch_size,
            hugging_face_dataset_name=hugging_face_dataset_name,
            hugging_face_dataset_config_name=hugging_face_dataset_config_name,
            model_name=model_name,
            **kwargs,
        )

        self._max_token_length: int = max_token_length
        self._dataset_dict_path: str = tokenized_dataset_directory
        self._dataset_already_tokenized: bool = os.path.exists(self._dataset_dict_path)

        self.__load_tokenized_datasets(
            model_name=model_name,
            text_feature_column_name=text_feature_column_name,
            postprocess_tokenized_dataset=postprocess_tokenized_dataset,
            max_token_length=max_token_length,
            token_truncation=token_truncation,
            token_padding=token_padding,
            aws_region=aws_region,
            s3_bucket_name=s3_bucket_name,
        )

        # Create the DataLoader for our training set
        train_data = TensorDataset(
            self._tokenized_datasets['train']['input_ids'],
            self._tokenized_datasets['train']['attention_mask'],
            self._tokenized_datasets['train']['labels']
        )
        train_sampler = RandomSampler(train_data)
        self._train_loader = WrappedLoader(train_data, sampler=train_sampler,
                                           batch_size=batch_size, dataset_name=self.dataset_name())

        def load_validation_data(key: str = 'validation'):
            return TensorDataset(
                self._tokenized_datasets[key]['input_ids'],
                self._tokenized_datasets[key]['attention_mask'],
                self._tokenized_datasets[key]['labels']
            )

        # Create the DataLoader for our validation set
        try:
            validation_data = load_validation_data('validation')
        except KeyError:
            self.log.warning(f'Cannot find tokenized datasets with key "validation".')
            validation_data = None

        if validation_data is None:
            try:
                validation_data = load_validation_data('test')
            except KeyError:
                self.log.warning(f'Cannot find tokenized datasets with key "test".')

        if validation_data is not None:
            validation_sampler = SequentialSampler(validation_data)
            self._test_loader = WrappedLoader(validation_data, sampler=validation_sampler,
                                              batch_size=batch_size, dataset_name=self.dataset_name())
        else:
            self.log.warning(f"Failed to create test loader for dataset '{self.name}'.")
            self._test_loader = None

    @staticmethod
    @abstractmethod
    def dataset_shortname()->str:
        pass

    @staticmethod
    @abstractmethod
    def tokenized_dataset_root_directory()->str:
        pass

    def __load_tokenized_datasets(
            self,
            model_name: str = "",
            text_feature_column_name: str = "text",
            postprocess_tokenized_dataset: Callable[[Any, str], Any] = None,
            max_token_length: int = 128,
            token_truncation: bool = True,
            token_padding: str = "max_length",
            aws_region:str = "us-east-1",
            s3_bucket_name:str = "distributed-notebook-public",
    ):
        if not self._dataset_already_tokenized:
            self.__tokenize_dataset(
                model_name=model_name,
                text_feature_column_name=text_feature_column_name,
                postprocess_tokenized_dataset=postprocess_tokenized_dataset,
                max_token_length=max_token_length,
                token_truncation=token_truncation,
                token_padding=token_padding,
                aws_region=aws_region,
                s3_bucket_name=s3_bucket_name,
            )
        else:
            self.__load_tokenized_dataset_from_disk()

    def __load_tokenized_dataset_from_disk(self):
        self._recorded_tokenization_overhead = True
        self.log.debug(f'The {self.name} dataset was already tokenized. Loading cached, tokenized {self.name} '
                       f'dataset from directory "{self._dataset_dict_path}" now...')

        _read_start: float = time.time()
        self._tokenized_datasets = datasets.load_from_disk(self._dataset_dict_path)

        self.log.debug(f'Read cached, tokenized {self.name} dataset from directory "{self._dataset_dict_path}" '
                       f'in {time.time() - _read_start} seconds.')

    def __tokenize_dataset(
            self,
            model_name: str = "",
            text_feature_column_name: str = "text",
            postprocess_tokenized_dataset: Callable[[Any, str], Any] = None,
            max_token_length: int = 128,
            token_truncation: bool = True,
            token_padding: str = "max_length",
            aws_region:str = "us-east-1",
            s3_bucket_name:str = "distributed-notebook-public",
    ):
        if not torch.cuda.is_available():
            self.__retrieve_tokenized_dataset_from_s3(
                aws_region=aws_region,
                s3_bucket_name=s3_bucket_name,
            )
            return

        self.log.debug(f'Tokenizing the {self.name} dataset now. '
                       f'Will cache tokenized data in directory "{self._dataset_dict_path}"')

        # When the kernel retrieves the tokenization time to record the overhead, we flip this flag to true,
        # That way, we don't double-count it in the future.
        self._recorded_tokenization_overhead = False

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
        self._tokenized_datasets = postprocess_tokenized_dataset(self._tokenized_datasets, text_feature_column_name)
        self._tokenized_datasets.set_format("torch")

        os.makedirs(self._dataset_dict_path, 0o750, exist_ok=True)

        self.log.debug(f'Finished tokenizing the {self.name} dataset in {time.time() - self._tokenize_start} '
                       f'seconds. Writing tokenized dataset to directory "{self._dataset_dict_path}".')

        write_start: float = time.time()

        self._tokenized_datasets.save_to_disk(dataset_dict_path=self._dataset_dict_path)

        self._tokenize_end: float = time.time()
        self._tokenize_duration = self._tokenize_end - self._tokenize_start

        self.log.debug(f'Wrote the tokenized {self.name} dataset to directory "{self._dataset_dict_path}" in '
                       f'{time.time() - write_start} seconds. Total time elapsed: {self._tokenize_duration} seconds.')

    def __retrieve_tokenized_dataset_from_s3(
            self,
            s3_bucket_name:str = "distributed-notebook-public",
            aws_region:str = "us-east-1",
    ):
        tokenized_datasets_directory:str = os.path.expanduser("~/.cache/distributed_notebook/tokenized_datasets/")

        filename:str = f"{self.dataset_shortname()}-{self._model_name}.tar.gz"
        download_path:str = os.path.join(tokenized_datasets_directory, filename)

        s3_key:str = f"tokenized_datasets/{filename}"

        # Ensure the download directory exists
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        # Initialize the S3 client
        s3_client = boto3.client('s3', region_name=aws_region)

        # Download the file from S3
        self.log.debug(f'Downloading object with key "{s3_key}" from S3 bucket "{s3_bucket_name}"...')
        download_start: float = time.time()

        s3_client.download_file(s3_bucket_name, s3_key, download_path)

        self.log.debug(f'Downloaded object with key "{s3_key}" from S3 bucket "{s3_bucket_name}" '
                       f'in {time.time() - download_start:,} seconds.')

        extract_path:str = os.path.join(tokenized_datasets_directory, self.tokenized_dataset_root_directory())

        # Extract the tar.gz file
        self.log.debug(f'Extracting downloaded file "{download_path}" to path "{extract_path}"...')
        extract_start_time: float = time.time()
        with tarfile.open(download_path, 'r:gz') as tar:
            tar.extractall(path=extract_path)

        extract_duration: float = time.time() - extract_start_time
        self.log.debug(f'Extracted downloaded file "{download_path}" to path '
                       f'"{extract_path}" in {extract_duration:,} seconds.')

        # Optionally, remove the downloaded tar.gz file after extraction
        os.remove(download_path)
        self.log.debug(f'Removed downloaded file "{download_path}".')

    @property
    def recorded_tokenization_overhead(self) -> bool:
        return self._recorded_tokenization_overhead

    @recorded_tokenization_overhead.setter
    def recorded_tokenization_overhead(self, val: bool = True):
        self._recorded_tokenization_overhead = val

    def set_recorded_tokenization_overhead(self, val: bool = True):
        """
        This should be called by the kernel when it retrieves the tokenization overhead, as we only
        tokenize the dataset once. This flag lets us know that we've already recorded the tokenization
        overhead and should not re-record it again in the future.
        """
        self._recorded_tokenization_overhead = val

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
    def train_loader(self) -> Optional[DataLoader]:
        return self._train_loader

    @property
    def test_loader(self) -> Optional[DataLoader]:
        return self._test_loader

    @property
    def description(self) -> Dict[str, Union[str, int, bool]]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["model_name"] = self._model_name

        return desc
