import boto3
import tarfile
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
            aws_region:str = "us-east-1",
            s3_bucket_name:str = "distributed-notebook-public",
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
        self.__download_dataset(
            hugging_face_dataset_config_name = hugging_face_dataset_config_name,
            aws_region = aws_region,
            s3_bucket_name = s3_bucket_name,
            root_dir = root_dir,
        )

    def __download_dataset(
            self,
            hugging_face_dataset_config_name: str,
            root_dir:str = "",
            aws_region:str = "us-east-1",
            s3_bucket_name:str = "distributed-notebook-public",
    ):
        self._dataset_already_downloaded: bool = os.path.exists(root_dir)

        # Download the dataset, or load it from the cache.
        self._download_start: float = time.time()

        if self.supports_aws_s3_download and not self._dataset_already_downloaded:

            self.retrieve_dataset_from_s3(
                aws_region = aws_region,
                s3_bucket_name = s3_bucket_name,
                root_dir = root_dir
            )

            assert os.path.exists(root_dir)

        self._dataset = load_dataset(
            path=self._hugging_face_dataset_name,
            name=hugging_face_dataset_config_name,
            download_mode=DownloadMode.REUSE_DATASET_IF_EXISTS
        )

        self._download_end: float = time.time()

        if not self._dataset_already_downloaded:
            self._download_duration_sec = self._download_end - self._download_start
            self.log.debug(f"The {self.name} dataset was downloaded to root directory \"{self._root_dir}\" in "
                           f"{self._download_duration_sec} seconds.")
        else:
            self.log.debug(f"The {self.name} dataset was already downloaded. Root directory: \"{self._root_dir}\"")

    @staticmethod
    @abstractmethod
    def dataset_shortname()->str:
        pass

    def retrieve_dataset_from_s3(
            self,
            root_dir: str = "",
            s3_bucket_name:str = "distributed-notebook-public",
            aws_region:str = "us-east-1",
    ):
        # Check if it already exists.
        if os.path.exists(root_dir):
            self.log.debug(f'"{self.dataset_name()}" dataset is already downloaded. '
                           f'No need to retrieve it from AWS S3.')
            return

        datasets_directory:str = os.path.expanduser("~/.cache/huggingface/datasets/")

        filename:str = f"{self.dataset_shortname()}.tar.gz"
        download_path:str = os.path.join(datasets_directory, filename)

        s3_key:str = f"datasets/{filename}"

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

        # Extract the tar.gz file
        self.log.debug(f'Extracting downloaded file "{download_path}" to path "{datasets_directory}"...')
        extract_start_time: float = time.time()
        with tarfile.open(download_path, 'r:gz') as tar:
            tar.extractall(path=datasets_directory)

        extract_duration: float = time.time() - extract_start_time
        self.log.debug(f'Extracted downloaded file "{download_path}" to path '
                       f'"{datasets_directory}" in {extract_duration:,} seconds.')

        # Optionally, remove the downloaded tar.gz file after extraction
        os.remove(download_path)
        self.log.debug(f'Removed downloaded file "{download_path}".')

    @property
    @abstractmethod
    def supports_aws_s3_download(self)->bool:
        pass

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
