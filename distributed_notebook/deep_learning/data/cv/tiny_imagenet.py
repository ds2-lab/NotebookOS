import boto3
import time
import os
import tarfile
from typing import Optional, Dict, Union, Any

import torch
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms

from distributed_notebook.deep_learning.data.cv.base import ComputerVisionDataset
from distributed_notebook.deep_learning.data.hugging_face import HuggingFaceDataset
from distributed_notebook.deep_learning.data.loader import WrappedLoader

from distributed_notebook.deep_learning.configuration import ComputerVision

class NoneTransform(object):
    """
    Does nothing to the image. To be used instead of None
    """

    def __call__(self, image):
        return image


class _TinyImageNetDataset(Dataset):
    def __init__(self, hf_dataset, transform=None):
        self.dataset = hf_dataset
        self.transform = transform

    def __len__(self):
        return len(self.dataset)

    def __getitem__(self, idx):
        # Load data
        item = self.dataset[idx]
        image = item['image']  # The key for images in the Hugging Face dataset
        label = item['label']  # The key for labels

        # Ensure image is RGB
        if image.mode != 'RGB':
            image = image.convert('RGB')

        # Apply transformations
        if self.transform:
            image = self.transform(image)

        return image, label


class TinyImageNet(HuggingFaceDataset, ComputerVisionDataset):
    default_root_directory: str = os.path.expanduser("~/.cache/huggingface/datasets/zh-plus___tiny-imagenet")

    # https://huggingface.co/datasets/zh-plus/tiny-imagenet
    hugging_face_dataset_name: str = "zh-plus/tiny-imagenet"

    text_feature_column_name: str = "text"

    hugging_face_dataset_config_name: Optional[str] = None

    def __init__(
            self,
            root_dir: str = default_root_directory,
            shuffle: bool = True,
            num_workers: int = 2,
            batch_size: int = 1,
            image_size: int = 224,  # 224 x 224 for ResNet-18, 299 x 299 for Inception v3.
            aws_region:str = "us-east-1",
            s3_bucket_name:str = "distributed-notebook-public",
            force_s3_download:bool = True,
            **kwargs
    ):
        assert image_size > 0
        assert batch_size > 0

        if force_s3_download:
            self.__retrieve_dataset_from_s3(
                s3_bucket_name = s3_bucket_name,
                aws_region = aws_region,
                root_dir = root_dir
            )

        super().__init__(
            root_dir=root_dir,
            shuffle=shuffle,
            num_workers=num_workers,
            hugging_face_dataset_name=TinyImageNet.hugging_face_dataset_name,
            hugging_face_dataset_config_name=TinyImageNet.hugging_face_dataset_config_name,
            batch_size=batch_size,
            **kwargs
        )

        self.log.debug(f'Creating Tiny ImageNet dataset with root directory "{root_dir}", batch size = {batch_size}, '
                       f'shuffle = {shuffle}, number of workers = {num_workers}, '
                       f'and image size = ({image_size}, {image_size}).')

        self.transform = transforms.Compose([
            transforms.Resize((image_size, image_size)),
            transforms.CenterCrop((image_size, image_size)),
            transforms.RandomHorizontalFlip(),
            transforms.PILToTensor(),
            transforms.ConvertImageDtype(torch.float),
            transforms.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225))
        ])

        self._image_size: int = image_size

        self._train_dataset: _TinyImageNetDataset = _TinyImageNetDataset(self._dataset["train"], transform=self.transform)
        self._test_dataset: _TinyImageNetDataset = _TinyImageNetDataset(self._dataset["valid"], transform=self.transform)

        # Prepare the data loaders
        self._train_loader = WrappedLoader(self._train_dataset, batch_size=batch_size,
                                           shuffle=shuffle, dataset_name=self.dataset_name())
        self._test_loader = WrappedLoader(self._test_dataset, batch_size=batch_size,
                                          shuffle=False, dataset_name=self.dataset_name())

    def __retrieve_dataset_from_s3(
            self,
            root_dir: str = default_root_directory,
            s3_bucket_name:str = "distributed-notebook-public",
            aws_region:str = "us-east-1",
    ):
        # Check if it already exists.
        if os.path.exists(root_dir):
            self.log.debug(f'"{self.dataset_name()}" dataset is already downloaded. '
                           f'No need to retrieve it from AWS S3.')
            return

        # Download the dataset, or load it from the cache.
        self._download_start: float = time.time()

        datasets_directory:str = os.path.expanduser("~/.cache/huggingface/datasets/")
        extract_path:str = os.path.join(datasets_directory, self.dataset_root_directory)

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

        self._download_end: float = time.time()
        self._download_duration_sec = self._download_end - self._download_start

        self.log.debug(f"The {self.name} dataset was downloaded to root directory \"{self._root_dir}\" "
                       f"from AWS S3 in {self._download_duration_sec} seconds.")

    @property
    def dataset_root_directory(self)->str:
        return "zh-plus___tiny-imagenet"

    @staticmethod
    def category() -> str:
        return ComputerVision

    @staticmethod
    def model_constructor_args() -> Dict[str, Any]:
        return {
            "out_features": 200,
        }

    @staticmethod
    def dataset_name() -> str:
        return "Tiny ImageNet"

    @property
    def name(self) -> str:
        return TinyImageNet.dataset_name()

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
    def description(self) -> dict[str, str | int | bool]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["batch_size"] = self._batch_size
        desc["image_size"] = self._image_size
        return desc

    @staticmethod
    def dataset_shortname()->str:
        return "tiny-imagenet"

    @staticmethod
    def huggingface_directory_name()->str:
        return "zh-plus___tiny-imagenet"

    @property
    def train_loader(self)->Optional[DataLoader]:
        return self._train_loader

    @property
    def test_loader(self)->Optional[DataLoader]:
        return self._test_loader