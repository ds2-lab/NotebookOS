import os.path
import time

from typing import Dict, Any, Union, Optional

from torch.utils.data import DataLoader
from torchvision import datasets, transforms

from distributed_notebook.deep_learning.datasets.custom_dataset import CustomDataset
from distributed_notebook.deep_learning.datasets.loader import WrappedLoader

from distributed_notebook.deep_learning.configuration import ComputerVision

class CIFAR100(CustomDataset):
    default_root_directory: str = os.path.expanduser("~/.cache/distributed_notebook/datasets/cifar100")

    def __init__(
            self,
            root_dir: str = default_root_directory,
            batch_size: int = 4,
            shuffle: bool = True,
            num_workers: int = 2,
            image_size: int = 224,
    ):
        super().__init__(root_dir=root_dir, shuffle=shuffle, num_workers=num_workers)

        self.log.debug(f'Creating {self.dataset_name()} dataset with root directory "{root_dir}", batch size = {batch_size}, '
                       f'shuffle = {shuffle}, number of workers = {num_workers}, '
                       f'and image size = ({image_size}, {image_size}).')

        self.transform = transforms.Compose([
            transforms.Resize((image_size, image_size)),
            transforms.CenterCrop((image_size, image_size)),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize(mean = (0.5071, 0.4867, 0.4408), std = (0.2675, 0.2565, 0.2761))  # CIFAR-100 mean and std
        ])

        self._dataset_already_downloaded: bool = self._check_if_downloaded(
            filenames=datasets.CIFAR100.train_list + datasets.CIFAR100.test_list,
            base_folder=datasets.CIFAR100.base_folder
        )

        self._image_size: int = image_size

        self._download_start = time.time()
        self._train_dataset = datasets.CIFAR100(root=root_dir, train=True, download=True, transform=self.transform)
        self._test_dataset = datasets.CIFAR100(root=root_dir, train=False, download=True, transform=self.transform)
        self._download_end = time.time()
        self._download_duration_sec = self._download_end - self._download_start

        self._train_loader = WrappedLoader(self._train_dataset, batch_size=batch_size, shuffle=shuffle,
                                           num_workers=num_workers, dataset_name=self.dataset_name())
        self._test_loader = WrappedLoader(self._test_dataset, batch_size=batch_size, shuffle=shuffle,
                                          num_workers=num_workers, dataset_name=self.dataset_name())

        if self._dataset_already_downloaded:
            print(f"The {self.name} dataset was already downloaded. Root directory: \"{root_dir}\"")
        else:
            print(
                f"The {self.name} dataset was downloaded to root directory \"{root_dir}\" in {self._download_duration_sec} seconds.")

    @staticmethod
    def category() -> str:
        return ComputerVision

    @staticmethod
    def model_constructor_args() -> Dict[str, Any]:
        return {
            "out_features": 10,
        }

    @staticmethod
    def dataset_name() -> str:
        return "CIFAR-100"

    @property
    def name(self) -> str:
        return CIFAR100.dataset_name()

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
    def description(self) -> dict[str, str | int | bool]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["image_size"] = self._image_size
        return desc

    @property
    def train_dataset(self):
        return self._train_dataset

    @property
    def train_loader(self)->Optional[DataLoader]:
        return self._train_loader

    @property
    def test_dataset(self):
        return self._test_dataset

    @property
    def test_loader(self)->Optional[DataLoader]:
        return self._test_loader

    @property
    def requires_tokenization(self) -> bool:
        return False

    @property
    def tokenization_start(self) -> float:
        return -1.0

    @property
    def tokenization_end(self) -> float:
        return -1.0

    @property
    def tokenization_duration_sec(self) -> float:
        return -1.0
