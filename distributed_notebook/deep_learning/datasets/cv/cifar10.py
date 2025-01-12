from torchvision import datasets, transforms
from torch.utils.data import DataLoader

import time

from distributed_notebook.datasets.custom_dataset import CustomDataset

class CIFAR10(CustomDataset):
    def __init__(self, root_dir:str = 'data', batch_size: int = 256, shuffle: bool = True, num_workers: int = 2):
        super().__init__(root_dir = root_dir, shuffle = shuffle, num_workers = num_workers)

        self.transform = transforms.Compose([
            transforms.RandomHorizontalFlip(),
            transforms.RandomCrop(32, padding=4),
            transforms.ToTensor(),
            transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010))  # CIFAR-10 mean and std
        ])

        self._dataset_already_downloaded: bool = self._check_if_downloaded(
            filenames = datasets.CIFAR10.train_list + datasets.CIFAR10.test_list,
            base_folder = datasets.CIFAR10.base_folder
        )

        self._download_start = time.time()
        self._train_dataset = datasets.CIFAR10(root=root_dir, train=True, download=True, transform=self.transform)
        self._test_dataset = datasets.CIFAR10(root=root_dir, train=False, download=True, transform=self.transform)
        self._download_end = time.time()
        self._download_duration_sec = self._download_end - self._download_start

        self._train_loader = DataLoader(self._train_dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers)
        self._test_loader = DataLoader(self._test_dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers)

        if self._dataset_already_downloaded:
            print(f"The {self.name} dataset was already downloaded. Root directory: \"{root_dir}\"")
        else:
            print(f"The {self.name} dataset was downloaded to root directory \"{root_dir}\" in {self._download_duration_sec} seconds.")

    @property
    def name(self)->str:
        return CIFAR10.dataset_name()

    @staticmethod
    def dataset_name()->str:
        return "CIFAR-10"

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
    def description(self)->dict[str, str|int|bool]:
        return super().description

    @property
    def train_dataset(self):
        return self._train_dataset

    @property
    def train_loader(self):
        return self._train_loader

    @property
    def test_dataset(self):
        return self._test_dataset

    @property
    def test_loader(self):
        return self._test_loader

    @property
    def requires_tokenization(self)->bool:
        return False

    @property
    def tokenization_start(self)->float:
        return -1.0

    @property
    def tokenization_end(self)->float:
        return -1.0

    @property
    def tokenization_duration_sec(self)->float:
        return -1.0