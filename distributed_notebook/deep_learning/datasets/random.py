import torch
from torch import Tensor
from torchvision import datasets, transforms
from torch.utils.data import DataLoader, Dataset

from distributed_notebook.deep_learning.datasets.custom_dataset import CustomDataset

import time
import os

class RandomDataset(Dataset):
    def __init__(self, tensor_size: int, num_samples: int = 100):
        """
        Args:
            num_samples (int): Number of samples to generate.
            tensor_size (tuple): Shape of each tensor.
        """
        self.num_samples = num_samples
        self.tensor_size = tensor_size

        self.data: list[Tensor] = []
        self.targets: list[Tensor] = []

        for _ in range(0, self.num_samples):
            X = torch.randn(1, tensor_size)
            # Targets: Linear combination of inputs with some noise
            true_weights = torch.rand(tensor_size)
            true_bias = 0.1
            y = X @ true_weights + true_bias + torch.randn(1) * 0.1
            y = y.unsqueeze(1)  # Make sure y has the right shape

            self.data.append(X)
            self.targets.append(y)

    def __len__(self):
        """Return the total number of samples."""
        return self.num_samples

    def __getitem__(self, idx):
        """Return a randomly generated tensor."""
        return self.data[idx], self.targets[idx]

class RandomCustomDataset(CustomDataset):
    """
    RandomCustomDataset is a simple dataset intended to be used for unit testing.
    """

    def __init__(
            self,
            input_size: int,
            num_training_samples: int = 512,
            num_test_samples: int = 64,
            root_dir:str = 'data',
            batch_size: int = 64,
            shuffle: bool = True,
            num_workers: int = 2,
            **kwargs):
        super().__init__(root_dir = root_dir, shuffle = shuffle, num_workers = num_workers)

        assert batch_size <= num_training_samples

        self.transform = transforms.Compose([
            transforms.ToTensor(),
        ])

        self._tensor_size = input_size
        self._num_training_samples: int = num_training_samples
        self._num_test_samples: int = num_test_samples
        self._dataset_already_downloaded: bool = True

        self._train_dataset: RandomDataset = RandomDataset(self._tensor_size, num_samples = num_training_samples)
        self._test_dataset: RandomDataset = RandomDataset(self._tensor_size, num_samples = num_test_samples)

        self._train_loader = DataLoader(self._train_dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers)
        self._test_loader = DataLoader(self._test_dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers)

    @property
    def download_duration_sec(self)->float:
        return 0

    @property
    def dataset_already_downloaded(self)->bool:
        return self._dataset_already_downloaded

    @property
    def download_start_time(self)->float:
        return -1

    @property
    def download_end_time(self)->float:
        return -1

    @property
    def download_duration(self)->float:
        return 0

    @property
    def download_start(self)->float:
        return -1

    @property
    def download_end(self)->float:
        return -1

    @property
    def description(self)->dict[str, str|int|bool]:
        return {
            "name": self._name,
            "root_dir": self._root_dir,
            "shuffle": self._shuffle,
            "num_workers": self._num_workers,
        }

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
    def tokenization_start(self) -> float:
        return -1

    @property
    def tokenization_end(self) -> float:
        return -1

    @property
    def tokenization_duration_sec(self) -> float:
        return -1

    @property
    def requires_tokenization(self) -> bool:
        return False

    @property
    def name(self) -> str:
        return "Random Custom Dataset"