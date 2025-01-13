import os.path
from typing import Optional

from torchvision import transforms

from distributed_notebook.deep_learning.datasets.hugging_face import HuggingFaceDataset
from distributed_notebook.deep_learning.datasets.nlp.util import get_username


class TinyImageNet(HuggingFaceDataset):
    default_root_directory: str = os.path.expanduser("~/.cache/huggingface/datasets/zh-plus___tiny-imagenet")

    # https://huggingface.co/datasets/zh-plus/tiny-imagenet
    hugging_face_dataset_name: str = "zh-plus/tiny-imagenet"

    text_feature_column_name: str = "text"

    hugging_face_dataset_config_name: Optional[str] = None

    def __init__(self, root_dir:str = default_root_directory, shuffle: bool = True, num_workers: int = 2):
        super().__init__(
            root_dir=root_dir,
            shuffle=shuffle,
            num_workers=num_workers,
            hugging_face_dataset_name=TinyImageNet.hugging_face_dataset_name,
            hugging_face_dataset_config_name=TinyImageNet.hugging_face_dataset_config_name,
        )

        self.transform = transforms.Compose([
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])

        # Prepare the data loaders
        self._train_loader = self._dataset["train"]
        self._test_loader = self._dataset["valid"]

    @property
    def name(self) -> str:
        return TinyImageNet.dataset_name()

    @staticmethod
    def dataset_name() -> str:
        return "Tiny ImageNet"

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
        return super().description

    @property
    def train_loader(self):
        return self._train_loader

    @property
    def test_loader(self):
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
