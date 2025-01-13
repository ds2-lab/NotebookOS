import os
from typing import Optional, Dict, Union, Any

from torch.utils.data import Dataset, DataLoader
from torchvision import transforms

from distributed_notebook.deep_learning.datasets.hugging_face import HuggingFaceDataset


class NoneTransform(object):
    """
    Does nothing to the image. To be used instead of None
    """

    def __call__(self, image):
        return image


class TinyImageNetDataset(Dataset):
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


class TinyImageNet(HuggingFaceDataset):
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
            batch_size: int = 64,
            image_size: int = 224,  # 224 x 224 for ResNet-18
    ):
        assert image_size > 0
        assert batch_size > 0

        super().__init__(
            root_dir=root_dir,
            shuffle=shuffle,
            num_workers=num_workers,
            hugging_face_dataset_name=TinyImageNet.hugging_face_dataset_name,
            hugging_face_dataset_config_name=TinyImageNet.hugging_face_dataset_config_name,
        )

        self.transform = transforms.Compose([
            transforms.RandomHorizontalFlip(),
            transforms.Resize((image_size, image_size)),
            transforms.ToTensor(),
            transforms.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225))
        ])

        self._batch_size: int = batch_size
        self._image_size: int = image_size

        self._train_dataset: TinyImageNetDataset = TinyImageNetDataset(self._dataset["train"], transform=self.transform)
        self._test_dataset: TinyImageNetDataset = TinyImageNetDataset(self._dataset["valid"], transform=self.transform)

        # Prepare the data loaders
        self._train_loader = DataLoader(self._train_dataset, batch_size=batch_size, shuffle=shuffle)
        self._test_loader = DataLoader(self._test_dataset, batch_size=batch_size, shuffle=False)

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
