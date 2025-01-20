from .custom_dataset import CustomDataset
from .speech import LibriSpeech
from .nlp import CoLA, IMDbLargeMovieReview, IMDbLargeMovieReviewTruncated
from .cv import CIFAR10, CIFAR100, TinyImageNet

from distributed_notebook.deep_learning.configuration import ComputerVision, Speech, NaturalLanguageProcessing

from typing import Type, List, Dict, Optional

ALL_DATASET_CLASSES: List[Type[CustomDataset]] = [
    CIFAR10, CIFAR100, TinyImageNet,
    CoLA, IMDbLargeMovieReview, IMDbLargeMovieReviewTruncated,
    LibriSpeech,
]

DatasetCategories: List[str] = [
    ComputerVision,
    NaturalLanguageProcessing,
    Speech
]

DatasetClassesByCategory: Dict[str, List[Type[CustomDataset]]] = {
    ComputerVision: [
        CIFAR10, CIFAR100, TinyImageNet
    ],
    NaturalLanguageProcessing: [
        CoLA, IMDbLargeMovieReview, IMDbLargeMovieReviewTruncated
    ],
    Speech: [
        LibriSpeech
    ]
}

DatasetNamesByCategory: Dict[str, List[str]] = {
    ComputerVision: [
        CIFAR10.dataset_name(), CIFAR100.dataset_name(), TinyImageNet.dataset_name()
    ],
    NaturalLanguageProcessing: [
        CoLA.dataset_name(), IMDbLargeMovieReview.dataset_name(), IMDbLargeMovieReviewTruncated.dataset_name()
    ],
    Speech: [
        LibriSpeech.dataset_name()
    ]
}

DatasetClassesByName: Dict[str, Type[CustomDataset]] = {
    CIFAR10.dataset_name(): CIFAR10,
    CIFAR100.dataset_name(): CIFAR100,
    TinyImageNet.dataset_name(): TinyImageNet,
    CoLA.dataset_name(): CoLA,
    IMDbLargeMovieReview.dataset_name(): IMDbLargeMovieReview,
    IMDbLargeMovieReviewTruncated.dataset_name(): IMDbLargeMovieReviewTruncated,
    LibriSpeech.dataset_name(): LibriSpeech,
}

DatasetNameToModelCategory: Dict[str, Type[CustomDataset]] = {
    CIFAR10.dataset_name(): ComputerVision,
    CIFAR100.dataset_name(): ComputerVision,
    TinyImageNet.dataset_name(): ComputerVision,
    CoLA.dataset_name(): NaturalLanguageProcessing,
    IMDbLargeMovieReview.dataset_name(): NaturalLanguageProcessing,
    IMDbLargeMovieReviewTruncated.dataset_name(): NaturalLanguageProcessing,
    LibriSpeech.dataset_name(): Speech,
}

def load_dataset(dataset_description: dict[str, str|bool|int])->CustomDataset:
    if 'name' not in dataset_description:
        raise ValueError(f"dataset description does not contain a '_name' field: {dataset_description}")

    dataset_name:str = dataset_description.pop('name')

    cls: Optional[Type] = None
    for dataset_class in ALL_DATASET_CLASSES:
        assert issubclass(dataset_class, CustomDataset)

        if dataset_name == dataset_class.dataset_name():
            cls = dataset_class
            break

    if cls is None:
        raise ValueError(f"unknown or unsupported dataset \"{dataset_name}\"")

    assert issubclass(cls, CustomDataset)

    print(f'Passing the following keyword arguments to constructor of dataset "{dataset_name}":', flush = True)
    for k, v in dataset_description.items():
        print(f'\t"{k}": {v}', flush = True)

    return cls(**dataset_description)