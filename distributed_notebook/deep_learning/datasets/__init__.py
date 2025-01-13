from .speech import LibriSpeech
from .nlp import CoLA, IMDbLargeMovieReview, IMDbLargeMovieReviewTruncated
from .cv import CIFAR10, TinyImageNet

from typing import Type, List, Dict

ComputerVision: str = "Computer Vision (CV)"
NaturalLanguageProcessing: str = "Natural Language Processing (NLP)"
Speech: str = "Speech"

DatasetCategories: List[str] = [
    ComputerVision,
    NaturalLanguageProcessing,
    Speech
]

DatasetClassesByCategory: Dict[str, List[Type]] = {
    ComputerVision: [
        CIFAR10, TinyImageNet
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
        CIFAR10.dataset_name(), TinyImageNet.dataset_name()
    ],
    NaturalLanguageProcessing: [
        CoLA.dataset_name(), IMDbLargeMovieReview.dataset_name(), IMDbLargeMovieReviewTruncated.dataset_name()
    ],
    Speech: [
        LibriSpeech.dataset_name()
    ]
}

DatasetClassesByName: Dict[str, Type] = {
    CIFAR10.dataset_name(): CIFAR10,
    TinyImageNet.dataset_name(): TinyImageNet,
    CoLA.dataset_name(): CoLA,
    IMDbLargeMovieReview.dataset_name(): IMDbLargeMovieReview,
    IMDbLargeMovieReviewTruncated.dataset_name(): IMDbLargeMovieReviewTruncated,
    LibriSpeech.dataset_name(): LibriSpeech,
}

DatasetNameToModelCategory: Dict[str, Type] = {
    CIFAR10.dataset_name(): ComputerVision,
    TinyImageNet.dataset_name(): ComputerVision,
    CoLA.dataset_name(): NaturalLanguageProcessing,
    IMDbLargeMovieReview.dataset_name(): NaturalLanguageProcessing,
    IMDbLargeMovieReviewTruncated.dataset_name(): NaturalLanguageProcessing,
    LibriSpeech.dataset_name(): Speech,
}