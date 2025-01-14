from .datasets import CIFAR10, TinyImageNet, CoLA, IMDbLargeMovieReview, IMDbLargeMovieReviewTruncated, LibriSpeech
from .models import ResNet18, VGG11, VGG13, VGG16, VGG19, InceptionV3, \
    Bert, GPT2, DeepSpeech, DeepSpeech2, DeepLearningModel, ComputerVisionModel, SimpleModel

from typing import Type, List, Dict

ModelNameToCompatibleDatasetClasses: Dict[str, List[Type]] = {
    ResNet18.model_name(): [
        CIFAR10, TinyImageNet
    ],
    VGG11.model_name(): [
        CIFAR10, TinyImageNet
    ],
    VGG13.model_name(): [
        CIFAR10, TinyImageNet
    ],
    VGG16.model_name(): [
        CIFAR10, TinyImageNet
    ],
    VGG19.model_name(): [
        CIFAR10, TinyImageNet
    ],
    InceptionV3.model_name(): [
        CIFAR10, TinyImageNet
    ],
    Bert.model_name(): [
        CoLA, IMDbLargeMovieReview, IMDbLargeMovieReviewTruncated
    ],
    GPT2.model_name(): [
        CoLA, IMDbLargeMovieReview, IMDbLargeMovieReviewTruncated
    ],
    DeepSpeech.model_name(): [
        LibriSpeech
    ],
    DeepSpeech2.model_name(): [
        LibriSpeech
    ],
}