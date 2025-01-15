import random
from typing import Optional, Tuple, Any, Type, List, Dict

from distributed_notebook.deep_learning.configuration import ComputerVision, NaturalLanguageProcessing, Testing, Speech

from .data import CIFAR10, CIFAR100, TinyImageNet, CoLA, IMDbLargeMovieReview, IMDbLargeMovieReviewTruncated, \
    LibriSpeech, CustomDataset, DatasetNamesByCategory, DatasetClassesByName
from .models import ResNet18, VGG11, VGG13, VGG16, VGG19, InceptionV3, \
    Bert, GPT2, DeepSpeech, DeepSpeech2, DeepLearningModel, ComputerVisionModel, SimpleModel, \
    ModelClassesByName, ModelNameToModelCategory

ModelNameToCompatibleDatasetClasses: Dict[str, List[Type]] = {
    ResNet18.model_name(): [
        CIFAR10, CIFAR100, TinyImageNet
    ],
    VGG11.model_name(): [
        CIFAR10, CIFAR100, TinyImageNet
    ],
    VGG13.model_name(): [
        CIFAR10, CIFAR100, TinyImageNet
    ],
    VGG16.model_name(): [
        CIFAR10, CIFAR100, TinyImageNet
    ],
    VGG19.model_name(): [
        CIFAR10, CIFAR100, TinyImageNet
    ],
    InceptionV3.model_name(): [
        CIFAR10, CIFAR100, TinyImageNet
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


def get_model_and_dataset(
        deep_learning_model_name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        dataset_kwargs: Dict[str, Any] = None,
) -> Tuple[DeepLearningModel, CustomDataset]:
    """
    Assign a deep learning model to this kernel.

    If deep_learning_model_name is a valid model name, then assign the specified model.
    Otherwise, assign the default model (ResNet-18).

    :param dataset_name: name of dataset to assign
    :param deep_learning_model_name: name of model to assign.
    """
    model_arguments: Dict[str, Any] = {}
    dataset_arguments: Dict[str, Any] = {}

    if deep_learning_model_name is None or deep_learning_model_name == "":
        print("No deep learning model specified. Using default model (ResNet-18).", flush = True)
        deep_learning_model_name = "ResNet-18"
    else:
        print(f"Will be creating instance of '{deep_learning_model_name}' model.", flush = True)

    if deep_learning_model_name not in ModelClassesByName:
        raise ValueError(f'Unknown or unsupported deep learning model specified: "{deep_learning_model_name}"')

    model_class: Type[DeepLearningModel] = ModelClassesByName[deep_learning_model_name]
    category: str = ModelNameToModelCategory[model_class.model_name()]
    if dataset_name is None or dataset_name == "":
        print(f"No dataset specified. Will randomly select dataset from '{category}' category.", flush = True)

        dataset_names: List[str] = DatasetNamesByCategory[category]
        dataset_name: str = random.choice(dataset_names)

    print(f"Creating and assigning {dataset_name} dataset to this kernel.", flush = True)

    if dataset_name not in DatasetClassesByName:
        print(f'Unknown or unsupported dataset specified: "{dataset_name}"', flush = True)
        raise ValueError(f'Unknown or unsupported dataset specified: "{dataset_name}"')

    dataset_class: Type = DatasetClassesByName[dataset_name]
    model_class: Type = ModelClassesByName[deep_learning_model_name]

    if category == ComputerVision:
        assert issubclass(model_class, ComputerVisionModel)
        dataset_arguments["image_size"] = model_class.expected_image_size()
    elif category == NaturalLanguageProcessing:
        assert issubclass(model_class, Bert) or issubclass(model_class, GPT2)
        dataset_arguments["model_name"] = model_class.model_name()

    if dataset_kwargs is not None:
        dataset_arguments.update(dataset_kwargs)
    dataset = dataset_class(**dataset_arguments)

    # If this particular dataset has a 'model_constructor_args' method, then call it.
    if hasattr(dataset_class, "model_constructor_args"):
        model_constructor_args: Dict[str, Any] = dataset_class.model_constructor_args()
        model_arguments.update(model_constructor_args)

    model = model_class(created_for_first_time=True, **model_arguments)

    return model, dataset
