from typing import Type, List, Dict

from distributed_notebook.deep_learning.configuration import NaturalLanguageProcessing, ComputerVision, Speech

from .cv import ResNet18, InceptionV3, VGG11, VGG13, VGG16, VGG19, ComputerVisionModel
from .speech import DeepSpeech, DeepSpeech2
from .nlp import Bert, GPT2
from .model import DeepLearningModel
from .simple_model import SimpleModel

ALL_MODEL_CLASSES: List[Type[DeepLearningModel]] = [
    ResNet18, InceptionV3, VGG11, VGG13, VGG16, VGG19,
    Bert, GPT2,
    DeepSpeech, DeepSpeech2,
    SimpleModel
]
""" ALL_MODEL_CLASSES is a list containing all "concrete" model classes. """

ModelCategories: List[str] = [
    ComputerVision,
    NaturalLanguageProcessing,
    Speech
]

ModelClassesByCategory: Dict[str, List[Type[DeepLearningModel]]] = {
    ComputerVision: [
        ResNet18, InceptionV3, VGG11, VGG13, VGG16, VGG19
    ],
    NaturalLanguageProcessing: [
        Bert, GPT2
    ],
    Speech: [
        DeepSpeech, DeepSpeech2
    ]
}

ModelClassesByName: Dict[str, Type[DeepLearningModel]] = {
    ResNet18.model_name(): ResNet18,
    VGG11.model_name(): VGG11,
    VGG13.model_name(): VGG13,
    VGG16.model_name(): VGG16,
    VGG19.model_name(): VGG19,
    InceptionV3.model_name(): InceptionV3,
    Bert.model_name(): Bert,
    GPT2.model_name(): GPT2,
    DeepSpeech.model_name(): DeepSpeech,
    DeepSpeech2.model_name(): DeepSpeech2,
}

ModelNameToModelCategory: Dict[str, str] = {
    ResNet18.model_name(): ComputerVision,
    VGG11.model_name(): ComputerVision,
    VGG13.model_name(): ComputerVision,
    VGG16.model_name(): ComputerVision,
    VGG19.model_name(): ComputerVision,
    InceptionV3.model_name(): ComputerVision,
    Bert.model_name(): NaturalLanguageProcessing,
    GPT2.model_name(): NaturalLanguageProcessing,
    DeepSpeech.model_name(): DeepSpeech,
    DeepSpeech2.model_name(): Speech,
}