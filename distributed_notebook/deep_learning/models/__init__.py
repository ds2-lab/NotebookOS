from typing import Type, List, Dict

from .cv import ResNet18, InceptionV3, VGG16, ComputerVisionModel
from .speech import DeepSpeech2
from .nlp import Bert, GPT2
from .model import DeepLearningModel
from .simple_model import SimpleModel

ComputerVision: str = "Computer Vision (CV)"
NaturalLanguageProcessing: str = "Natural Language Processing (NLP)"
Speech: str = "Speech"

ModelCategories: List[str] = [
    ComputerVision,
    NaturalLanguageProcessing,
    Speech
]

ModelClassesByCategory: Dict[str, List[Type]] = {
    ComputerVision: [
        ResNet18, InceptionV3, VGG16
    ],
    NaturalLanguageProcessing: [
        Bert, GPT2
    ],
    Speech: [
        DeepSpeech2
    ]
}

ModelClassesByName: Dict[str, Type] = {
    ResNet18.model_name(): ResNet18,
    VGG16.model_name(): VGG16,
    InceptionV3.model_name(): InceptionV3,
    Bert.model_name(): Bert,
    GPT2.model_name(): GPT2,
    DeepSpeech2.model_name(): DeepSpeech2,
}

ModelNameToModelCategory: Dict[str, str] = {
    ResNet18.model_name(): ComputerVision,
    VGG16.model_name(): ComputerVision,
    InceptionV3.model_name(): ComputerVision,
    Bert.model_name(): NaturalLanguageProcessing,
    GPT2.model_name(): NaturalLanguageProcessing,
    DeepSpeech2.model_name(): Speech,
}