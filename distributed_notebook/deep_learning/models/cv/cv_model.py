from abc import ABC, abstractmethod

from torch import nn

from distributed_notebook.deep_learning.configuration import ComputerVision
from distributed_notebook.deep_learning.models.model import DeepLearningModel

from typing import Dict, Any, Type

class ComputerVisionModel(DeepLearningModel, ABC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def constructor_args(self) -> Dict[str, Any]:
        return super().constructor_args

    @property
    @abstractmethod
    def output_layer(self) -> nn.Module:
        pass

    @staticmethod
    @abstractmethod
    def model_name() -> str:
        pass

    @staticmethod
    @abstractmethod
    def expected_image_size() -> int:
        pass

    @staticmethod
    @abstractmethod
    def expected_model_class() -> Type:
        pass

    @staticmethod
    def category() -> str:
        return ComputerVision