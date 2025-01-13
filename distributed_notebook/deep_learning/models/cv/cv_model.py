from abc import ABC, abstractmethod

from torch import nn

from distributed_notebook.deep_learning.models.model import DeepLearningModel


class ComputerVisionModel(DeepLearningModel, ABC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

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