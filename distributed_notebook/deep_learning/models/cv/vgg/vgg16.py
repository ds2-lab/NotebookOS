from typing import Optional, Dict, Any, Type

import torch.nn as nn
import torch.optim as optim
from torchvision import models

from distributed_notebook.deep_learning.models.cv.cv_model import ComputerVisionModel


class VGG16(ComputerVisionModel):
    def __init__(
            self,
            out_features: int = 10, # 10 for CIFAR-10, 200 for Tiny ImageNet
            optimizer: Optional[nn.Module] = None,
            optimizer_state_dict: Optional[Dict[str, Any]] = None,
            criterion: Optional[nn.Module] = None,
            criterion_state_dict: Optional[Dict[str, Any]] = None,
            model_state_dict: Optional[Dict[str, Any]] = None,
            created_for_first_time: bool = False,
            **kwargs,
    ):
        super().__init__(
            criterion=criterion,
            criterion_state_dict=criterion_state_dict,
            out_features=out_features,
            created_for_first_time=created_for_first_time,
            **kwargs,
        )

        self.model = models.vgg16(pretrained=False, num_classes=out_features)
        self._output_layer: nn.Module = self.model.classifier[-1]

        if model_state_dict is not None:
            self.model.load_state_dict(model_state_dict)

        if optimizer is not None:
            self._optimizer = optimizer
        else:
            self._optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4)

        if optimizer_state_dict is not None:
            self._optimizer.load_state_dict(optimizer_state_dict)

    @staticmethod
    def model_name() -> str:
        return "VGG-16"

    @staticmethod
    def expected_image_size() -> int:
        return 224

    @staticmethod
    def expected_model_class() -> Type:
        return models.VGG

    @property
    def name(self) -> str:
        return VGG16.model_name()

    @property
    def output_layer(self) -> nn.Module:
        return self._output_layer

    @property
    def constructor_args(self) -> dict[str, Any]:
        base_args: dict[str, Any] = super().constructor_args
        args: dict[str, Any] = {}
        base_args.update(args)
        return base_args

    def __str__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"

    def __repr__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"
