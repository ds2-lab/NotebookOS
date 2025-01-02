import gc
import time
from typing import Optional, Dict, Any

import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import models

from distributed_notebook.models.model import DeepLearningModel

ResNet18Name = "ResNet-18"

class ResNet18(DeepLearningModel):
    def __init__(
            self,
            out_features: int = 10,
            optimizer: Optional[nn.Module] = None,
            optimizer_state_dict: Optional[Dict[str, Any]] = None,
            criterion: Optional[nn.Module] = None,
            criterion_state_dict: Optional[Dict[str, Any]] = None,
            model_state_dict: Optional[Dict[str, Any]] = None,
            created_for_first_time: bool = False,
            **kwargs,
    ):
        super().__init__(
            name=ResNet18Name,
            criterion = criterion,
            criterion_state_dict = criterion_state_dict,
            out_features=out_features,
            created_for_first_time=created_for_first_time,
            **kwargs,
        )

        self.model = models.resnet18(pretrained=False)
        self.model.fc = nn.Linear(self.model.fc.in_features, out_features)  # Modify the fully connected layer for 10 classes

        if model_state_dict is not None:
            self.model.load_state_dict(model_state_dict)

        if optimizer is not None:
            self._optimizer = optimizer
        else:
            self._optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4)

        if optimizer_state_dict is not None:
            self._optimizer.load_state_dict(optimizer_state_dict)

    @property
    def constructor_args(self)->dict[str, Any]:
        base_args: dict[str, Any] = super(ResNet18).constructor_args
        args: dict[str, Any] = {}
        base_args.update(args)
        return base_args

    def __str__(self)->str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"

    def __repr__(self)->str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"