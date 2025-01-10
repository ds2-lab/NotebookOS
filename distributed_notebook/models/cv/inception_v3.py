import gc
import time
from typing import Optional, Dict, Any

import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import models
from torchvision.transforms import transforms

from distributed_notebook.models.model import DeepLearningModel

class InceptionV3(DeepLearningModel):
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
            criterion = criterion,
            criterion_state_dict = criterion_state_dict,
            out_features=out_features,
            created_for_first_time=created_for_first_time,
            **kwargs,
        )

        # transform = transforms.Compose([
        #     transforms.Resize((299, 299)),  # Resize to 299x299 to match Inception v3 input size
        #     transforms.ToTensor(),
        #     transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])  # Normalize as required by Inception v3
        # ])

        self.model = models.inception_v3(pretrained=False)
        self.model.aux_logits = False  # Disable auxiliary outputs for simplicity
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
    def name(self) -> str:
        return "Inception v3"

    @property
    def constructor_args(self)->dict[str, Any]:
        base_args: dict[str, Any] = super(InceptionV3).constructor_args
        args: dict[str, Any] = {}
        base_args.update(args)
        return base_args

    def __str__(self)->str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"

    def __repr__(self)->str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"