from typing import Dict, Any, Optional
import gc
import time
from typing import Optional, Dict, Any

import torch
import torch.nn as nn
import torch.optim as optim

from distributed_notebook.models.model import DeepLearningModel

class SimpleModule(nn.Module):
    def __init__(self, input_size=1, output_size=1):
        super(SimpleModule, self).__init__()
        # Single linear layer
        self.fc = nn.Linear(in_features = input_size, out_features = output_size)

    def forward(self, x):
        return self.fc(x)

    def set_weights(self, val: float):
        """
        Set all the weights in the model's linear, fully-connected layer to the given value.
        :param val: the value for the weights
        """
        self.fc.weight.data.fill_(val)

    def set_bias(self, val: float):
        """
        Set the bias of the model's linear, fully-connected layer to the given value.
        :param val: the new bias value
        """
        self.fc.bias.data.fill_(val)

    def __str__(self)->str:
        return f"SimpleModule[Bias={self.fc.bias}, Weights={self.fc.weight}]"

    def __repr__(self)->str:
        return self.__str__()

class SimpleModel(DeepLearningModel):
    """
    SimpleModel is an extremely simple PyTorch neural network that is created to be small, light-weight, and to
    expose an API that enables the user to directly modify the network's weights.

    It is only intended to be used in unit tests.
    """
    def __init__(
            self,
            input_size: int = 10,
            out_features: int = 10,
            initial_weights: Optional[float|int] = None,
            initial_bias: Optional[float|int] = None,
            optimizer: Optional[nn.Module] = None,
            optimizer_state_dict: Optional[Dict[str, Any]] = None,
            criterion: Optional[nn.Module] = None,
            criterion_state_dict: Optional[Dict[str, Any]] = None,
            model_state_dict: Optional[Dict[str, Any]] = None,
            **kwargs,
    ):
        super().__init__(
            name="SimpleModel",
            criterion = criterion,
            criterion_state_dict = criterion_state_dict,
            out_features=out_features,
            **kwargs,
        )

        self.model = SimpleModule(input_size = input_size, output_size = out_features)

        if model_state_dict is not None:
            self.model.load_state_dict(model_state_dict)

        if optimizer is not None:
            self._optimizer = optimizer
        else:
            self._optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4)

        if optimizer_state_dict is not None:
            self._optimizer.load_state_dict(optimizer_state_dict)

        if initial_weights is not None:
            assert isinstance(initial_weights, float) or isinstance(initial_weights, int)
            self.model.set_weights(initial_weights)

        if initial_bias is not None:
            assert isinstance(initial_bias, float) or isinstance(initial_bias, int)
            self.model.set_bias(initial_bias)

    def set_weights(self, val: float):
        """
        Set all the weights in the model's linear, fully-connected layer to the given value.
        :param val: the value for the weights
        """
        assert isinstance(self.model, SimpleModule)
        self.model.set_weights(val)

    def set_bias(self, val: float):
        """
        Set the bias of the model's linear, fully-connected layer to the given value.
        :param val: the new bias value
        """
        assert isinstance(self.model, SimpleModule)
        self.model.set_bias(val)

    def __str__(self)->str:
        return f"{self.name} [TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"

    def __repr__(self)->str:
        return f"{self.name} [TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"