from typing import Optional, Dict, Any

import torch
import gc
import time
import torch.nn as nn
import torch.optim as optim
from torch.nn import Module
from torchvision import datasets, transforms, models
from torch.utils.data import DataLoader

import logging
import time

from distributed_notebook.logs import ColoredLogFormatter

from abc import ABC, abstractmethod

class DeepLearningModel(ABC):
    def __init__(
            self,
            name:str = "",
            criterion: Module = None,
            criterion_state_dict: Optional[Dict[str, Any]] = None,
            out_features: int = 10,
            total_training_time_seconds: int = 0,
            total_num_epochs: int = 0,
    ):
        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        self.gpu_device = None
        self.cpu_device = torch.device('cpu')
        if torch.cuda.is_available():
            self.log.debug("CUDA is available.")
            self.gpu_device = torch.device('cuda')
            self.gpu_available: bool = True
        else:
            self.log.warning("CUDA is NOT available.")
            self.gpu_available: bool = False

        if criterion is not None:
            self._criterion: Optional[Module] = criterion
        else:
            self._criterion: Optional[Module] = nn.CrossEntropyLoss()

        if criterion_state_dict is not None:
            self._criterion.load_state_dict(criterion_state_dict)

        self.total_training_time_seconds: int = total_training_time_seconds
        self.total_num_epochs: int = total_num_epochs
        self.model: Optional[Module] = None
        self._optimizer: Optional[Module] = None
        self._name:str = name
        self._out_features: int = out_features

        # List of the times, in seconds, spent copying data from the CPU to the GPU.
        self._cpu2gpu_times: list[float] = []

        # List of the times, in seconds, spent copying data from the GPU to the CPU.
        self._gpu2cpu_times: list[float] = []

    @property
    def cpu_to_gpu_times(self)->list[float]:
        """
        :return: the list of the times, in seconds, spent copying data from the CPU to the GPU.
        """
        return self._cpu2gpu_times

    @property
    def gpu_to_cpu_times(self)->list[float]:
        """
        :return: the list of the times, in seconds, spent copying data from the GPU to the CPU.
        """
        return self._gpu2cpu_times

    @property
    def optimizer(self)->Optional[Module]:
        return self._optimizer

    @property
    def criterion(self)->Optional[Module]:
        return self._criterion

    @abstractmethod
    def apply_model_state_dict(self, model_state_dict: Dict[str, Any]):
        pass

    @abstractmethod
    def apply_optimizer_state_dict(self, optimizer_state_dict: Dict[str, Any]):
        pass

    @abstractmethod
    def apply_criterion_state_dict(self, criterion_state_dict: Dict[str, Any]):
        pass

    @abstractmethod
    def test(self, loader):
        pass

    @abstractmethod
    def train_epochs(self, loader, num_epochs: int = 1):
        pass

    @abstractmethod
    def train(self, loader, training_duration_millis: int|float = 0.0)->tuple[float, float, float]:
        pass

    @property
    def out_features(self)->int:
        return self._out_features

    @property
    def state_dict(self) -> Optional[Dict[str, Any]]:
        if self.model is None:
            return None

        return self.model.state_dict()

    @property
    def optimizer_state_dict(self) -> Optional[Dict[str, Any]]:
        if self._optimizer is None:
            return None

        return self._optimizer.state_dict()

    @property
    def criterion_state_dict(self) -> Optional[Dict[str, Any]]:
        if self._criterion is None:
            return None

        return self._criterion.state_dict()

    @property
    def name(self)->str:
        return self._name

    @property
    def size_bytes(self)->int:
        size_all_mb = self.size_mb
        return int(size_all_mb * 1.0e6)

    @property
    def size_mb(self)->float:
        param_size = 0
        for param in self.model.parameters():
            param_size += param.nelement() * param.element_size()
        buffer_size = 0
        for buffer in self.model.buffers():
            buffer_size += buffer.nelement() * buffer.element_size()

        size_all_mb = (param_size + buffer_size) / 1024**2
        return size_all_mb

    def to_gpu(self)->float: # Return the total time elapsed in seconds.
        if self.gpu_device is None or not self.gpu_available:
            raise ValueError("GPU is unavailable. Cannot move RESNET-18 model, optimizer, and criterion to the GPU.")

        size_mb = self.size_mb
        self.log.debug(f"Moving RESNET-18 model, optimizer, and criterion to the GPU. Model size: {size_mb} MB.")

        st: float = time.time()
        # Move the model to the GPU.
        self.model = self.model.to(self.gpu_device)
        et_model: float = time.time()

        # Move the optimizer back to GPU
        for state in self._optimizer.state.values():
            for k, v in state.items():
                if isinstance(v, torch.Tensor):
                    state[k] = v.to(self.gpu_device)

        et_optimizer: float = time.time()

        self._criterion = self._criterion.to(self.gpu_device)

        et_criterion: float = time.time()

        total_time_elapsed: float = et_criterion - st
        self.log.debug(f"Finished moving RESNET-18 model, optimizer, and criterion to GPU. Model size: {size_mb} MB.")
        self.log.debug(f"\tTotal time elapsed: {total_time_elapsed} seconds.")
        self.log.debug(f"\t\tCopied model in {et_optimizer - et_model} seconds.")
        self.log.debug(f"\t\tCopied model in {et_criterion - et_optimizer} seconds.")

        self.cpu_to_gpu_times.append(total_time_elapsed)

        return total_time_elapsed

    def to_cpu(self)->float: # Return the total time elapsed in seconds.
        size_mb: float = self.size_mb
        self.log.debug(f"Moving RESNET-18 model, optimizer, and criterion to the CPU. Model size: {size_mb} MB.")

        st: float = time.time()
        # Move the model to the CPU.
        self.model = self.model.to(self.cpu_device)
        et_model: float = time.time()

        # Move the optimizer back to CPU
        for state in self._optimizer.state.values():
            for k, v in state.items():
                if isinstance(v, torch.Tensor):
                    state[k] = v.cpu()

        et_optimizer: float = time.time()

        self._criterion = self._criterion.cpu()

        et_criterion: float = time.time()

        total_time_elapsed: float = et_criterion - st
        self.log.debug(f"Finished moving RESNET-18 model, optimizer, and criterion to CPU. Model size: {size_mb} MB.")
        self.log.debug(f"\tTotal time elapsed: {total_time_elapsed} seconds.")
        self.log.debug(f"\t\tCopied model in {et_optimizer - et_model} seconds.")
        self.log.debug(f"\t\tCopied model in {et_criterion - et_optimizer} seconds.")

        self.gpu_to_cpu_times.append(total_time_elapsed)

        return total_time_elapsed