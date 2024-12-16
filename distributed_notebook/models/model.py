from typing import Optional, Dict, Any

import torch
import gc
import time
import torch.nn as nn
import torch.optim as optim
from torch.nn import Module
from torch.xpu import device
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
        self._device_ids: list[int] = []

        # Flag that is set to True everytime we train and set to False everytime we write the model's parameters or
        # state dictionary to remote storage.
        self._requires_checkpointing: bool = False

        # List of the times, in seconds, spent copying data from the CPU to the GPU.
        # IMPORTANT: These are NOT checkpointed as of right now.
        self._cpu2gpu_times: list[float] = []

        # List of the times, in seconds, spent copying data from the GPU to the CPU.
        # IMPORTANT: These are NOT checkpointed as of right now.
        self._gpu2cpu_times: list[float] = []

    def set_gpu_device_ids(self, device_ids: list[int] = None):
        """
        Change the GPU device IDs used by the model.

        If the model is wrapped in torch.nn.DataParallel, then this will first unwrap the model before
        either re-wrapping it in torch.nn.DataParallel, or just specifying a new PyTorch device with the
        specified GPU device ID.
        """
        if len(device_ids) == 0:
            raise ValueError("device IDs list is empty")

        st: float = time.time()

        # Unwrap from DataParallel (if in DataParallel).
        if isinstance(self.model, torch.nn.DataParallel):
            old_device_ids: list[int] = self.model.device_ids
            self.log.debug(f"Unwrapping model from DataParallel. Old GPU device IDs: {old_device_ids}.")
            self.model = self.model.module
        else:
            old_device_ids: list[int] = [0]

        if len(device_ids) == 1:
            gpu_device_id: int = device_ids[0]
            self.log.debug(f"Using GPU #{gpu_device_id}")
            self.gpu_device = torch.device(f'cuda:{gpu_device_id}')

            self.model = self.model.to(self.gpu_device)
        else:
            self.log.debug(f"Wrapping model from DataParallel. GPU device IDs: {device_ids}.")
            self.model = torch.nn.DataParallel(self.model, device_ids = device_ids)

        et: float = time.time()
        self.log.debug(f"Changed GPU device IDs from {old_device_ids} to {device_ids} in {et - st} seconds.")

        self._device_ids = device_ids

    def checkpointed(self):
        """
        This should be called whenever the model's state dictionary is written to remote storage.
        """
        if not self._requires_checkpointing:
            raise ValueError(f"model '{self._name}' does not require checkpointing")

        self._requires_checkpointing = False

    @property
    def requires_checkpointing(self)->bool:
        """
        Return a bool indicating whether this model has updated state that needs to be checkpointed.
        """
        return self._requires_checkpointing

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
        self.log.debug(f"\tTotal time elapsed: {total_time_elapsed * 1.0e3} ms.")
        self.log.debug(f"\t\tCopied optimizer in {(et_optimizer - et_model) * 1.0e3} ms.")
        self.log.debug(f"\t\tCopied criterion in {(et_criterion - et_optimizer) * 1.0e3} ms.")

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
        self.log.debug(f"\tTotal time elapsed: {total_time_elapsed * 1.0e3} ms.")
        self.log.debug(f"\t\tCopied optimizer in {(et_optimizer - et_model) * 1.0e6} μs.")
        self.log.debug(f"\t\tCopied criterion in {(et_criterion - et_optimizer) * 1.0e6} μs.")

        self.gpu_to_cpu_times.append(total_time_elapsed)

        return total_time_elapsed