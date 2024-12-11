import torch
import gc
import time
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, transforms, models
from torch.utils.data import DataLoader

import logging
import time

from distributed_notebook.logging import ColoredLogFormatter

class DeepLearningModel(object):
    def __init__(self, criterion = None):
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
            self.criterion = criterion
        else:
            self.criterion = nn.CrossEntropyLoss()

        self.total_epochs_trained: int = 0
        self.model = None
        self.optimizer = None

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
        for state in self.optimizer.state.values():
            for k, v in state.items():
                if isinstance(v, torch.Tensor):
                    state[k] = v.to(self.gpu_device)

        et_optimizer: float = time.time()

        self.criterion = self.criterion.to(self.gpu_device)

        et_criterion: float = time.time()

        total_time_elapsed: float = et_criterion - st
        self.log.debug(f"Finished moving RESNET-18 model, optimizer, and criterion to GPU. Model size: {size_mb} MB.")
        self.log.debug(f"\tTotal time elapsed: {total_time_elapsed} seconds.")
        self.log.debug(f"\t\tCopied model in {et_optimizer - et_model} seconds.")
        self.log.debug(f"\t\tCopied model in {et_criterion - et_optimizer} seconds.")

        return total_time_elapsed

    def to_cpu(self)->float: # Return the total time elapsed in seconds.
        size_mb: float = self.size_mb
        self.log.debug(f"Moving RESNET-18 model, optimizer, and criterion to the CPU. Model size: {size_mb} MB.")

        st: float = time.time()
        # Move the model to the CPU.
        self.model = self.model.to(self.cpu_device)
        et_model: float = time.time()

        # Move the optimizer back to CPU
        for state in self.optimizer.state.values():
            for k, v in state.items():
                if isinstance(v, torch.Tensor):
                    state[k] = v.cpu()

        et_optimizer: float = time.time()

        self.criterion = self.criterion.cpu()

        et_criterion: float = time.time()

        total_time_elapsed: float = et_criterion - st
        self.log.debug(f"Finished moving RESNET-18 model, optimizer, and criterion to CPU. Model size: {size_mb} MB.")
        self.log.debug(f"\tTotal time elapsed: {total_time_elapsed} seconds.")
        self.log.debug(f"\t\tCopied model in {et_optimizer - et_model} seconds.")
        self.log.debug(f"\t\tCopied model in {et_criterion - et_optimizer} seconds.")

        return total_time_elapsed