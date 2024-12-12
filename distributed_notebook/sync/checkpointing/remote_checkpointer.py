from abc import ABC, abstractmethod

import logging

from distributed_notebook.datasets.base import Dataset
from distributed_notebook.logging import ColoredLogFormatter
from distributed_notebook.models.model import DeepLearningModel
from distributed_notebook.sync.checkpointing.pointer import DatasetPointer, ModelPointer


class RemoteCheckpointer(ABC):
    def __init__(self):
        self.log = logging.getLogger(__class__.__name__)
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

    @abstractmethod
    def read_dataset(self, pointer: DatasetPointer)->Dataset:
        pass

    @abstractmethod
    def read_model_state_dict(self, pointer: ModelPointer)->DeepLearningModel:
        pass

    @abstractmethod
    def write_dataset(self, pointer: DatasetPointer):
        pass

    @abstractmethod
    def write_model_state_dict(self, pointer: ModelPointer):
        pass