from abc import ABC, abstractmethod

import logging
from typing import Any, Dict

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

    @property
    @abstractmethod
    def storage_name(self)->str:
        pass

    @abstractmethod
    def read_state_dicts(self, pointer: ModelPointer)->tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """
        Read the state dictionaries of the model, its optimizer, and its criterion from intermediate storage.

        :param pointer: a pointer to the DeepLearningModel whose state dictionaries we're going to write.
        :return: return the state dictionaries of the model, its optimizer, and its criterion.
        """
        pass

    @abstractmethod
    def write_state_dicts(self, pointer: ModelPointer)->None:
        """
        Write the state dictionaries of the model, its optimizer, and its criterion to intermediate storage.

        :param pointer: a pointer to the DeepLearningModel whose state dictionaries we're going to write.
        """
        pass