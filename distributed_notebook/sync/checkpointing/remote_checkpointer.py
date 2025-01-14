import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

from distributed_notebook.logs import ColoredLogFormatter
from distributed_notebook.sync.checkpointing.pointer import ModelPointer


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
    def storage_name(self) -> str:
        pass

    @abstractmethod
    def read_state_dicts(self, pointer: ModelPointer) -> tuple[
        Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """
        Read the state dictionaries of the model, its optimizer, and its criterion from intermediate storage.

        :param pointer: a pointer to the DeepLearningModel whose state dictionaries we're going to write.
        :return: return the state dictionaries of the model, its optimizer, and its criterion.
        """
        pass

    @abstractmethod
    async def read_state_dicts_async(self, pointer: ModelPointer) -> tuple[
        Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """
        Asynchronously read the state dictionaries of the model, its optimizer, and its criterion from intermediate storage.

        :param pointer: a pointer to the DeepLearningModel whose state dictionaries we're going to write.
        :return: return the state dictionaries of the model, its optimizer, and its criterion.
        """
        pass

    @abstractmethod
    async def write_state_dicts_async(self, pointer: ModelPointer) -> None:
        """
        Asynchronously write the state dictionaries of the model, its optimizer, and its criterion to intermediate storage.

        :param pointer: a pointer to the DeepLearningModel whose state dictionaries we're going to write.
        """
        pass

    @abstractmethod
    def write_state_dicts(self, pointer: ModelPointer) -> None:
        """
        Write the state dictionaries of the model, its optimizer, and its criterion to intermediate storage.

        :param pointer: a pointer to the DeepLearningModel whose state dictionaries we're going to write.
        """
        pass
