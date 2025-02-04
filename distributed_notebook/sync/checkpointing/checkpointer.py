import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

from distributed_notebook.logs import ColoredLogFormatter
from distributed_notebook.sync.checkpointing.pointer import ModelPointer
from distributed_notebook.sync.storage.remote_storage_provider import RemoteStorageProvider


class Checkpointer(ABC):
    def __init__(self):
        self.log = logging.getLogger(__class__.__name__)
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

    @property
    @abstractmethod
    def storage_provider(self)->RemoteStorageProvider:
        pass

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
    async def write_state_dicts_async(self, pointer: ModelPointer)->list[str]:
        """
        Asynchronously write the state dictionaries of the model, its optimizer, and its criterion to intermediate storage.

        :param pointer: a pointer to the DeepLearningModel whose state dictionaries we're going to write.
        """
        pass

    @abstractmethod
    def write_state_dicts(self, pointer: ModelPointer)->list[str]:
        """
        Write the state dictionaries of the model, its optimizer, and its criterion to intermediate storage.

        :param pointer: a pointer to the DeepLearningModel whose state dictionaries we're going to write.
        """
        pass

    def delete_data(self, key: str)->bool:
        """
        Delete an object from remote storage.
        :param key: the name/key of the object to delete
        :return: True if the object was deleted successfully, otherwise False.
        """
        pass


    async def delete_data_async(self, key: str)->bool:
        """
        Asynchronously delete an object from remote storage.
        :param key: the name/key of the object to delete
        :return: True if the object was deleted successfully, otherwise False.
        """
        pass
