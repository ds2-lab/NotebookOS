from abc import ABC, abstractmethod

from distributed_notebook.deep_learning.datasets.custom_dataset import CustomDataset
from distributed_notebook.deep_learning.models.model import DeepLearningModel
from distributed_notebook.sync.log import SynchronizedValue

from typing import Optional

class SyncPointer(SynchronizedValue, ABC):
    """
    SyncPointer is a base class for any pointer type.

    Pointers are a special kind of SynchronizedValue that references some object stored in remote storage.

    The pointer encodes information about how to retrieve the object.

    Pointers are used for objects that are too large to be replicated efficiently using the standard SMR protocol
    (e.g., 10s or 100s of MB or larger).
    """
    def __init__(
            self,
            large_object_name: str = "",
            user_namespace_variable_name: str = "",
            **kwargs
    ):
        super().__init__(None, None, **kwargs)

        self._large_object_name: str = large_object_name
        self._user_namespace_variable_name:str = user_namespace_variable_name

    @property
    def large_object_name(self)->str:
        return self._large_object_name

    @property
    def user_namespace_variable_name(self)->str:
        return self._user_namespace_variable_name

    @property
    def path(self)->str:
        return str(self._key)

    def __setstate__(self, d):
        self.__dict__.update(d)

    @abstractmethod
    def __getstate__(self):
        pass

class DatasetPointer(SyncPointer):
    """
    DatasetPointer is a SyncPointer for datasets.
    """
    def __init__(
            self,
            dataset: CustomDataset = None,
            dataset_remote_storage_path:str = "",
            user_namespace_variable_name: str = "",
            **kwargs
    ):
        super().__init__(
            large_object_name= dataset.name,
            user_namespace_variable_name = user_namespace_variable_name,
            key = dataset_remote_storage_path,
            **kwargs
        )

        self._dataset: Optional[CustomDataset] = dataset
        self._dataset_description: dict[str, str|int|bool] = dataset.description

    def __getstate__(self):
        d = dict(self.__dict__)

        del d['_dataset']

        return d

    @property
    def dataset_name(self)->str:
        return self._large_object_name

    @property
    def dataset(self)->Optional[CustomDataset]:
        return self._dataset

    @property
    def dataset_description(self)->Optional[dict[str, str|int|bool]]:
        return self._dataset_description

class ModelPointer(SyncPointer):
    """
    ModelPointer is a SyncPointer for models/model parameters.
    """
    def __init__(
            self,
            deep_learning_model: DeepLearningModel = None,
            model_path:str = "",
            user_namespace_variable_name: str = "",
            **kwargs
    ):
        assert deep_learning_model is not None

        super().__init__(
            large_object_name= deep_learning_model.name,
            key = model_path,
            user_namespace_variable_name = user_namespace_variable_name,
            **kwargs
        )

        self._model: DeepLearningModel = deep_learning_model
        self._out_features: int = deep_learning_model.out_features
        self._total_training_time_seconds: int = deep_learning_model.total_training_time_seconds
        self._total_num_epochs: int = deep_learning_model.total_num_epochs

        if hasattr(deep_learning_model, "input_size"):
            self._input_size: int = deep_learning_model.input_size

    def __getstate__(self):
        d = dict(self.__dict__)

        if '_model' in d:
            del d['_model']

        return d

    def wrote_model_state(self):
        """
        Record that we wrote the model's state to remote storage.
        """
        if not self._model.requires_checkpointing:
            raise ValueError(f"model '{self._model.name}' does not seem to require checkpointing")

        self._model.checkpointed()

    @property
    def model_name(self)->str:
        return self._large_object_name

    @property
    def total_num_epochs(self)->int:
        return self._total_num_epochs

    @property
    def total_training_time_seconds(self)->int:
        return self._total_training_time_seconds

    @property
    def out_features(self)->int:
        return self._out_features

    @property
    def input_size(self)->Optional[int]:
        if self._model is not None and hasattr(self._model, "input_size"):
            return self._model.input_size
        elif hasattr(self, "_input_size"):
            return self._input_size

        return None

    @property
    def model(self)->Optional[DeepLearningModel]:
        return self._model