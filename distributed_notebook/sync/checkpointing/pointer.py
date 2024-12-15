from abc import ABC, abstractmethod

from distributed_notebook.datasets.base import Dataset
from distributed_notebook.models.model import DeepLearningModel
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
    def __init__(self, name: str = "", **kwargs):
        super().__init__(**kwargs)

        self._name: str = name

    @property
    def name(self)->str:
        return self._name

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
    def __init__(self, dataset: Dataset = None, dataset_path:str = "", **kwargs):
        super().__init__(name = dataset.name, key = dataset_path, **kwargs)

        self._dataset: Optional[Dataset] = dataset

    def __getstate__(self):
        d = dict(self.__dict__)

        if self._dataset is not None:
            d['dataset_description'] = self._dataset.description

        del d['_dataset']

        return d

    @property
    def dataset(self)->Optional[Dataset]:
        return self._dataset

    @property
    def dataset_description(self)->Optional[dict[str, str|int|bool]]:
        if self._dataset is not None:
            return self._dataset.description

        return None

class ModelPointer(SyncPointer):
    """
    ModelPointer is a SyncPointer for models/model parameters.
    """
    def __init__(self, deep_learning_model: DeepLearningModel = None, model_path:str = "", **kwargs):
        super().__init__(name = deep_learning_model.name, key = model_path, **kwargs)

        self._model: DeepLearningModel = deep_learning_model
        self._out_features: int = deep_learning_model.out_features
        self._total_training_time_seconds: int = deep_learning_model.total_training_time_seconds
        self._total_num_epochs: int = deep_learning_model.total_num_epochs

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['_model']
        return d

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
    def model(self)->Optional[DeepLearningModel]:
        return self._model