from typing import Optional, Dict, Any, Type

from distributed_notebook.deep_learning import DeepLearningModel, SimpleModel, \
    VGG16, VGG19, ResNet18, InceptionV3, Bert, GPT2, DeepSpeech2
from distributed_notebook.deep_learning.models import ALL_MODEL_CLASSES


def load_model(
        model_name: str = None,
        existing_model: Optional[DeepLearningModel] = None,
        out_features: int = 10,
        total_training_time_seconds: int = 0,
        total_num_epochs: int = 0,
        model_state_dict: Optional[Dict[str, Any]] = None,
        optimizer_state_dict: Optional[Dict[str, Any]] = None,
        criterion_state_dict: Optional[Dict[str, Any]] = None,
        **kwargs,
) -> DeepLearningModel:
    if existing_model is not None and existing_model.name == model_name:
        existing_model.apply_model_state_dict(model_state_dict)
        existing_model.apply_optimizer_state_dict(optimizer_state_dict)
        existing_model.apply_criterion_state_dict(criterion_state_dict)
        existing_model.total_training_time_seconds = total_training_time_seconds
        existing_model.total_num_epochs = total_num_epochs

        return existing_model

    cls: Optional[Type] = None
    for model_class in ALL_MODEL_CLASSES:
        assert issubclass(model_class, DeepLearningModel)

        if model_name == model_class.model_name():
            cls = model_class
            break

    if cls is None:
        raise ValueError(f"unknown or unsupported deep learning model \"{model_name}\"")

    assert issubclass(cls, DeepLearningModel)
    return cls(
        out_features=out_features,
        total_training_time_seconds=total_training_time_seconds,
        total_num_epochs=total_num_epochs,
        model_state_dict=model_state_dict,
        optimizer_state_dict=optimizer_state_dict,
        criterion_state_dict=criterion_state_dict,
        **kwargs,
    )
