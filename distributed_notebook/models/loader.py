from typing import Optional, Dict, Any

from distributed_notebook.models.model import DeepLearningModel
from distributed_notebook.models.resnet18 import ResNet18Name, ResNet18


def load_model(
        model_name:str = None,
        existing_model: Optional[DeepLearningModel] = None,
        out_features: int = 10,
        total_training_time_seconds: int = 0,
        total_num_epochs: int = 0,
        model_state_dict: Optional[Dict[str, Any]] = None,
        optimizer_state_dict: Optional[Dict[str, Any]] = None,
        criterion_state_dict: Optional[Dict[str, Any]] = None,
)->DeepLearningModel:
    if existing_model is not None and existing_model.name == model_name:
        existing_model.apply_model_state_dict(model_state_dict)
        existing_model.apply_optimizer_state_dict(optimizer_state_dict)
        existing_model.apply_criterion_state_dict(criterion_state_dict)
        existing_model.total_training_time_seconds = total_training_time_seconds
        existing_model.total_num_epochs = total_num_epochs

        return existing_model

    if model_name == ResNet18Name:
        return ResNet18(
            out_features = out_features,
            total_training_time_seconds = total_training_time_seconds,
            total_num_epochs = total_num_epochs,
            model_state_dict = model_state_dict,
            optimizer_state_dict = optimizer_state_dict,
            criterion_state_dict = criterion_state_dict,
        )

    raise ValueError(f"unknown or unsupported deep learning model \"{model_name}\"")