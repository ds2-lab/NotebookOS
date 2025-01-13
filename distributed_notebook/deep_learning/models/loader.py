from typing import Optional, Dict, Any, Type

from distributed_notebook.deep_learning import VGG16, InceptionV3, DeepSpeech2, ResNet18, SimpleModel, DeepLearningModel, Bert, GPT2


def load_model(
        model_name:str = None,
        existing_model: Optional[DeepLearningModel] = None,
        out_features: int = 10,
        total_training_time_seconds: int = 0,
        total_num_epochs: int = 0,
        model_state_dict: Optional[Dict[str, Any]] = None,
        optimizer_state_dict: Optional[Dict[str, Any]] = None,
        criterion_state_dict: Optional[Dict[str, Any]] = None,
        **kwargs,
)->DeepLearningModel:
    if existing_model is not None and existing_model.name == model_name:
        existing_model.apply_model_state_dict(model_state_dict)
        existing_model.apply_optimizer_state_dict(optimizer_state_dict)
        existing_model.apply_criterion_state_dict(criterion_state_dict)
        existing_model.total_training_time_seconds = total_training_time_seconds
        existing_model.total_num_epochs = total_num_epochs

        return existing_model

    if model_name == ResNet18.model_name():
        cls = ResNet18
    elif model_name == VGG16.model_name():
        cls = VGG16
    elif model_name == InceptionV3.model_name():
        cls = InceptionV3
    elif model_name == GPT2.model_name():
        cls = GPT2
    elif model_name == Bert.model_name():
        cls = Bert
    elif model_name == DeepSpeech2.model_name():
        cls = DeepSpeech2
    elif model_name == SimpleModel.model_name():
        cls = SimpleModel
    else:
        raise ValueError(f"unknown or unsupported deep learning model \"{model_name}\"")

    assert issubclass(cls, DeepLearningModel)
    return cls(
        out_features = out_features,
        total_training_time_seconds = total_training_time_seconds,
        total_num_epochs = total_num_epochs,
        model_state_dict = model_state_dict,
        optimizer_state_dict = optimizer_state_dict,
        criterion_state_dict = criterion_state_dict,
        **kwargs,
    )