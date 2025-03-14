import os
import uuid
from typing import Any, Type

import pytest
from torch import Tensor

from distributed_notebook.deep_learning import ResNet18, VGG11, VGG13, VGG16, VGG19, InceptionV3, \
    ComputerVisionModel, Bert, IMDbLargeMovieReviewTruncated, GPT2, LibriSpeech, CIFAR10, DeepSpeech2, TinyImageNet, \
    CoLA, get_model_and_dataset
from distributed_notebook.deep_learning.data import ComputerVision, NaturalLanguageProcessing, Speech
from distributed_notebook.deep_learning.data.custom_dataset import CustomDataset
from distributed_notebook.deep_learning.data.random import RandomCustomDataset
from distributed_notebook.deep_learning.models.loader import load_model
from distributed_notebook.deep_learning.models.model import DeepLearningModel
from distributed_notebook.deep_learning.models.simple_model import SimpleModel, SimpleModule
from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer
from distributed_notebook.sync.checkpointing.pointer import ModelPointer
from distributed_notebook.sync.remote_storage.error import InvalidKeyError
from distributed_notebook.sync.remote_storage.local_provider import LocalStorageProvider


def test_create():
    local_provider: LocalStorageProvider = LocalStorageProvider()
    checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)

    assert checkpointer is not None
    assert isinstance(checkpointer, RemoteCheckpointer)


def test_read_after_write():
    local_provider: LocalStorageProvider = LocalStorageProvider()
    checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)

    model: SimpleModel = SimpleModel(input_size=2, out_features=4, created_for_first_time=True)
    model_pointer: ModelPointer = ModelPointer(
        deep_learning_model=model,
        user_namespace_variable_name="model",
        model_path=os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id=1,
    )

    checkpointer.write_state_dicts(model_pointer)

    # The size will now be three -- as we wrote the model state, the state of the model's optimizer, and
    # the state of the model's criterion.
    model_state, optimizer_state, criterion_state, constructor_args_state = checkpointer.read_state_dicts(model_pointer)

    assert model_state is not None
    assert optimizer_state is not None
    assert criterion_state is not None
    assert constructor_args_state is not None

    assert isinstance(model_state, dict)
    assert isinstance(optimizer_state, dict)
    assert isinstance(criterion_state, dict)
    assert isinstance(constructor_args_state, dict)


def test_write_model_that_does_not_require_checkpointing():
    """
    Write a model that does NOT require checkpointing, which should cause a ValueError to be raised.
    """
    local_provider: LocalStorageProvider = LocalStorageProvider()
    checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)

    model: SimpleModel = SimpleModel(input_size=2, out_features=4, created_for_first_time=False)
    model_pointer: ModelPointer = ModelPointer(
        deep_learning_model=model,
        user_namespace_variable_name="model",
        model_path=os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id=1,
    )

    with pytest.raises(ValueError):
        checkpointer.write_state_dicts(model_pointer)


def test_read_empty():
    local_provider: LocalStorageProvider = LocalStorageProvider()
    checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)

    model: SimpleModel = SimpleModel(input_size=2, out_features=4, created_for_first_time=True)
    model_pointer: ModelPointer = ModelPointer(
        deep_learning_model=model,
        user_namespace_variable_name="model",
        model_path=os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id=1,
    )

    with pytest.raises(InvalidKeyError):
        checkpointer.read_state_dicts(model_pointer)


def test_checkpoint_after_training():
    local_provider: LocalStorageProvider = LocalStorageProvider()
    checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)

    # Create the model.
    input_size: int = 4
    model: SimpleModel = SimpleModel(input_size=input_size, out_features=1, created_for_first_time=True)
    initial_weights = model.model.fc.weight.clone()

    # Create the dataset.
    dataset: RandomCustomDataset = RandomCustomDataset(
        input_size,
        num_training_samples=64,
        num_test_samples=16,
        batch_size=8
    )

    # Checkpoint the initial model weights.
    model_pointer: ModelPointer = ModelPointer(
        deep_learning_model=model,
        user_namespace_variable_name="model",
        model_path=os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id=1,
    )
    checkpointer.write_state_dicts(model_pointer)

    num_epochs: int = 3
    num_training_loops: int = 3
    previous_weights: Tensor = initial_weights
    for i in range(0, num_training_loops):
        # Train for a while.
        model.train_epochs(dataset.train_loader, num_epochs)

        # Establish that the model's weights have changed.
        updated_weights = model.model.fc.weight
        assert previous_weights.equal(updated_weights) == False
        previous_weights = updated_weights.clone()

        # Before re-writing the updated model's weights, verify that the weights in remote storage
        # match the initial weights and no longer match the model's weights.
        old_model_state, old_optimizer_state, old_criterion_state, old_constructor_args_state = checkpointer.read_state_dicts(
            model_pointer)

        assert old_model_state is not None
        assert old_optimizer_state is not None
        assert old_criterion_state is not None
        assert old_constructor_args_state is not None
        assert isinstance(old_model_state, dict)
        assert isinstance(old_optimizer_state, dict)
        assert isinstance(old_criterion_state, dict)
        assert isinstance(old_constructor_args_state, dict)

        current_model_state: dict[str, Any] = model.state_dict

        for old_val, new_val in zip(old_model_state.values(), current_model_state.values()):
            if isinstance(old_val, Tensor) and isinstance(new_val, Tensor):
                assert old_val.equal(new_val) == False

        # Write the updated model state to remote storage.
        model_pointer = ModelPointer(
            deep_learning_model=model,
            user_namespace_variable_name="model",
            model_path=os.path.join(f"store/{str(uuid.uuid4())}", model.name),
            proposer_id=1,
        )
        checkpointer.write_state_dicts(model_pointer)

        # Verify that the weights in remote storage match the updated weights.
        remote_model_state, remote_optimizer_state, remote_criterion_state, remote_constructor_state = checkpointer.read_state_dicts(
            model_pointer)
        local_model_state: dict[str, Any] = model.state_dict
        for remote_val, local_val in zip(remote_model_state.values(), local_model_state.values()):
            if isinstance(remote_val, Tensor) and isinstance(local_val, Tensor):
                assert remote_val.equal(local_val)

        # Load a new instance of the model using the state checkpointed in remote storage.
        checkpointed_model: DeepLearningModel = load_model(
            model_name=model_pointer.large_object_name,
            existing_model=None,
            out_features=model_pointer.out_features,
            model_state_dict=remote_model_state,
            optimizer_state_dict=remote_optimizer_state,
            criterion_state_dict=remote_criterion_state,
            input_size=model_pointer.input_size,
        )

        assert checkpointed_model is not None
        assert isinstance(checkpointed_model, SimpleModel)
        assert checkpointed_model.model is not None
        assert isinstance(checkpointed_model.model, SimpleModule)

        # Compare the state of the model loaded from remote storage with the original, local model.
        local_model_state: dict[str, Any] = model.state_dict
        checkpointed_model_state: dict[str, Any] = checkpointed_model.state_dict

        for checkpointed_val, local_val in zip(checkpointed_model_state.values(), local_model_state.values()):
            if isinstance(checkpointed_val, Tensor) and isinstance(local_val, Tensor):
                assert checkpointed_val.equal(local_val)


def test_checkpoint_and_train_simple_model():
    local_provider: LocalStorageProvider = LocalStorageProvider()
    checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)

    # Create the model.
    input_size: int = 4
    model: SimpleModel = SimpleModel(input_size=input_size, out_features=1, created_for_first_time=True)
    initial_weights = model.model.fc.weight.clone()

    # Create the dataset.
    dataset: RandomCustomDataset = RandomCustomDataset(
        input_size,
        num_training_samples=64,
        num_test_samples=16,
        batch_size=8
    )

    # Checkpoint the initial model weights.
    model_pointer: ModelPointer = ModelPointer(
        deep_learning_model=model,
        user_namespace_variable_name="model",
        model_path=os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id=1,
    )
    checkpointer.write_state_dicts(model_pointer)

    num_epochs: int = 3
    num_training_loops: int = 3
    previous_weights: Tensor = initial_weights
    for i in range(0, num_training_loops):
        # Train for a while.
        model.train_epochs(dataset.train_loader, num_epochs)

        # Establish that the model's weights have changed.
        updated_weights = model.model.fc.weight
        assert previous_weights.equal(updated_weights) == False
        previous_weights = updated_weights.clone()

        # Before re-writing the updated model's weights, verify that the weights in remote storage
        # match the initial weights and no longer match the model's weights.
        old_model_state, old_optimizer_state, old_criterion_state, old_constructor_state = checkpointer.read_state_dicts(
            model_pointer)

        assert old_model_state is not None
        assert old_optimizer_state is not None
        assert old_criterion_state is not None
        assert old_constructor_state is not None
        assert isinstance(old_model_state, dict)
        assert isinstance(old_optimizer_state, dict)
        assert isinstance(old_criterion_state, dict)
        assert isinstance(old_constructor_state, dict)

        current_model_state: dict[str, Any] = model.state_dict

        for old_val, new_val in zip(old_model_state.values(), current_model_state.values()):
            if isinstance(old_val, Tensor) and isinstance(new_val, Tensor):
                assert old_val.equal(new_val) == False

        # Write the updated model state to remote storage.
        model_pointer = ModelPointer(
            deep_learning_model=model,
            user_namespace_variable_name="model",
            model_path=os.path.join(f"store/{str(uuid.uuid4())}", model.name),
            proposer_id=1,
        )
        checkpointer.write_state_dicts(model_pointer)

        # Verify that the weights in remote storage match the updated weights.
        remote_model_state, remote_optimizer_state, remote_criterion_state, remote_constructor_state = checkpointer.read_state_dicts(
            model_pointer)
        local_model_state: dict[str, Any] = model.state_dict
        for remote_val, local_val in zip(remote_model_state.values(), local_model_state.values()):
            if isinstance(remote_val, Tensor) and isinstance(local_val, Tensor):
                assert remote_val.equal(local_val)

        # Load a new instance of the model using the state checkpointed in remote storage.
        checkpointed_model: DeepLearningModel = load_model(
            model_name=model_pointer.large_object_name,
            existing_model=None,
            out_features=model_pointer.out_features,
            model_state_dict=remote_model_state,
            optimizer_state_dict=remote_optimizer_state,
            criterion_state_dict=remote_criterion_state,
            input_size=model_pointer.input_size,
        )

        assert checkpointed_model is not None
        assert isinstance(checkpointed_model, SimpleModel)
        assert checkpointed_model.model is not None
        assert isinstance(checkpointed_model.model, SimpleModule)

        # Compare the state of the model loaded from remote storage with the original, local model.
        local_model_state: dict[str, Any] = model.state_dict
        checkpointed_model_state: dict[str, Any] = checkpointed_model.state_dict

        for checkpointed_val, local_val in zip(checkpointed_model_state.values(), local_model_state.values()):
            if isinstance(checkpointed_val, Tensor) and isinstance(local_val, Tensor):
                assert checkpointed_val.equal(local_val)

        model = checkpointed_model


def perform_training_for_model(
        model_class: Type,
        dataset_class: Type,
        num_training_loops: int = 5,
        target_training_duration_ms: float = 1000.0
):
    """
    Perform deep learning training on a model of type 'cls', where 'cls' is some subtype of ComputerVisionModel.
    """
    assert issubclass(model_class, DeepLearningModel)
    assert issubclass(dataset_class, CustomDataset)

    local_provider: LocalStorageProvider = LocalStorageProvider()
    checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)

    # # Create the model.
    # model = model_class(created_for_first_time=True)
    # initial_weights = model.output_layer.weight.clone()
    #
    # # Create the dataset.
    # if dataset_class.category() == ComputerVision:
    #     assert issubclass(model_class, ComputerVisionModel)
    #     dataset = dataset_class(image_size=model_class.expected_image_size())
    # elif dataset_class.category() == NaturalLanguageProcessing:
    #     assert issubclass(model_class, Bert) or issubclass(model_class, GPT2)
    #     dataset = dataset_class(model_name = model_class.model_name())
    # else:
    #     assert dataset_class.category() == Speech
    #     dataset = dataset_class(train_split = LibriSpeech.test_clean, test_split = LibriSpeech.test_other)

    model, dataset = get_model_and_dataset(model_class.model_name(), dataset_class.dataset_name())
    assert isinstance(model, model_class)
    assert isinstance(dataset, dataset_class)
    initial_weights: Tensor = model.output_layer.weight.clone()

    # Checkpoint the initial model weights.
    model_pointer: ModelPointer = ModelPointer(
        deep_learning_model=model,
        user_namespace_variable_name="model",
        model_path=os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id=1,
    )
    checkpointer.write_state_dicts(model_pointer)

    previous_weights: Tensor = initial_weights
    for i in range(0, num_training_loops):
        # Train for a while.
        model.train(dataset.train_loader, target_training_duration_ms)

        # Establish that the model's weights have changed.
        updated_weights = model.output_layer.weight
        assert previous_weights.equal(updated_weights) == False
        previous_weights = updated_weights.clone()

        # Before re-writing the updated model's weights, verify that the weights in remote storage
        # match the initial weights and no longer match the model's weights.
        old_model_state, old_optimizer_state, old_criterion_state, old_constructor_state = checkpointer.read_state_dicts(
            model_pointer)

        assert old_model_state is not None
        assert old_optimizer_state is not None
        assert old_criterion_state is not None
        assert old_constructor_state is not None
        assert isinstance(old_model_state, dict)
        assert isinstance(old_optimizer_state, dict)
        assert isinstance(old_criterion_state, dict)
        assert isinstance(old_constructor_state, dict)

        current_model_state: dict[str, Any] = model.state_dict

        for old_val, new_val in zip(old_model_state.values(), current_model_state.values()):
            if isinstance(old_val, Tensor) and isinstance(new_val, Tensor):
                assert old_val.equal(new_val) == False

        # Write the updated model state to remote storage.
        model_pointer = ModelPointer(
            deep_learning_model=model,
            user_namespace_variable_name="model",
            model_path=os.path.join(f"store/{str(uuid.uuid4())}", model.name),
            proposer_id=1,
        )
        checkpointer.write_state_dicts(model_pointer)

        # Verify that the weights in remote storage match the updated weights.
        remote_model_state, remote_optimizer_state, remote_criterion_state, remote_constructor_state = checkpointer.read_state_dicts(
            model_pointer)
        local_model_state: dict[str, Any] = model.state_dict
        for remote_val, local_val in zip(remote_model_state.values(), local_model_state.values()):
            if isinstance(remote_val, Tensor) and isinstance(local_val, Tensor):
                assert remote_val.equal(local_val)

        # Load a new instance of the model using the state checkpointed in remote storage.
        checkpointed_model = load_model(
            model_name=model_pointer.large_object_name,
            existing_model=None,
            out_features=model_pointer.out_features,
            model_state_dict=remote_model_state,
            optimizer_state_dict=remote_optimizer_state,
            criterion_state_dict=remote_criterion_state,
        )

        assert checkpointed_model is not None
        assert isinstance(checkpointed_model, model_class)
        assert checkpointed_model.model is not None

        if checkpointed_model.expected_model_class() is not None:
            assert isinstance(checkpointed_model.model, checkpointed_model.expected_model_class())

        # Compare the state of the model loaded from remote storage with the original, local model.
        local_model_state: dict[str, Any] = model.state_dict
        checkpointed_model_state: dict[str, Any] = checkpointed_model.state_dict

        for checkpointed_val, local_val in zip(checkpointed_model_state.values(), local_model_state.values()):
            if isinstance(checkpointed_val, Tensor) and isinstance(local_val, Tensor):
                assert checkpointed_val.equal(local_val)

        model = checkpointed_model


@pytest.mark.parametrize("model_class,dataset_class", [
    (ResNet18, CIFAR10), (ResNet18, TinyImageNet),
    # (InceptionV3, CIFAR10), (InceptionV3, TinyImageNet),
    # (VGG11, CIFAR10), (VGG11, TinyImageNet),
    # (VGG13, CIFAR10), (VGG13, TinyImageNet),
    # (VGG16, CIFAR10), (VGG16, TinyImageNet),
    # (VGG19, CIFAR10), (VGG19, TinyImageNet),
    # (Bert, IMDbLargeMovieReviewTruncated), (Bert, CoLA),
    # (GPT2, IMDbLargeMovieReviewTruncated), (GPT2, CoLA),
    # (DeepSpeech2, LibriSpeech)
])
def test_perform_training_for_model(model_class: Type[DeepLearningModel], dataset_class: Type[CustomDataset]):
    perform_training_for_model(model_class, dataset_class, target_training_duration_ms=2000.0, num_training_loops=3)