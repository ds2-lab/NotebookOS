import os
import uuid
from typing import Any

import pytest
from torch import Tensor

from distributed_notebook.datasets.random import RandomCustomDataset
from distributed_notebook.models.simple_model import SimpleModel
from distributed_notebook.sync.checkpointing.local_checkpointer import LocalCheckpointer
from distributed_notebook.sync.checkpointing.pointer import ModelPointer


def test_create():
    checkpointer: LocalCheckpointer = LocalCheckpointer()

    assert checkpointer is not None
    assert isinstance(checkpointer, LocalCheckpointer)
    assert len(checkpointer) == 0
    assert checkpointer.size == 0

def test_read_after_write():
    checkpointer: LocalCheckpointer = LocalCheckpointer()

    model: SimpleModel = SimpleModel(input_size = 2, out_features = 4, created_for_first_time = True)
    model_pointer: ModelPointer = ModelPointer(
        deep_learning_model = model,
        user_namespace_variable_name = "model",
        model_path = os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id = 1,
    )

    checkpointer.write_state_dicts(model_pointer)

    # The size will now be three -- as we wrote the model state, the state of the model's optimizer, and
    # the state of the model's criterion.
    assert checkpointer.size == 3
    assert len(checkpointer) == 3

    model_state, optimizer_state, criterion_state = checkpointer.read_state_dicts(model_pointer)

    assert model_state is not None
    assert optimizer_state is not None
    assert criterion_state is not None

    assert isinstance(model_state, dict)
    assert isinstance(optimizer_state, dict)
    assert isinstance(criterion_state, dict)

def test_write_model_that_does_not_require_checkpointing():
    """
    Write a model that does NOT require checkpointing, which should cause a ValueError to be raised.
    """
    checkpointer: LocalCheckpointer = LocalCheckpointer()

    model: SimpleModel = SimpleModel(input_size = 2, out_features = 4, created_for_first_time = False)
    model_pointer: ModelPointer = ModelPointer(
        deep_learning_model = model,
        user_namespace_variable_name = "model",
        model_path = os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id = 1,
    )

    with pytest.raises(ValueError):
        checkpointer.write_state_dicts(model_pointer)

def test_read_empty():
    checkpointer: LocalCheckpointer = LocalCheckpointer()

    model: SimpleModel = SimpleModel(input_size = 2, out_features = 4, created_for_first_time = True)
    model_pointer: ModelPointer = ModelPointer(
        deep_learning_model = model,
        user_namespace_variable_name = "model",
        model_path = os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id = 1,
    )

    with pytest.raises(ValueError):
        checkpointer.read_state_dicts(model_pointer)

def test_checkpoint_after_training():
    checkpointer: LocalCheckpointer = LocalCheckpointer()

    # Create the model.
    input_size: int = 4
    model: SimpleModel = SimpleModel(input_size = input_size, out_features = 1, created_for_first_time = True)
    initial_weights = model.model.fc.weight.clone()

    # Create the dataset.
    dataset: RandomCustomDataset = RandomCustomDataset(
        input_size,
        num_training_samples = 64,
        num_test_samples = 16,
        batch_size = 8
    )

    # Checkpoint the initial model weights.
    model_pointer: ModelPointer = ModelPointer(
        deep_learning_model = model,
        user_namespace_variable_name = "model",
        model_path = os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id = 1,
    )
    checkpointer.write_state_dicts(model_pointer)

    # Train for a while.
    model.train_epochs(dataset.train_dataset, 6)

    # Establish that the model's weights have changed.
    updated_weights = model.model.fc.weight
    assert initial_weights.equal(updated_weights) == False

    # Before re-writing the updated model's weights, verify that the weights in remote storage
    # match the initial weights and no longer match the model's weights.
    old_model_state, old_optimizer_state, old_criterion_state = checkpointer.read_state_dicts(model_pointer)

    assert old_model_state is not None
    assert old_optimizer_state is not None
    assert old_criterion_state is not None
    assert isinstance(old_model_state, dict)
    assert isinstance(old_optimizer_state, dict)
    assert isinstance(old_criterion_state, dict)

    current_model_state: dict[str, Any] = model.state_dict

    for old_val, new_val in zip(old_model_state.values(), current_model_state.values()):
        if isinstance(old_val, Tensor) and isinstance(new_val, Tensor):
            assert old_val.equal(new_val) == False

    # Write the updated model state to remote storage.
    model_pointer = ModelPointer(
        deep_learning_model = model,
        user_namespace_variable_name = "model",
        model_path = os.path.join(f"store/{str(uuid.uuid4())}", model.name),
        proposer_id = 1,
    )
    checkpointer.write_state_dicts(model_pointer)

    # Verify that the weights in remote storage match the updated weights.
    remote_model_state, _, _ = checkpointer.read_state_dicts(model_pointer)
    local_model_state: dict[str, Any] = model.state_dict
    for remote_val, local_val in zip(remote_model_state.values(), local_model_state.values()):
        if isinstance(remote_val, Tensor) and isinstance(local_val, Tensor):
            assert remote_val.equal(local_val)