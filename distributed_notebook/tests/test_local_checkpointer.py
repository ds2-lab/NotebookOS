import os
import uuid

import pytest

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