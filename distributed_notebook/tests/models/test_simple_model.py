from torch import Size
from torch.nn.parameter import Parameter

from distributed_notebook.deep_learning.datasets.random import RandomCustomDataset
from distributed_notebook.deep_learning.models.simple_model import SimpleModel, SimpleModule

def test_instantiate():
    input_size: int = 3
    out_features: int = 5
    model: SimpleModel = SimpleModel(
        input_size = input_size,
        out_features = out_features
    )

    assert model is not None
    assert isinstance(model, SimpleModel)

    simple_module: SimpleModule = model.model
    assert simple_module is not None
    assert isinstance(simple_module, SimpleModule)
    assert simple_module.fc.num_features == input_size
    assert simple_module.fc.out_features == out_features

    weight: Parameter = simple_module.fc.weight
    weight_size: Size = weight.size()
    assert weight_size is not None
    assert isinstance(weight_size, Size)
    assert weight_size.numel() == input_size * out_features
    assert weight_size[1] == input_size
    assert weight_size[0] == out_features

def test_instantiate_with_initial_values():
    input_size: int = 3
    out_features: int = 5

    initial_weights: float = 100.0
    initial_bias: float = 0.100
    model: SimpleModel = SimpleModel(
        input_size = input_size,
        out_features = out_features,
        initial_weights = initial_weights,
        initial_bias = initial_bias,
    )
    simple_module: SimpleModule = model.model
    weight: Parameter = simple_module.fc.weight
    for w in weight.data[0]:
        assert w == initial_weights

    for w in weight.data[1]:
        assert w == initial_weights

    for w in simple_module.fc.bias.data:
        assert w == initial_bias

    weight_size: Size = weight.size()
    assert weight_size is not None
    assert isinstance(weight_size, Size)
    assert weight_size.numel() == input_size * out_features
    assert weight_size[1] == input_size
    assert weight_size[0] == out_features

def test_set_weights():
    input_size: int = 3
    out_features: int = 5

    initial_weights: float = 100.0
    model: SimpleModel = SimpleModel(
        input_size = input_size,
        out_features = out_features,
        initial_weights = initial_weights,
    )
    simple_module: SimpleModule = model.model
    weight: Parameter = simple_module.fc.weight
    for w in weight.data[0]:
        assert w == initial_weights

    for w in weight.data[1]:
        assert w == initial_weights

    updated_weight_val: float = 500.525
    model.set_weights(updated_weight_val)

    weight: Parameter = simple_module.fc.weight
    for w in weight.data[0]:
        assert w == updated_weight_val

    for w in weight.data[1]:
        assert w == updated_weight_val

def test_set_bias():
    input_size: int = 3
    out_features: int = 5

    initial_bias: float = 0.100
    model: SimpleModel = SimpleModel(
        input_size = input_size,
        out_features = out_features,
        initial_bias = initial_bias,
    )
    simple_module: SimpleModule = model.model
    for b in simple_module.fc.bias.data:
        assert b == initial_bias

    updated_bias_val: float = 123.456
    model.set_bias(updated_bias_val)
    for b in simple_module.fc.bias.data:
        assert b == updated_bias_val

def test_requires_checkpointing_after_training():
    input_size: int = 3
    out_features: int = 1
    model: SimpleModel = SimpleModel(
        input_size = input_size,
        out_features = out_features,
        created_for_first_time = False # So that _requires_checkpointing is initially false
    )

    assert model.requires_checkpointing == False

    dataset: RandomCustomDataset = RandomCustomDataset(
        input_size,
        num_training_samples = 64,
        num_test_samples = 16,
        batch_size = 8
    )

    model.train_epochs(dataset.train_loader, 1)

    assert model.requires_checkpointing == True

def test_training_for_time_updates_weights():
    input_size: int = 3
    out_features: int = 1
    model: SimpleModel = SimpleModel(
        input_size = input_size,
        out_features = out_features,
        created_for_first_time = True
    )

    initial_weights = model.model.fc.weight.clone()

    dataset: RandomCustomDataset = RandomCustomDataset(
        input_size,
        num_training_samples = 64,
        num_test_samples = 16,
        batch_size = 8
    )

    model.train(dataset.train_loader, 500)

    updated_weights = model.model.fc.weight

    assert initial_weights.equal(updated_weights) == False

def test_training_for_epochs_updates_weights():
    input_size: int = 3
    out_features: int = 1
    model: SimpleModel = SimpleModel(
        input_size = input_size,
        out_features = out_features,
        created_for_first_time = True
    )

    initial_weights = model.model.fc.weight.clone()

    dataset: RandomCustomDataset = RandomCustomDataset(
        input_size,
        num_training_samples = 64,
        num_test_samples = 16,
        batch_size = 8
    )

    model.train_epochs(dataset.train_loader, 1)

    updated_weights = model.model.fc.weight

    assert initial_weights.equal(updated_weights) == False