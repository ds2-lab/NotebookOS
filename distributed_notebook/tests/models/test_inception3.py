import torch
import pytest

from distributed_notebook.deep_learning import CIFAR10, TinyImageNet, InceptionV3

def test_train_inception_v3_on_cifar10_cpu():
    """
    Train the Inception v3 model on the CIFAR-10 dataset using the CPU.
    Validate that the weights are updated correctly.
    """
    dataset: CIFAR10 = CIFAR10(image_size=InceptionV3.expected_image_size())
    model: InceptionV3 = InceptionV3(created_for_first_time=True)

    training_duration_ms = 3250

    # Set this to False in order to force CPU training.
    model.gpu_available = False

    # Access the classification head (last layer)
    output_layer = model.output_layer

    # Extract weights and biases
    prev_weights = output_layer.weight.detach().cpu()
    for _ in range(0, 3):
        print(f"Initial weights: {prev_weights}")
        model.train(dataset.train_loader, training_duration_ms)

        updated_weights = output_layer.weight.detach().cpu()
        print(f"Updated weights: {updated_weights}")

        if prev_weights.equal(updated_weights):
            print(f"Initial weights: {prev_weights}")

        assert not prev_weights.equal(updated_weights)
        prev_weights = updated_weights

if __name__ == "__main__":
    test_train_inception_v3_on_cifar10_cpu()

def test_train_inception_v3_on_cifar10():
    """
    Train the Inception v3 model on the CIFAR-10 dataset. Validate that the weights are updated correctly.
    """
    dataset: CIFAR10 = CIFAR10(image_size = InceptionV3.expected_image_size())
    model: InceptionV3 = InceptionV3(created_for_first_time=True)

    training_duration_ms: int = 2000
    if not torch.cuda.is_available():
        training_duration_ms = 3250

    # Access the classification head (last layer)
    output_layer = model.output_layer

    # Extract weights and biases
    prev_weights = output_layer.weight.detach().cpu()
    for _ in range(0, 3):
        print(f"Initial weights: {prev_weights}")
        model.train(dataset.train_loader, training_duration_ms)

        updated_weights = output_layer.weight.detach().cpu()
        print(f"Updated weights: {updated_weights}")

        assert prev_weights.equal(updated_weights) == False
        prev_weights = updated_weights


def test_train_inception_v3_on_tiny_imagenet():
    """
    Train the Inception v3 model on the Tiny ImageNet dataset. Validate that the weights are updated correctly.
    """
    dataset: TinyImageNet = TinyImageNet(image_size = InceptionV3.expected_image_size())
    model: InceptionV3 = InceptionV3(created_for_first_time=True, out_features = 200)

    training_duration_ms: int = 2000
    if not torch.cuda.is_available():
        training_duration_ms = 3250

    # Access the classification head (last layer)
    output_layer = model.output_layer

    # Extract weights and biases
    prev_weights = output_layer.weight.detach().cpu()
    for _ in range(0, 3):
        print(f"Initial weights: {prev_weights}")
        model.train(dataset.train_loader, training_duration_ms)

        updated_weights = output_layer.weight.detach().cpu()
        print(f"Updated weights: {updated_weights}")

        assert prev_weights.equal(updated_weights) == False
        prev_weights = updated_weights


@pytest.mark.skipif(torch.cuda.device_count() <= 1, reason="requires >= 2 torch.cuda.devices (i.e., 2+ GPUs)")
def test_train_inception_v3_on_cifar10_multi_gpu():
    """
    Train the ResNet-18 model on the CIFAR-10 dataset. Validate that the weights are updated correctly.

    This version uses multiple GPUs.
    """
    dataset: CIFAR10 = CIFAR10(image_size=InceptionV3.expected_image_size())
    model: InceptionV3 = InceptionV3(created_for_first_time=True, out_features=200)

    training_duration_ms: int = 2000
    if not torch.cuda.is_available():
        training_duration_ms = 3250

    model.set_gpu_device_ids([0, 1])

    # Access the classification head (last layer)
    output_layer = model.output_layer

    # Extract weights and biases
    prev_weights = output_layer.weight.detach().cpu()
    for _ in range(0, 3):
        print(f"Initial weights: {prev_weights}")
        model.train(dataset.train_loader, training_duration_ms)

        updated_weights = output_layer.weight.detach().cpu()
        print(f"Updated weights: {updated_weights}")

        assert not prev_weights.equal(updated_weights)
        prev_weights = updated_weights