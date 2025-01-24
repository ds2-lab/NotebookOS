from distributed_notebook.deep_learning import CIFAR10, TinyImageNet, VGG11, VGG13, VGG16, VGG19

import torch

def test_train_vgg11_on_cifar10():
    """
    Train the VGG-11 model on the CIFAR-10 dataset. Validate that the weights are updated correctly.
    """
    dataset: CIFAR10 = CIFAR10(image_size = VGG11.expected_image_size())
    model: VGG11 = VGG11(created_for_first_time=True)

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


def test_train_vgg11_on_tiny_imagenet():
    """
    Train the VGG-11 model on the Tiny ImageNet dataset. Validate that the weights are updated correctly.
    """
    dataset: TinyImageNet = TinyImageNet(image_size = VGG11.expected_image_size())
    model: VGG11 = VGG11(created_for_first_time=True, out_features = 200)

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


def test_train_vgg13_on_cifar10():
    """
    Train the VGG-13 model on the CIFAR-10 dataset. Validate that the weights are updated correctly.
    """
    dataset: CIFAR10 = CIFAR10(image_size = VGG13.expected_image_size())
    model: VGG13 = VGG13(created_for_first_time=True)

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


def test_train_vgg13_on_tiny_imagenet():
    """
    Train the VGG-13 model on the Tiny ImageNet dataset. Validate that the weights are updated correctly.
    """
    dataset: TinyImageNet = TinyImageNet(image_size = VGG13.expected_image_size())
    model: VGG13 = VGG13(created_for_first_time=True, out_features = 200)

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

def test_train_vgg16_on_cifar10():
    """
    Train the VGG-16 model on the CIFAR-10 dataset. Validate that the weights are updated correctly.
    """
    dataset: CIFAR10 = CIFAR10(image_size = VGG16.expected_image_size())
    model: VGG16 = VGG16(created_for_first_time=True)

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


def test_train_vgg16_on_tiny_imagenet():
    """
    Train the VGG-16 model on the Tiny ImageNet dataset. Validate that the weights are updated correctly.
    """
    dataset: TinyImageNet = TinyImageNet(image_size = VGG16.expected_image_size())
    model: VGG16 = VGG16(created_for_first_time=True, out_features = 200)

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


def test_train_vgg19_on_cifar10():
    """
    Train the VGG-19 model on the CIFAR-10 dataset. Validate that the weights are updated correctly.
    """
    dataset: CIFAR10 = CIFAR10(image_size = VGG19.expected_image_size())
    model: VGG19 = VGG19(created_for_first_time=True)

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


def test_train_vgg19_on_tiny_imagenet():
    """
    Train the VGG-19 model on the Tiny ImageNet dataset. Validate that the weights are updated correctly.
    """
    dataset: TinyImageNet = TinyImageNet(image_size = VGG19.expected_image_size())
    model: VGG19 = VGG19(created_for_first_time=True, out_features = 200)

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