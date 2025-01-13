from distributed_notebook.deep_learning import CIFAR10, ResNet18, TinyImageNet


def test_train_resnet18_on_cifar10():
    """
    Train the ResNet-18 model on the CIFAR-10 dataset. Validate that the weights are updated correctly.
    """
    dataset: CIFAR10 = CIFAR10()
    model: ResNet18 = ResNet18(created_for_first_time=True)

    training_duration_ms: int = 1000

    # Access the classification head (last layer)
    fc = model.model.fc

    # Extract weights and biases
    prev_weights = fc.weight.detach().cpu()
    for _ in range(0, 3):
        print(f"Initial weights: {prev_weights}")
        model.train(dataset.train_loader, training_duration_ms)

        updated_weights = fc.weight.detach().cpu()
        print(f"Updated weights: {updated_weights}")

        assert prev_weights.equal(updated_weights) == False
        prev_weights = updated_weights


def test_train_resnet18_on_tiny_imagenet():
    """
    Train the ResNet-18 model on the Tiny ImageNet dataset. Validate that the weights are updated correctly.
    """
    dataset: TinyImageNet = TinyImageNet()
    model: ResNet18 = ResNet18(created_for_first_time=True, out_features = 200)

    training_duration_ms: int = 1000

    # Access the classification head (last layer)
    fc = model.model.fc

    # Extract weights and biases
    prev_weights = fc.weight.detach().cpu()
    for _ in range(0, 3):
        print(f"Initial weights: {prev_weights}")
        model.train(dataset.train_loader, training_duration_ms)

        updated_weights = fc.weight.detach().cpu()
        print(f"Updated weights: {updated_weights}")

        assert prev_weights.equal(updated_weights) == False
        prev_weights = updated_weights
