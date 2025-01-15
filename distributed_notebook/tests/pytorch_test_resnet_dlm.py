from distributed_notebook.deep_learning import CIFAR10, ResNet18

dataset: CIFAR10 = CIFAR10(image_size=ResNet18.expected_image_size())
model: ResNet18 = ResNet18(created_for_first_time=True, gpu_device_ids=[0])

training_duration_ms: int = 2000

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

model.set_gpu_device_ids([0, 1])

training_duration_ms: int = 2000

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