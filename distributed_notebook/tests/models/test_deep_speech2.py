import os
import torch

from distributed_notebook.deep_learning.data import LibriSpeech
from distributed_notebook.deep_learning.models import DeepSpeech2


def test_train_deep_speech2():
    """
    Train the Deep Speech 2 model on the LibriSpeech dataset. Validate that the weights are updated correctly.
    """
    dataset: LibriSpeech = LibriSpeech(
        root_dir=os.path.expanduser("~/.cache"),
        train_split=LibriSpeech.test_clean, # Use the 'test_clean' split as a training split bc it's (relatively) small.
        test_split=None,
        batch_size=20,
    )
    model: DeepSpeech2 = DeepSpeech2()
    output_layer: torch.nn.Module = model.output_layer

    training_duration_ms: int = 2000
    if not torch.cuda.is_available():
        training_duration_ms = 3250

    # Extract weights and biases
    prev_weights = output_layer.weight.detach().cpu()
    for _ in range(0, 3):
        print(f"Initial weights: {prev_weights}")
        model.train(dataset.train_loader, training_duration_ms)

        updated_weights = output_layer.weight.detach().cpu()
        print(f"Updated weights: {updated_weights}")

        assert prev_weights.equal(updated_weights) == False
        prev_weights = updated_weights
