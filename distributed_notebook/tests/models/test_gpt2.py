from distributed_notebook.datasets import CoLA, IMDbLargeMovieReviewTruncated
from distributed_notebook.models.nlp.gpt2 import GPT2

def test_train_gpt2_on_cola():
    """
    Train the GPT-2 model on the CoLA dataset. Validate that the weights are updated correctly.
    """
    dataset: CoLA = CoLA(model_name = "gpt2")
    model: GPT2 = GPT2(out_features = 2)

    training_duration_ms: int = 1000

    # Access the classification head (last layer)
    classifier = model.model.classifier

    # Extract weights and biases
    prev_weights = classifier.weight.detach().cpu()
    for _ in range(0, 3):
        print(f"Initial weights: {prev_weights}")
        model.train(dataset.train_loader, training_duration_ms)

        updated_weights = classifier.weight.detach().cpu()
        print(f"Updated weights: {updated_weights}")

        assert prev_weights.equal(updated_weights) == False
        prev_weights = updated_weights

def test_train_gpt2_on_imdb():
    """
    Train the GPT-2 model on the Truncated IMDb dataset. Validate that the weights are updated correctly.
    """
    dataset: IMDbLargeMovieReviewTruncated = IMDbLargeMovieReviewTruncated(model_name = "gpt2")
    model: GPT2 = GPT2(out_features = 2)

    training_duration_ms: int = 1000

    # Access the classification head (last layer)
    classifier = model.model.classifier

    # Extract weights and biases
    prev_weights = classifier.weight.detach().cpu()
    for _ in range(0, 3):
        print(f"Initial weights: {prev_weights}")
        model.train(dataset.train_loader, training_duration_ms)

        updated_weights = classifier.weight.detach().cpu()
        print(f"Updated weights: {updated_weights}")

        assert prev_weights.equal(updated_weights) == False
        prev_weights = updated_weights
