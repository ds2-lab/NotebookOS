from distributed_notebook.deep_learning.data import CoLA, IMDbLargeMovieReview, IMDbLargeMovieReviewTruncated
from distributed_notebook.deep_learning.models.nlp.bert import Bert

import torch

def test_train_bert_on_cola_cpu():
    """
    Train the Bert model on the CoLA dataset using the CPU.
    Validate that the weights are updated correctly.
    """
    dataset: CoLA = CoLA(model_name = "bert")
    model: Bert = Bert(out_features = 2)

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
    test_train_bert_on_cola_cpu()

def test_train_bert_on_cola():
    """
    Train the BERT model on the CoLA dataset. Validate that the weights are updated correctly.
    """
    dataset: CoLA = CoLA(model_name = "bert")
    model: Bert = Bert(out_features = 2)

    training_duration_ms: int = 2000
    if not torch.cuda.is_available():
        training_duration_ms = 3250

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

def test_train_bert_on_truncated_imdb():
    """
    Train the BERT model on the Truncated IMDb dataset. Validate that the weights are updated correctly.
    """
    dataset: IMDbLargeMovieReviewTruncated = IMDbLargeMovieReviewTruncated(model_name = "bert")
    model: Bert = Bert(out_features = 2)

    training_duration_ms: int = 2000
    if not torch.cuda.is_available():
        training_duration_ms = 3250

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

def test_train_bert_on_imdb():
    """
    Train the BERT model on the Truncated IMDb dataset. Validate that the weights are updated correctly.
    """
    dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")
    model: Bert = Bert(out_features = 2)

    training_duration_ms: int = 2000
    if not torch.cuda.is_available():
        training_duration_ms = 3250

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
