import shutil
import os

from distributed_notebook.datasets import IMDbLargeMovieReview

def test_cola_download_fresh_with_bert():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/bert"):
        shutil.rmtree("~/tokenized_datasets/glue/bert")

    cola_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir("~/tokenized_datasets/glue/bert")

def test_cola_download_fresh_with_gpt2():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/gpt-2"):
        shutil.rmtree("~/tokenized_datasets/glue/gpt-2")

    if os.path.isdir("~/tokenized_datasets/glue/gpt2"):
        shutil.rmtree("~/tokenized_datasets/glue/gpt2")

    cola_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "gpt-2")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir("~/tokenized_datasets/glue/gpt-2")

def test_create_cola_already_downloaded_bert():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/bert"):
        shutil.rmtree("~/tokenized_datasets/glue/bert")

    cola_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir("~/tokenized_datasets/glue/bert")

    # Now remove the existing, tokenized data.
    if os.path.isdir("~/tokenized_datasets/glue/bert"):
        shutil.rmtree("~/tokenized_datasets/glue/bert")

    cola_dataset = IMDbLargeMovieReview(model_name = "bert")

    assert cola_dataset is not None

    assert cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

def test_create_cola_already_downloaded_and_tokenized_bert():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/bert"):
        shutil.rmtree("~/tokenized_datasets/glue/bert")

    cola_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir("~/tokenized_datasets/glue/bert")

    cola_dataset = IMDbLargeMovieReview(model_name = "bert")

    assert cola_dataset is not None

    assert cola_dataset.dataset_already_downloaded
    assert cola_dataset.dataset_already_tokenized

def test_create_cola_already_downloaded_gpt2():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/gpt-2"):
        shutil.rmtree("~/tokenized_datasets/glue/gpt-2")

    cola_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "gpt-2")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir("~/tokenized_datasets/glue/gpt-2")

    # Now remove the existing, tokenized data.
    if os.path.isdir("~/tokenized_datasets/glue/gpt-2"):
        shutil.rmtree("~/tokenized_datasets/glue/gpt-2")

    cola_dataset = IMDbLargeMovieReview(model_name = "gpt-2")

    assert cola_dataset is not None

    assert cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

def test_create_cola_already_downloaded_and_tokenized_gpt2():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/gpt-2"):
        shutil.rmtree("~/tokenized_datasets/glue/gpt-2")

    cola_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "gpt-2")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/gpt-2")
    assert os.path.isdir("~/tokenized_datasets/glue/gpt-2")

    cola_dataset = IMDbLargeMovieReview(model_name = "gpt-2")

    assert cola_dataset is not None

    assert cola_dataset.dataset_already_downloaded
    assert cola_dataset.dataset_already_tokenized

dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")