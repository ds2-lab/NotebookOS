import shutil
import os

from distributed_notebook.datasets import IMDbLargeMovieReview

def test_imdb_download_fresh_with_bert():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/bert"):
        shutil.rmtree("~/tokenized_datasets/glue/bert")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir("~/tokenized_datasets/glue/bert")

def test_imdb_download_fresh_with_gpt2():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/gpt-2"):
        shutil.rmtree("~/tokenized_datasets/glue/gpt-2")

    if os.path.isdir("~/tokenized_datasets/glue/gpt2"):
        shutil.rmtree("~/tokenized_datasets/glue/gpt2")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir("~/tokenized_datasets/glue/gpt-2")

def test_create_imdb_already_downloaded_bert():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/bert"):
        shutil.rmtree("~/tokenized_datasets/glue/bert")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir("~/tokenized_datasets/glue/bert")

    # Now remove the existing, tokenized data.
    if os.path.isdir("~/tokenized_datasets/glue/bert"):
        shutil.rmtree("~/tokenized_datasets/glue/bert")

    imdb_dataset = IMDbLargeMovieReview(model_name = "bert")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

def test_create_imdb_already_downloaded_and_tokenized_bert():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/bert"):
        shutil.rmtree("~/tokenized_datasets/glue/bert")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir("~/tokenized_datasets/glue/bert")

    imdb_dataset = IMDbLargeMovieReview(model_name = "bert")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert imdb_dataset.dataset_already_tokenized

def test_create_imdb_already_downloaded_gpt2():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/gpt-2"):
        shutil.rmtree("~/tokenized_datasets/glue/gpt-2")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir("~/tokenized_datasets/glue/gpt-2")

    # Now remove the existing, tokenized data.
    if os.path.isdir("~/tokenized_datasets/glue/gpt-2"):
        shutil.rmtree("~/tokenized_datasets/glue/gpt-2")

    imdb_dataset = IMDbLargeMovieReview(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

def test_create_imdb_already_downloaded_and_tokenized_gpt2():
    if os.path.isdir("~/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree("~/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir("~/tokenized_datasets/glue/gpt-2"):
        shutil.rmtree("~/tokenized_datasets/glue/gpt-2")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir("~/.cache/huggingface/datasets/gpt-2")
    assert os.path.isdir("~/tokenized_datasets/glue/gpt-2")

    imdb_dataset = IMDbLargeMovieReview(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert imdb_dataset.dataset_already_tokenized

dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")