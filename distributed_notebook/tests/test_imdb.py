import shutil
import os

from distributed_notebook.datasets import IMDbLargeMovieReview
from distributed_notebook.tests.util import get_username

print(f"Current user: '{get_username()}'")


def test_imdb_download_fresh_with_bert():
    if os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert"):
        shutil.rmtree(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert")

def test_imdb_download_fresh_with_gpt2():
    if os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2"):
        shutil.rmtree(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2")

    if os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt2"):
        shutil.rmtree(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt2")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2")

def test_create_imdb_already_downloaded_bert():
    if os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert"):
        shutil.rmtree(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert")

    # Now remove the existing, tokenized data.
    if os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert"):
        shutil.rmtree(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert")

    imdb_dataset = IMDbLargeMovieReview(model_name = "bert")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

def test_create_imdb_already_downloaded_and_tokenized_bert():
    if os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert"):
        shutil.rmtree(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "bert")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/bert")

    imdb_dataset = IMDbLargeMovieReview(model_name = "bert")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert imdb_dataset.dataset_already_tokenized

def test_create_imdb_already_downloaded_gpt2():
    if os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2"):
        shutil.rmtree(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2")

    # Now remove the existing, tokenized data.
    if os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2"):
        shutil.rmtree(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2")

    imdb_dataset = IMDbLargeMovieReview(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

def test_create_imdb_already_downloaded_and_tokenized_gpt2():
    if os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb"):
        shutil.rmtree(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")

    if os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2"):
        shutil.rmtree(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2")

    imdb_dataset: IMDbLargeMovieReview = IMDbLargeMovieReview(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb")
    assert os.path.isdir(f"/home/{get_username()}/tokenized_datasets/stanfordnlp___imdb/gpt-2")

    imdb_dataset = IMDbLargeMovieReview(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert imdb_dataset.dataset_already_tokenized