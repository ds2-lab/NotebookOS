import shutil
import os

from distributed_notebook.deep_learning.datasets.nlp.imdb_truncated import IMDbLargeMovieReviewTruncated
from distributed_notebook.tests.util import get_username

print(f"Current user: '{get_username()}'")


def test_imdb_download_fresh_with_bert():
    if os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.default_root_directory)

    if os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert")):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert"))

    imdb_dataset: IMDbLargeMovieReviewTruncated = IMDbLargeMovieReviewTruncated(model_name = "bert")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory)
    assert os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert"))

def test_imdb_download_fresh_with_gpt2():
    if os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.default_root_directory)

    if os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2")):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2"))

    if os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2")):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2"))

    imdb_dataset: IMDbLargeMovieReviewTruncated = IMDbLargeMovieReviewTruncated(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory)
    assert os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2"))

def test_create_imdb_already_downloaded_bert():
    if os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.default_root_directory)

    if os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert")):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert"))

    imdb_dataset: IMDbLargeMovieReviewTruncated = IMDbLargeMovieReviewTruncated(model_name = "bert")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory)
    assert os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert"))

    # Now remove the existing, tokenized data.
    if os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert")):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert"))

    imdb_dataset = IMDbLargeMovieReviewTruncated(model_name = "bert")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

def test_create_imdb_already_downloaded_and_tokenized_bert():
    if os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.default_root_directory)

    if os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert")):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert"))

    imdb_dataset: IMDbLargeMovieReviewTruncated = IMDbLargeMovieReviewTruncated(model_name = "bert")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory)
    assert os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("bert"))

    imdb_dataset = IMDbLargeMovieReviewTruncated(model_name = "bert")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert imdb_dataset.dataset_already_tokenized

def test_create_imdb_already_downloaded_gpt2():
    if os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.default_root_directory)

    if os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2")):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2"))

    imdb_dataset: IMDbLargeMovieReviewTruncated = IMDbLargeMovieReviewTruncated(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory)
    assert os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2"))

    # Now remove the existing, tokenized data.
    if os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2")):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2"))

    imdb_dataset = IMDbLargeMovieReviewTruncated(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

def test_create_imdb_already_downloaded_and_tokenized_gpt2():
    if os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.default_root_directory)

    if os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2")):
        shutil.rmtree(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2"))

    imdb_dataset: IMDbLargeMovieReviewTruncated = IMDbLargeMovieReviewTruncated(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert not imdb_dataset.dataset_already_downloaded
    assert not imdb_dataset.dataset_already_tokenized

    assert os.path.isdir(IMDbLargeMovieReviewTruncated.default_root_directory)
    assert os.path.isdir(IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory("gpt-2"))

    imdb_dataset = IMDbLargeMovieReviewTruncated(model_name = "gpt-2")

    assert imdb_dataset is not None

    assert imdb_dataset.dataset_already_downloaded
    assert imdb_dataset.dataset_already_tokenized