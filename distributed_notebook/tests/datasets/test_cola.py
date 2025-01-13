import shutil
import os

from distributed_notebook.deep_learning.datasets import CoLA
from distributed_notebook.tests.util import get_username

print(f"Current user: '{get_username()}'")


def test_cola_download_fresh_with_bert():
    if os.path.isdir(CoLA.default_root_directory):
        shutil.rmtree(CoLA.default_root_directory)

    if os.path.isdir(CoLA.get_tokenized_dataset_directory("bert")):
        shutil.rmtree(CoLA.get_tokenized_dataset_directory("bert"))

    cola_dataset: CoLA = CoLA(model_name = "bert")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir(CoLA.default_root_directory)
    assert os.path.isdir(CoLA.get_tokenized_dataset_directory("bert"))

def test_cola_download_fresh_with_gpt2():
    if os.path.isdir(CoLA.default_root_directory):
        shutil.rmtree(CoLA.default_root_directory)

    if os.path.isdir(CoLA.get_tokenized_dataset_directory("gpt-2")):
        shutil.rmtree(CoLA.get_tokenized_dataset_directory("gpt-2"))

    if os.path.isdir(CoLA.get_tokenized_dataset_directory("gpt-2")):
        shutil.rmtree(CoLA.get_tokenized_dataset_directory("gpt-2"))

    cola_dataset: CoLA = CoLA(model_name = "gpt-2")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir(CoLA.default_root_directory)
    assert os.path.isdir(CoLA.get_tokenized_dataset_directory("gpt-2"))

def test_create_cola_already_downloaded_bert():
    if os.path.isdir(CoLA.default_root_directory):
        shutil.rmtree(CoLA.default_root_directory)

    if os.path.isdir(CoLA.get_tokenized_dataset_directory("bert")):
        shutil.rmtree(CoLA.get_tokenized_dataset_directory("bert"))

    cola_dataset: CoLA = CoLA(model_name = "bert")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir(CoLA.default_root_directory)
    assert os.path.isdir(CoLA.get_tokenized_dataset_directory("bert"))

    # Now remove the existing, tokenized data.
    if os.path.isdir(CoLA.get_tokenized_dataset_directory("bert")):
        shutil.rmtree(CoLA.get_tokenized_dataset_directory("bert"))

    cola_dataset = CoLA(model_name = "bert")

    assert cola_dataset is not None

    assert cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

def test_create_cola_already_downloaded_and_tokenized_bert():
    if os.path.isdir(CoLA.default_root_directory):
        shutil.rmtree(CoLA.default_root_directory)

    if os.path.isdir(CoLA.get_tokenized_dataset_directory("bert")):
        shutil.rmtree(CoLA.get_tokenized_dataset_directory("bert"))

    cola_dataset: CoLA = CoLA(model_name = "bert")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir(CoLA.default_root_directory)
    assert os.path.isdir(CoLA.get_tokenized_dataset_directory("bert"))

    cola_dataset = CoLA(model_name = "bert")

    assert cola_dataset is not None

    assert cola_dataset.dataset_already_downloaded
    assert cola_dataset.dataset_already_tokenized

def test_create_cola_already_downloaded_gpt2():
    if os.path.isdir(CoLA.default_root_directory):
        shutil.rmtree(CoLA.default_root_directory)

    if os.path.isdir(CoLA.get_tokenized_dataset_directory("gpt-2")):
        shutil.rmtree(CoLA.get_tokenized_dataset_directory("gpt-2"))

    cola_dataset: CoLA = CoLA(model_name = "gpt-2")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir(CoLA.default_root_directory)
    assert os.path.isdir(CoLA.get_tokenized_dataset_directory("gpt-2"))

    # Now remove the existing, tokenized data.
    if os.path.isdir(CoLA.get_tokenized_dataset_directory("gpt-2")):
        shutil.rmtree(CoLA.get_tokenized_dataset_directory("gpt-2"))

    cola_dataset = CoLA(model_name = "gpt-2")

    assert cola_dataset is not None

    assert cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

def test_create_cola_already_downloaded_and_tokenized_gpt2():
    if os.path.isdir(CoLA.default_root_directory):
        shutil.rmtree(CoLA.default_root_directory)

    if os.path.isdir(CoLA.get_tokenized_dataset_directory("gpt-2")):
        shutil.rmtree(CoLA.get_tokenized_dataset_directory("gpt-2"))

    cola_dataset: CoLA = CoLA(model_name = "gpt-2")

    assert cola_dataset is not None

    assert not cola_dataset.dataset_already_downloaded
    assert not cola_dataset.dataset_already_tokenized

    assert os.path.isdir(CoLA.default_root_directory)
    assert os.path.isdir(CoLA.get_tokenized_dataset_directory("gpt-2"))

    cola_dataset = CoLA(model_name = "gpt-2")

    assert cola_dataset is not None

    assert cola_dataset.dataset_already_downloaded
    assert cola_dataset.dataset_already_tokenized