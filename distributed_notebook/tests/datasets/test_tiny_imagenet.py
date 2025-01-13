import os
import shutil

from distributed_notebook.deep_learning.datasets import TinyImageNet


def test_tiny_imagenet_dataset_fresh():
    """
    Ensure that the LibriSpeech dataset is created correctly -- with just the test split downloaded.
    """
    root_directory: str = TinyImageNet.default_root_directory

    if os.path.isdir(root_directory):
        print(f'Removing existing root directory "{root_directory}" for Tiny ImageNet dataset before unit test.')
        shutil.rmtree(root_directory)
    else:
        print(f'Root directory "{root_directory}" for Tiny ImageNet dataset does not yet exist (before unit test).')

    libri_speech_dataset: TinyImageNet = TinyImageNet()

    assert libri_speech_dataset is not None

    assert not libri_speech_dataset.dataset_already_downloaded

    assert os.path.isdir(root_directory)

    print(f'Removing existing root directory "{root_directory}" for LibriSpeech dataset after unit test.')
    shutil.rmtree(root_directory)


def test_tiny_imagenet_dataset_already_downloaded():
    """
    Ensure that the LibriSpeech dataset is created correctly -- with just the test split downloaded.
    """
    root_directory: str = TinyImageNet.default_root_directory

    if os.path.isdir(root_directory):
        print(f'Removing existing root directory "{root_directory}" for Tiny ImageNet dataset before unit test.')
        shutil.rmtree(root_directory)
    else:
        print(f'Root directory "{root_directory}" for Tiny ImageNet dataset does not yet exist (before unit test).')

    libri_speech_dataset: TinyImageNet = TinyImageNet()

    assert libri_speech_dataset is not None
    assert not libri_speech_dataset.dataset_already_downloaded
    assert os.path.isdir(root_directory)

    # Re-create without removing anything.
    libri_speech_dataset = TinyImageNet()

    assert libri_speech_dataset is not None
    assert libri_speech_dataset.dataset_already_downloaded
    assert os.path.isdir(root_directory)

    print(f'Removing existing root directory "{root_directory}" for LibriSpeech dataset after unit test.')
    shutil.rmtree(root_directory)
