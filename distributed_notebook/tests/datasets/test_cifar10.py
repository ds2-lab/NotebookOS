import os
import shutil

from distributed_notebook.deep_learning import CIFAR10


def test_tiny_imagenet_dataset_fresh():
    """
    Ensure that the LibriSpeech dataset is created correctly -- with just the test split downloaded.
    """
    root_directory: str = CIFAR10.default_root_directory

    if os.path.isdir(root_directory):
        print(f'Removing existing root directory "{root_directory}" for the CIFAR-10 dataset before unit test.')
        shutil.rmtree(root_directory)
    else:
        print(f'Root directory "{root_directory}" for the CIFAR-10 dataset does not yet exist (before unit test).')

    dataset: CIFAR10 = CIFAR10()

    assert dataset is not None

    assert not dataset.dataset_already_downloaded

    assert os.path.isdir(root_directory)

    print(f'Removing existing root directory "{root_directory}" for the CIFAR-10 dataset after unit test.')
    shutil.rmtree(root_directory)


def test_tiny_imagenet_dataset_already_downloaded():
    """
    Ensure that the LibriSpeech dataset is created correctly -- with just the test split downloaded.
    """
    root_directory: str = CIFAR10.default_root_directory

    if os.path.isdir(root_directory):
        print(f'Removing existing root directory "{root_directory}" for the CIFAR-10 dataset before unit test.')
        shutil.rmtree(root_directory)
    else:
        print(f'Root directory "{root_directory}" for the CIFAR-10 dataset does not yet exist (before unit test).')

    dataset: CIFAR10 = CIFAR10()

    assert dataset is not None
    assert not dataset.dataset_already_downloaded
    assert os.path.isdir(root_directory)

    # Re-create without removing anything.
    dataset = CIFAR10()

    assert dataset is not None
    assert dataset.dataset_already_downloaded
    assert os.path.isdir(root_directory)

    print(f'Removing existing root directory "{root_directory}" for the CIFAR-10 dataset after unit test.')
    shutil.rmtree(root_directory)
