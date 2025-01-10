import pytest
import os
import shutil

import torchaudio.datasets

from distributed_notebook.datasets import LibriSpeech

def test_libri_speech_runtime_error():
    """
    The constructor of the LibriSpeech dataset should raise a ValueError if both splits are specified as None.
    """
    with pytest.raises(ValueError):
        LibriSpeech(train_split = None, test_split = None)

def test_libri_speech_test_dataset():
    """
    Ensure that the LibriSpeech dataset is created correctly -- with just the test split downloaded.
    """
    pwd: str = os.getcwd()
    root_directory: str = os.path.join(pwd, "libri_speech_dataset")

    if os.path.isdir(root_directory):
        print(f'Removing existing root directory "{root_directory}" for LibriSpeech dataset before unit test.')
        shutil.rmtree(root_directory)
    else:
        print(f'Root directory "{root_directory}" for LibriSpeech dataset does not yet exist (before unit test).')

    # Create it, since we know it'll be empty.
    os.makedirs(name = root_directory, mode = 0o750, exist_ok = False)

    libri_speech_dataset: LibriSpeech = LibriSpeech(
        root_dir = root_directory, train_split = None, test_split = LibriSpeech.test_clean)

    assert libri_speech_dataset is not None

    assert not libri_speech_dataset.dataset_already_downloaded

    downloaded_archive_path: str = os.path.join(root_directory, torchaudio.datasets.librispeech.FOLDER_IN_ARCHIVE)
    downloaded_dataset_path: str = os.path.join(downloaded_archive_path, LibriSpeech.test_clean)

    assert os.path.isdir(downloaded_archive_path)
    assert os.path.isdir(downloaded_dataset_path)

    print(f'Removing existing root directory "{root_directory}" for LibriSpeech dataset after unit test.')
    shutil.rmtree(root_directory)

def test_libri_speech_test_dataset_already_downloaded():
    """
    Ensure that the LibriSpeech dataset is created correctly -- even after its already been downloaded
    """
    pwd: str = os.getcwd()
    root_directory: str = os.path.join(pwd, "libri_speech_dataset")

    if os.path.isdir(root_directory):
        print(f'Removing existing root directory "{root_directory}" for LibriSpeech dataset before unit test.')
        shutil.rmtree(root_directory)
    else:
        print(f'Root directory "{root_directory}" for LibriSpeech dataset does not yet exist (before unit test).')

    # Create it, since we know it'll be empty.
    os.makedirs(name = root_directory, mode = 0o750, exist_ok = False)

    libri_speech_dataset: LibriSpeech = LibriSpeech(
        root_dir = root_directory, train_split = None, test_split = LibriSpeech.test_clean)

    assert libri_speech_dataset is not None
    assert not libri_speech_dataset.dataset_already_downloaded

    downloaded_archive_path: str = os.path.join(root_directory, torchaudio.datasets.librispeech.FOLDER_IN_ARCHIVE)
    downloaded_dataset_path: str = os.path.join(downloaded_archive_path, LibriSpeech.test_clean)
    assert os.path.isdir(downloaded_archive_path)
    assert os.path.isdir(downloaded_dataset_path)

    # Re-create the LibriSpeech variable without removing the directories, so that the data is already downloaded.
    libri_speech_dataset = LibriSpeech(
        root_dir = root_directory, train_split = None, test_split = LibriSpeech.test_clean)

    assert libri_speech_dataset is not None
    assert libri_speech_dataset.dataset_already_downloaded
    assert os.path.isdir(downloaded_archive_path)
    assert os.path.isdir(downloaded_dataset_path)

    print(f'Removing existing root directory "{root_directory}" for LibriSpeech dataset after unit test.')
    shutil.rmtree(root_directory)