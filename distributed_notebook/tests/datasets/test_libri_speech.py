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
        shutil.rmtree(root_directory)

    libri_speech_dataset: LibriSpeech = LibriSpeech(
        train_split = None, test_split = LibriSpeech.test_clean)

    assert libri_speech_dataset is not None

    assert not libri_speech_dataset.dataset_already_downloaded

    downloaded_archive_path: str = os.path.join(root_directory, torchaudio.datasets.librispeech.FOLDER_IN_ARCHIVE)
    downloaded_dataset_path: str = os.path.join(downloaded_archive_path, LibriSpeech.test_clean)

    assert os.path.isdir(downloaded_archive_path)
    assert os.path.isdir(downloaded_dataset_path)