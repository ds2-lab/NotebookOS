import shutil

import random
import time
import torch
import os
import traceback

from typing import List, Optional, Dict, Union

import torchaudio
import torch.nn as nn
from torch.utils.data import DataLoader

from torchaudio import datasets

from distributed_notebook.deep_learning.data.custom_dataset import CustomDataset

from distributed_notebook.deep_learning.configuration import Speech
from distributed_notebook.deep_learning.data.loader import WrappedLoader

char_map_str: str = """
 ' 0
 <SPACE> 1
 a 2
 b 3
 c 4
 d 5
 e 6
 f 7
 g 8
 h 9
 i 10
 j 11
 k 12
 l 13
 m 14
 n 15
 o 16
 p 17
 q 18
 r 19
 s 20
 t 21
 u 22
 v 23
 w 24
 x 25
 y 26
 z 27
 """

class TextTransform:
    """Maps characters to integers and vice versa"""
    def __init__(self):
        _char_map_str: str = char_map_str
        self.char_map = {}
        self.index_map = {}
        for line in _char_map_str.strip().split('\n'):
            ch, index = line.split()
            self.char_map[ch] = int(index)
            self.index_map[int(index)] = ch
        self.index_map[1] = ' '

    def text_to_int(self, text):
        """ Use a character map and convert text to an integer sequence """
        int_sequence = []
        for c in text:
            if c == ' ':
                # I don't know if this part here is right. The example had:
                # ch = self.char_map['']
                # And that wasn't working (KeyError).
                ch = self.char_map["'"]
            else:
                ch = self.char_map[c]
            int_sequence.append(ch)
        return int_sequence

    def int_to_text(self, labels):
        """ Use a character map and convert integer labels to a text sequence """
        string = []
        for i in labels:
            string.append(self.index_map[i])
        return ''.join(string).replace('', ' ')

valid_audio_transforms = torchaudio.transforms.MelSpectrogram()
text_transform = TextTransform()
train_audio_transforms = nn.Sequential(
    torchaudio.transforms.MelSpectrogram(sample_rate=16000, n_mels=128),
    torchaudio.transforms.FrequencyMasking(freq_mask_param=15),
    torchaudio.transforms.TimeMasking(time_mask_param=35)
)

def data_processing(data, data_type="train"):
    spectrograms = []
    labels = []
    input_lengths = []
    label_lengths = []
    for (waveform, _, utterance, _, _, _) in data:
        if data_type == 'train':
            spec = train_audio_transforms(waveform).squeeze(0).transpose(0, 1)
        else:
            spec = valid_audio_transforms(waveform).squeeze(0).transpose(0, 1)
        spectrograms.append(spec)
        label = torch.Tensor(text_transform.text_to_int(utterance.lower()))
        labels.append(label)
        input_lengths.append(spec.shape[0]//2)
        label_lengths.append(len(label))

    spectrograms = nn.utils.rnn.pad_sequence(spectrograms, batch_first=True).unsqueeze(1).transpose(2, 3)
    labels = nn.utils.rnn.pad_sequence(labels, batch_first=True)

    return spectrograms, labels, input_lengths, label_lengths

# Data preprocessing function.
# This is written specifically for use with the Deep Speech v1 model.
def collate_fn(batch):
    """
    Data preprocessing function.
    This is written specifically for use with the Deep Speech v1 model.
    """
    waveforms, labels, input_lengths, label_lengths = [], [], [], []
    for waveform, _, label, *_ in batch:
        waveforms.append(waveform)
        labels.append(label)
        input_lengths.append(waveform.shape[1])
        label_lengths.append(len(label))
    waveforms = torch.nn.utils.rnn.pad_sequence(waveforms, batch_first=True).squeeze(1)
    labels = torch.cat(labels)
    return waveforms, labels, torch.tensor(input_lengths), torch.tensor(label_lengths)

class LibriSpeech(CustomDataset):
    train_clean_100:str = "train-clean-100"
    train_clean_360:str = "train-clean-360"
    train_clean_500:str = "train-other-500"

    test_clean:str = "test-clean"
    test_other:str = "test-other"

    _train_splits: List[str] = [
        "train-clean-100",
        "train-clean-360",
        "train-other-500"
    ]

    _test_splits: List[str] = ["test-clean", "test-other"]

    default_root_directory:str = os.path.expanduser("~/.cache/distributed_notebook/datasets/librispeech")

    def __init__(
            self,
            root_dir: str = default_root_directory,
            folder_in_archive: str = datasets.librispeech.FOLDER_IN_ARCHIVE,
            batch_size: int = 1,
            shuffle: bool = True,
            num_workers: int = 2,
            # Default to test_clean, even though it's a test set, because it is small (good for unit tests)
            train_split: Optional[str] = test_clean,
            test_split: Optional[str] = None,
            **kwargs,
    ):
        assert folder_in_archive is not None

        super().__init__(root_dir=root_dir, shuffle=shuffle, num_workers=num_workers, **kwargs)

        if train_split is not None and train_split not in LibriSpeech._train_splits[0]:
            self.log.debug(f'[WARNING] Specified training split "{train_split}" is technically NOT a training split.')

        if test_split is not None and test_split not in LibriSpeech._test_splits[0]:
            self.log.debug(f'[WARNING] Specified test/validation split "{test_split}" is technically NOT a test/validation split.')

        if train_split is None and test_split is None:
            raise ValueError("At least one of the training split and test split should be non-null.")

        self.log.debug(f'Creating root directory for {self.name} dataset (if it does not already exist): "{root_dir}"')
        os.makedirs(root_dir, 0o750, exist_ok=True)

        self._train_split: Optional[str] = train_split
        self._test_split: Optional[str] = test_split
        self._folder_in_archive: str = folder_in_archive

        self.__init_dataset(
            train_split=train_split,
            test_split=test_split,
            root_dir=root_dir,
            folder_in_archive=folder_in_archive,
        )

        if self._train_dataset is not None:
            self._train_loader: Optional[WrappedLoader] = WrappedLoader(
                self._train_dataset,
                batch_size=batch_size,
                shuffle=shuffle,
                num_workers=num_workers,
                collate_fn=lambda x: data_processing(x, 'train'),
                dataset_name=self.dataset_name(),
            )
        else:
            self._train_loader: Optional[WrappedLoader] = None

        if self._test_dataset is not None:
            self._test_loader: Optional[WrappedLoader] = WrappedLoader(
                self._test_dataset,
                batch_size=batch_size,
                shuffle=shuffle,
                num_workers=num_workers,
                collate_fn=lambda x: data_processing(x, 'valid'),
                dataset_name=self.dataset_name(),
            )
        else:
            self._test_loader: Optional[WrappedLoader] = None

    def __init_dataset_no_download(self, train_split:str, test_split: str, root_dir: str, folder_in_archive: str):
        """
        Attempts to create the LibriSpeech dataset using the TorchAudio module without downloading anything.

        That is, this tries to create the LibriSpeech dataset using files that are already downloaded locally.
        """
        # Create the dataset without downloading it.
        # If there's a RuntimeError, then it hasn't been downloaded yet.
        if train_split is not None:
            self._train_dataset: Optional[datasets.LIBRISPEECH] = datasets.LIBRISPEECH(
                root=root_dir, download=False, url=train_split, folder_in_archive = folder_in_archive)
        else:
            self._train_dataset: Optional[datasets.LIBRISPEECH] = None

        if test_split is not None:
            self._test_dataset: Optional[datasets.LIBRISPEECH] = datasets.LIBRISPEECH(
                root=root_dir, download=False, url=test_split, folder_in_archive = folder_in_archive)
        else:
            self._test_dataset: Optional[datasets.LIBRISPEECH] = None

        self.log.debug(f"The {self.name} dataset was already downloaded. Root directory: \"{root_dir}\"")

        # No RuntimeError. The dataset must have already been downloaded.
        self._dataset_already_downloaded: bool = True

    def __init_dataset_download(self, train_split:str, test_split:str, root_dir: str, folder_in_archive: str):
        """
        Attempts to create the LibriSpeech dataset using the TorchAudio module by downloading the data.
        """
        self._dataset_already_downloaded: bool = False

        self.log.debug(f'Downloading {self.name} dataset to root directory "{root_dir}" now. '
                       f'Specifying training URL="{train_split}" and test URL="{test_split}".')

        self._download_start = time.time()

        _root: str | bytes = os.fspath(root_dir)
        base_url: str = "http://www.openslr.org/resources/12/"
        ext_archive: str = ".tar.gz"

        if train_split is not None:
            training_filename: str = train_split + ext_archive
            training_archive: str = os.path.join(_root, training_filename)
            training_download_url: str = os.path.join(base_url, training_filename)

            self.log.debug(f'Will attempt to download LibriSpeech split "{train_split}" from URL '
                           f'"{training_download_url}" to local archive file "{training_archive}".')

            self._train_dataset: Optional[datasets.LIBRISPEECH] = datasets.LIBRISPEECH(
                root=root_dir, download=True, url=train_split, folder_in_archive = folder_in_archive)
        else:
            self._train_dataset: Optional[datasets.LIBRISPEECH] = None

        if test_split is not None:
            test_filename: str = train_split + ext_archive
            test_archive: str = os.path.join(_root, test_filename)
            test_download_url: str = os.path.join(base_url, test_filename)

            self.log.debug(f'Will attempt to download LibriSpeech split "{test_split}" from URL '
                           f'"{test_download_url}" to local archive file "{test_archive}".')

            self._test_dataset: Optional[datasets.LIBRISPEECH] = datasets.LIBRISPEECH(
                root=root_dir, download=True, url=test_split, folder_in_archive = folder_in_archive)
        else:
            self._test_dataset: Optional[datasets.LIBRISPEECH] = None

        self._download_end = time.time()
        self._download_duration_sec = self._download_end - self._download_start

        self.log.debug(f"The {self.name} dataset was downloaded to root directory \"{root_dir}\" in {self._download_duration_sec} seconds.")


    def __init_dataset(self, train_split:str, test_split: str, root_dir: str, folder_in_archive: str):
        """
        Creates the LibriSpeech dataset using the TorchAudio module.
        """
        try:
            self.__init_dataset_no_download(
                train_split=train_split,
                test_split=test_split,
                root_dir=root_dir,
                folder_in_archive=folder_in_archive,
            )
        except RuntimeError:
            num_tries: int = 0
            base_retry_delay: int = 1
            max_delay: int = 30
            max_num_tries: int = 10

            # We'll give it a few tries...
            while num_tries < max_num_tries:
                try:
                    self.__init_dataset_download(
                        train_split=train_split,
                        test_split=test_split,
                        root_dir=root_dir,
                        folder_in_archive=folder_in_archive,
                    )

                    # If we got through the above with no exceptions, then we're good to go.
                    return
                except ConnectionRefusedError as ex:
                    self.log.warn("ConnectionRefusedError encountered while attempting to download the LibriSpeech dataset.")
                    self.log.warn(f'TrainSplit="{train_split}", TestSplit="{test_split}", RootDir="{root_dir}", FolderInArchive="{folder_in_archive}"')
                    self.log.warn(f"Exception: {ex}")
                    self.log.warn(traceback.format_exc())

                    # Don't bother sleeping if we're just going to exit the loop.
                    if num_tries + 1 == max_num_tries:
                        break

                    # Basic exponential backoff with jitter.
                    sleep_interval: float = base_retry_delay * (2 ** num_tries) + random.uniform(0, 1)
                    if sleep_interval > max_delay: # Clamp, but still add some Jitter.
                        sleep_interval = max_delay + random.uniform(0, 1)

                    self.log.warn(f"Sleeping for {sleep_interval} seconds before trying again...")
                    time.sleep(sleep_interval)

                    num_tries += 1
                except Exception as ex:
                    self.log.error(f"{type(ex).__name__} encountered while attempting to download the LibriSpeech dataset.")
                    self.log.error(f'TrainSplit="{train_split}", TestSplit="{test_split}", RootDir="{root_dir}", FolderInArchive="{folder_in_archive}"')
                    self.log.error(f"Exception: {ex}")
                    self.log.error(traceback.format_exc())

                    # Don't bother sleeping if we're just going to exit the loop.
                    if num_tries + 1 == max_num_tries:
                        break

                    # Basic exponential backoff with jitter.
                    sleep_interval: float = base_retry_delay * (2 ** num_tries) + random.uniform(0, 1)
                    if sleep_interval > max_delay: # Clamp, but still add some Jitter.
                        sleep_interval = max_delay + random.uniform(0, 1)

                    self.log.warn(f"Sleeping for {sleep_interval} seconds before trying again...")
                    time.sleep(sleep_interval)

                    num_tries += 1

            self.log.error(f"Failed to download the LibriSpeech dataset after {max_num_tries} tries.")
            self.log.error(f'TrainSplit="{train_split}", TestSplit="{test_split}", RootDir="{root_dir}", FolderInArchive="{folder_in_archive}"')
            raise ValueError(f"Failed to download the LibriSpeech dataset after {max_num_tries} tries.")

    def remove_local_files(self):
        """
        Remove any local files on disk.
        """
        self.log.debug(f'Cleaning up cache files for "{self.name}" dataset. Removing directory "{self.root_directory}".')

        st: float = time.time()

        shutil.rmtree(self.root_directory, ignore_errors=True)

        self.log.debug(f'Successfully cleaned-up cache files for "{self.name}" '
                       f'dataset in {round(time.time() - st, 3):,} seconds.')

    @staticmethod
    def category() -> str:
        return Speech

    @property
    def download_duration_sec(self) -> float:
        return self._download_duration_sec

    @property
    def dataset_already_downloaded(self) -> bool:
        return self._dataset_already_downloaded

    @dataset_already_downloaded.setter
    def dataset_already_downloaded(self, val: bool):
        self._dataset_already_downloaded = val

    @property
    def download_start_time(self) -> float:
        return self._download_start

    @property
    def download_end_time(self) -> float:
        return self._download_end

    @property
    def download_duration(self) -> float:
        return self._download_duration_sec

    @property
    def download_start(self) -> float:
        return self._download_start

    @property
    def download_end(self) -> float:
        return self._download_end

    @property
    def folder_in_archive(self) -> str:
        return self._folder_in_archive

    @property
    def description(self) -> Dict[str, Union[str, int, bool]]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["train_split"] = self._train_split
        desc["test_split"] = self._test_split
        desc["folder_in_archive"] = self._folder_in_archive

        return desc

    @property
    def train_split(self) -> Optional[str]:
        return self._train_split

    @property
    def test_split(self) -> Optional[str]:
        return self._test_split

    @property
    def train_dataset(self)->Optional[datasets.LIBRISPEECH]:
        return self._train_dataset

    @property
    def train_loader(self)->Optional[DataLoader]:
        return self._train_loader

    @property
    def test_dataset(self)->Optional[datasets.LIBRISPEECH]:
        return self._test_dataset

    @property
    def test_loader(self)->Optional[DataLoader]:
        return self._test_loader

    @property
    def tokenization_start(self) -> float:
        return -1

    @property
    def tokenization_end(self) -> float:
        return -1

    @property
    def tokenization_duration_sec(self) -> float:
        return -1

    @property
    def recorded_tokenization_overhead(self)->bool:
        """
        Speech datasets do not need to be tokenized, so we always return True.
        :return:
        """
        return True

    def set_recorded_tokenization_overhead(self, val: bool = True):
        """
        This should be called by the kernel when it retrieves the tokenization overhead, as we only
        tokenize the dataset once. This flag lets us know that we've already recorded the tokenization
        overhead and should not re-record it again in the future.
        """
        # No-op
        pass

    @property
    def requires_tokenization(self) -> bool:
        return False

    @property
    def name(self)->str:
        return LibriSpeech.dataset_name()

    @staticmethod
    def dataset_name()->str:
        return "LibriSpeech"