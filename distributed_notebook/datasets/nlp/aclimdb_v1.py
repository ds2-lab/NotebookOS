import os

from datasets import load_dataset, DownloadMode

from distributed_notebook.datasets.base import CustomDataset

import time

from distributed_notebook.datasets.nlp.base import NLPDataset
from distributed_notebook.datasets.nlp.util import get_tokenizer

IMDbName:str = "IMDb Large Movie Review Dataset"

class IMDbLargeMovieReview(NLPDataset):
    def __init__(self, batch_size: int = 256, shuffle: bool = True, num_workers: int = 2, model_name:str = "", **kwargs):
        assert model_name is not None and model_name != ""

        super().__init__(
            dataset_name = IMDbName,
            root_dir = "~/.cache/huggingface/datasets/stanfordnlp___imdb",
            shuffle = shuffle,
            num_workers = num_workers
        )

        self._dataset_already_downloaded: bool = os.path.exists("~/.cache/huggingface/datasets/stanfordnlp___imdb")

        self._download_start = time.time()

        dataset = load_dataset(
            path = "stanfordnlp/imdb",
            download_mode = DownloadMode.REUSE_DATASET_IF_EXISTS,
        )
        self.tokenizer = get_tokenizer(model_name)

        self._download_end = time.time()

        # Tokenization
        def tokenize_function(example):
            return self.tokenizer(example["text"], truncation=True, padding="max_length", max_length=128)

        tokenized_datasets = dataset.map(tokenize_function, batched=True)
        tokenized_datasets = tokenized_datasets.remove_columns(["sentence", "idx"])
        tokenized_datasets = tokenized_datasets.rename_column("label", "labels")
        tokenized_datasets.set_format("torch")

        # If the dataset was not already downloaded, then we'll record the download time now.
        if not self._dataset_already_downloaded:
            self._download_duration_sec = self._download_end - self._download_start

        # Prepare the data loaders
        self._train_loader = tokenized_datasets["train"]
        self._test_loader = tokenized_datasets["validation"]

        if self._dataset_already_downloaded:
            print(f"{IMDbName} dataset was already downloaded. Root directory: \"{self._root_dir}\"")
        else:
            print(f"{IMDbName} dataset was downloaded to root directory \"{self._root_dir}\" in "
                  f"{self._download_duration_sec} seconds.")

    @property
    def download_duration_sec(self)->float:
        return self._download_duration_sec

    @property
    def dataset_already_downloaded(self)->bool:
        return self._dataset_already_downloaded

    @property
    def download_start_time(self)->float:
        return self._download_start

    @property
    def download_end_time(self)->float:
        return self._download_end

    @property
    def download_duration(self)->float:
        return self._download_duration_sec

    @property
    def download_start(self)->float:
        return self._download_start

    @property
    def download_end(self)->float:
        return self._download_end

    @property
    def description(self)->dict[str, str|int|bool]:
        return {
            "name": self._name,
            "root_dir": self._root_dir,
            "shuffle": self._shuffle,
            "num_workers": self._num_workers,
        }

    @property
    def train_dataset(self):
        return datasets.IMDB(split='train', download=True, root=self._root_dir)

    @property
    def train_loader(self):
        return self._train_loader

    @property
    def test_dataset(self):
        return datasets.IMDB(split='test', download=True, root=self._root_dir)

    @property
    def test_loader(self):
        return self._test_loader