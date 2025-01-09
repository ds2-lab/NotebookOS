import os.path

from datasets import load_dataset, DownloadMode

from distributed_notebook.datasets.custom_dataset import CustomDataset

import time

from distributed_notebook.datasets.nlp.util import get_tokenizer

CoLAName:str = "Corpus of Linguistic Acceptability (CoLA)"

class CoLA(CustomDataset):
    def __init__(self, batch_size: int = 256, shuffle: bool = True, num_workers: int = 2, model_name:str = "", **kwargs):
        assert model_name is not None and model_name != ""

        super().__init__(
            name = CoLAName,
            root_dir = "~/.cache/huggingface/datasets/stanfordnlp___imdb",
            shuffle = shuffle,
            num_workers = num_workers
        )

        self._dataset_already_downloaded: bool = os.path.exists("~/.cache/huggingface/datasets/stanfordnlp___imdb")
        self._dataset_already_tokenized: bool = False

        # Download the dataset, or load it from the cache.
        self._download_start: float = time.time()
        self._dataset = load_dataset(
            path = "cola",
            download_mode = DownloadMode.REUSE_DATASET_IF_EXISTS,
        )
        self._download_end: float = time.time()

        if not self._dataset_already_downloaded:
            self._download_duration_sec: float = self._download_end - self._download_start
            print(f"The {CoLAName} dataset was downloaded to root directory \"{self._root_dir}\" in "
                  f"{self._download_duration_sec} seconds.")
        else:
            print(f"The {CoLAName} dataset was already downloaded. Root directory: \"{self._root_dir}\"")

        if not self._dataset_already_tokenized:
            self._tokenize_start: float = time.time()

            self.tokenizer = get_tokenizer(model_name)

            # Tokenization
            def tokenize_function(example):
                return self.tokenizer(example["sentence"], truncation=True, padding="max_length", max_length=128)

            tokenized_datasets = self._dataset.map(tokenize_function, batched=True)
            tokenized_datasets = tokenized_datasets.remove_columns(["sentence", "idx"])
            tokenized_datasets = tokenized_datasets.rename_column("label", "labels")
            tokenized_datasets.set_format("torch")

            self._tokenize_end: float = time.time()
            self._tokenize_duration: float = self._tokenize_end - self._tokenize_start

            print(f'The {CoLAName} dataset was tokenized and cached in directory "TBD" in '
                  f'{self._tokenize_duration} seconds.')

            # Prepare the data loaders
            self._train_loader = tokenized_datasets["train"]
            self._test_loader = tokenized_datasets["validation"]
        else:
            # TODO: Read the cached tokenized dataset.
            print(f"The {CoLAName} dataset was already tokenized.")

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
    def tokenization_start(self)->float:
        return self._tokenize_start

    @property
    def tokenization_end(self)->float:
        return self._tokenize_end

    @property
    def tokenization_duration_sec(self)->float:
        return self._tokenize_duration

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
        return self._train_dataset

    @property
    def train_loader(self):
        return self._train_loader

    @property
    def test_dataset(self):
        return self._test_dataset

    @property
    def test_loader(self):
        return self._test_loader