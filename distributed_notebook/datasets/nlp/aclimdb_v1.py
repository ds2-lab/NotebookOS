import torch
from torch.utils.data.datapipes.datapipe import IterDataPipe
from torchtext import datasets
from torchtext.data.utils import get_tokenizer
from torchtext.vocab import build_vocab_from_iterator
from torch.utils.data import DataLoader

from distributed_notebook.datasets.base import CustomDataset

import time

IMDbName:str = "IMDb Large Movie Review Dataset"

class IMDbLargeMovieReview(CustomDataset):
    def __init__(self, root_dir:str = 'data', batch_size: int = 256, shuffle: bool = True, num_workers: int = 2, **kwargs):
        super().__init__(name = IMDbName, root_dir = root_dir, shuffle = shuffle, num_workers = num_workers)

        self._dataset_already_downloaded: bool = self._check_if_downloaded(
            filenames = datasets.CoLA.train_list + datasets.CIFAR10.test_list,
            base_folder = datasets.CIFAR10.base_folder
        )

        self._download_start = time.time()

        # Load IMDB Dataset
        train_datapipe, test_datapipe = datasets.IMDB(split=('train', 'test'), root=root_dir, download=True)
        # self._train_dataset: tuple[int, str] = datasets.IMDB(root=root_dir, split='train', download=True)
        # self._test_dataset: tuple[int, str] = datasets.IMDB(root=root_dir, split='test', download=True)

        # Tokenize the text
        self.tokenizer = get_tokenizer('basic_english')

        # Build the vocabulary
        def yield_tokens(data_iter):
            for _, text in data_iter:
                yield self.tokenizer(text)

        vocab = build_vocab_from_iterator(yield_tokens(train_datapipe), specials=["<unk>"])
        vocab.set_default_index(vocab["<unk>"])

        # Reload train_iter as it is exhausted during vocab building
        train_datapipe = datasets.IMDB(split='train', root=root_dir, download=True)
        test_datapipe = datasets.IMDB(split='test', root=root_dir, download=True)

        # Convert text to numerical indices
        def text_pipeline(x):
            return [vocab[token] for token in self.tokenizer(x)]

        label_pipeline = lambda x: int(x == "pos")

        def collate_batch(batch):
            label_list, text_list, offsets = [], [], [0]
            for (_label, _text) in batch:
                label_list.append(label_pipeline(_label))
                processed_text = torch.tensor(text_pipeline(_text), dtype=torch.int64)
                text_list.append(processed_text)
                offsets.append(processed_text.size(0))
            label_list = torch.tensor(label_list, dtype=torch.int64)
            offsets = torch.tensor(offsets[:-1]).cumsum(dim=0)
            text_list = torch.cat(text_list)
            return label_list, text_list, offsets

        self._download_end = time.time()
        self._download_duration_sec = self._download_end - self._download_start

        # Prepare the data loaders
        self._train_loader = DataLoader(train_datapipe, batch_size=64, collate_fn=collate_batch)
        self._test_loader = DataLoader(test_datapipe, batch_size=64, collate_fn=collate_batch)

        if self._dataset_already_downloaded:
            print(f"The {IMDbName} was already downloaded. Root directory: \"{root_dir}\"")
        else:
            print(f"The {IMDbName} was downloaded to root directory \"{root_dir}\" in {self._download_duration_sec} seconds.")

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