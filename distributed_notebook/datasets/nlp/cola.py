from torchtext import datasets
from torchtext.data.utils import get_tokenizer
from torchtext.vocab import build_vocab_from_iterator

from distributed_notebook.datasets.base import CustomDataset

import time

CoLAName:str = "Corpus of Linguistic Acceptability (CoLA)"

def label_pipeline(x):
    return int(x)

class CoLA(CustomDataset):
    def __init__(self, root_dir:str = 'data', batch_size: int = 256, shuffle: bool = True, num_workers: int = 2, **kwargs):
        super().__init__(name = CoLAName, root_dir = root_dir, shuffle = shuffle, num_workers = num_workers)

        self._dataset_already_downloaded: bool = self._check_if_downloaded(
            filenames = datasets.CoLA.train_list + datasets.CIFAR10.test_list,
            base_folder = datasets.CIFAR10.base_folder
        )

        self._download_start = time.time()
        self._train_dataset = datasets.CoLA(root=root_dir, train=True, download=True)

        # Load the dataset
        train_iter, valid_iter, test_iter = datasets.CoLA()

        # Tokenize the text
        self.tokenizer = get_tokenizer('basic_english')

        # Build the vocabulary
        def yield_tokens(data_iter):
            for _, label, text in data_iter:
                yield self.tokenizer(text)

        self.vocab = build_vocab_from_iterator(yield_tokens(train_iter), specials=["<unk>"])
        self.vocab.set_default_index(self.vocab["<unk>"])

        self._download_end = time.time()
        self._download_duration_sec = self._download_end - self._download_start

        # Prepare the data loaders
        self._train_loader = datasets.CoLA(split='train', text_transform=self.text_pipeline, label_transform=label_pipeline)
        self._test_loader = datasets.CoLA(split='test', text_transform=self.text_pipeline, label_transform=label_pipeline)

        if self._dataset_already_downloaded:
            print(f"{CoLAName} dataset was already downloaded. Root directory: \"{root_dir}\"")
        else:
            print(f"{CoLAName} dataset was downloaded to root directory \"{root_dir}\" in {self._download_duration_sec} seconds.")

    def text_pipeline(self, x):
        return self.vocab(self.tokenizer(x))

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