import json
import math
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import numpy as np
import sox
import torch
from torch.utils.data import Dataset, Sampler, DistributedSampler, DataLoader
import torchaudio

torchaudio.set_audio_backend("sox_io")

class DSRandomSampler(Sampler):
    """
    Implementation of a Random Sampler for sampling the dataset.
    Added to ensure we reset the start index when an epoch is finished.
    This is essential since we support saving/loading state during an epoch.
    """

    def __init__(self, dataset, batch_size=1):
        super().__init__(data_source=dataset)

        self.dataset = dataset
        self.start_index = 0
        self.epoch = 0
        self.batch_size = batch_size
        ids = list(range(len(self.dataset)))
        self.bins = [ids[i:i + self.batch_size] for i in range(0, len(ids), self.batch_size)]

    def __iter__(self):
        # deterministically shuffle based on epoch
        g = torch.Generator()
        g.manual_seed(self.epoch)
        indices = (
            torch.randperm(len(self.bins) - self.start_index, generator=g)
            .add(self.start_index)
            .tolist()
        )
        for x in indices:
            batch_ids = self.bins[x]
            np.random.shuffle(batch_ids)
            yield batch_ids

    def __len__(self):
        return len(self.bins) - self.start_index

    def set_epoch(self, epoch):
        self.epoch = epoch


class DSElasticDistributedSampler(DistributedSampler):
    """
    Overrides the ElasticDistributedSampler to ensure we reset the start index when an epoch is finished.
    This is essential since we support saving/loading state during an epoch.
    """

    def __init__(self, dataset, num_replicas=None, rank=None, batch_size=1):
        super().__init__(dataset=dataset, num_replicas=num_replicas, rank=rank)
        self.start_index = 0
        self.batch_size = batch_size
        ids = list(range(len(dataset)))
        self.bins = [ids[i:i + self.batch_size] for i in range(0, len(ids), self.batch_size)]
        self.num_samples = int(
            math.ceil(float(len(self.bins) - self.start_index) / self.num_replicas)
        )
        self.total_size = self.num_samples * self.num_replicas

    def __iter__(self):
        # deterministically shuffle based on epoch
        g = torch.Generator()
        g.manual_seed(self.epoch)
        indices = (
            torch.randperm(len(self.bins) - self.start_index, generator=g)
            .add(self.start_index)
            .tolist()
        )

        # add extra samples to make it evenly divisible
        indices += indices[: (self.total_size - len(indices))]
        assert len(indices) == self.total_size

        # subsample
        indices = indices[self.rank: self.total_size: self.num_replicas]
        assert len(indices) == self.num_samples
        for x in indices:
            batch_ids = self.bins[x]
            np.random.shuffle(batch_ids)
            yield batch_ids

    def __len__(self):
        return self.num_samples