from __future__ import print_function

import json
import os

import numpy as np

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim

from torch.autograd import Variable

import torchvision
import torchvision.datasets as datasets
import torchvision.transforms as transforms
import torchvision.models as models

import matplotlib.pyplot as plt
from itertools import islice

directory = "datasets/tiny-imagenet-200/"
num_classes = 200

# modify this depending on memory constraints
batch_size = 64

# the magic normalization parameters come from the example
transform_mean = np.array([ 0.485, 0.456, 0.406 ])
transform_std = np.array([ 0.229, 0.224, 0.225 ])

train_transform = transforms.Compose([
    transforms.RandomResizedCrop(224),
    # transforms.RandomHorizontalFlip(),
    # transforms.ToTensor(),
    # transforms.Normalize(mean = transform_mean, std = transform_std),
])

val_transform = transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    transforms.Normalize(mean = transform_mean, std = transform_std),
])

traindir = os.path.join(directory, "train")
# be careful with this set, the labels are not defined using the directory structure
valdir = os.path.join(directory, "val")

train = datasets.ImageFolder(traindir, train_transform)
val = datasets.ImageFolder(valdir, val_transform)

train_loader = torch.utils.data.DataLoader(train, batch_size=batch_size, shuffle=True)
val_loader = torch.utils.data.DataLoader(val, batch_size=batch_size, shuffle=True)

assert num_classes == len(train_loader.dataset.classes)

pre_trained_model = models.alexnet(pretrained=True)

