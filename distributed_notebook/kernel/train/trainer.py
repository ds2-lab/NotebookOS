import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, transforms, models
from torch.utils.data import DataLoader

from tqdm import tqdm

from typing import Optional, Callable

import logging
import time

from distributed_notebook.logging import ColoredLogFormatter


def print_cuda_memory(
        action: Optional[Callable] = None,
        beforeHeader:Optional[str] = None,
        afterHeader: Optional[str] = None
):
    t = torch.cuda.get_device_properties(0).total_memory
    r = torch.cuda.memory_reserved(0)
    a = torch.cuda.memory_allocated(0)
    f = r-a  # free inside reserved

    prefix:str = ""
    if beforeHeader is not None:
        print("=================================")
        print(beforeHeader)
        prefix = "\t"

    print("---------------------------------")
    print(f"{prefix}Total memory: {t:,} bytes")
    print(f"{prefix}Memory reserved: {r:,} bytes")
    print(f"{prefix}Memory allocated: {a:,} bytes")
    print(f"{prefix}Free inside reserved: {f:,} bytes")
    print("---------------------------------")

    if action is not None:
        st: float  = time.time()
        res = action()
        et: float  = time.time()
        time_elapsed = et - st
    else:
        return None

    if afterHeader is not None:
        print(afterHeader % time_elapsed)
        prefix = "\t"

    print("---------------------------------")
    print(f"{prefix}Total memory: {t:,} bytes")
    print(f"{prefix}Memory reserved: {r:,} bytes")
    print(f"{prefix}Memory allocated: {a:,} bytes")
    print(f"{prefix}Free inside reserved: {f:,} bytes")
    print("---------------------------------")

    print("=================================\n")

    return res


class Trainer(object):
    def __init__(self):
        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        self.gpu_device = None
        self.cpu_device = torch.device('cpu')

        if torch.cuda.is_available():
            self.log.debug("CUDA is available.")
            self.gpu_device = torch.device('cuda')
        else:
            self.log.warning("CUDA is NOT available.")

        self.transform = transforms.Compose([
            transforms.RandomHorizontalFlip(),
            transforms.RandomCrop(32, padding=4),
            transforms.ToTensor(),
            transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010))  # CIFAR-10 mean and std
        ])
        self.train_dataset = datasets.CIFAR10(root='data', train=True, download=True, transform=self.transform)
        self.test_dataset = datasets.CIFAR10(root='data', train=False, download=True, transform=self.transform)

        self.train_loader = DataLoader(self.train_dataset, batch_size=64, shuffle=True, num_workers=2)
        self.test_loader = DataLoader(self.test_dataset, batch_size=64, shuffle=False, num_workers=2)

        # 4. Load a pre-trained model (ResNet18) and modify for CIFAR-10
        self.model = models.resnet18(pretrained=False)
        self.model.fc = nn.Linear(self.model.fc.in_features, 10)  # Modify the fully connected layer for 10 classes
        # self.model = self.model.to(device)

        self.criterion = nn.CrossEntropyLoss()
        self.optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4)

        self.training_epoch: int = 0

    def train(self):
        copied_to_gpu: bool = False
        if self.gpu_device is not None:
            def action():
                self.model = self.model.to(self.gpu_device)
            print_cuda_memory(
                action = action,
                beforeHeader = f"Copying model from the CPU to the GPU...",
                afterHeader = "Copied model from the CPU to the GPU in %.9f seconds."
            )
            copied_to_gpu = True

        self.model.train()
        running_loss = 0.0
        for images, labels in self.train_loader:
            if copied_to_gpu:
                images, labels = images.to(self.gpu_device), labels.to(self.gpu_device)

            # Zero the parameter gradients
            self.optimizer.zero_grad()

            # Forward pass
            outputs = self.model(images)
            loss = self.criterion(outputs, labels)

            # Backward pass and optimization
            loss.backward()
            self.optimizer.step()

            running_loss += loss.item()

            if copied_to_gpu:
                del images
                del labels

        if copied_to_gpu:
            self.model = self.model.to(self.cpu_device)

        print_cuda_memory(
            action = lambda: torch.cuda.empty_cache(),
            beforeHeader = "Preparing to empty CUDA cache.",
            afterHeader = "Emptied CUDA cache in %.9f seconds."
        )

        return running_loss / len(self.train_loader)

    # 7. Testing function
    def test(self):
        copied_to_gpu: bool = False
        if self.gpu_device is not None:
            self.model = self.model.to(self.gpu_device)

        self.model.eval()
        correct = 0
        total = 0
        test_loss = 0.0
        with torch.no_grad():
            for images, labels in self.test_loader:
                if copied_to_gpu:
                    images, labels = images.to(self.gpu_device), labels.to(self.gpu_device)

                outputs = self.model(images)
                loss = self.criterion(outputs, labels)
                test_loss += loss.item()

                # Calculate accuracy
                _, predicted = outputs.max(1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()

                if copied_to_gpu:
                    del images
                    del labels

        accuracy = 100.0 * correct / total

        if copied_to_gpu:
            self.model = self.model.to(self.cpu_device)

        print_cuda_memory(
            action = lambda: torch.cuda.empty_cache(),
            beforeHeader = "Preparing to empty CUDA cache.",
            afterHeader = "Emptied CUDA cache in %.9f seconds."
        )

        return test_loss / len(self.test_loader), accuracy

    def run(self, num_epochs:int = 4):
        print(f"Training for {num_epochs} epoch(s).")
        self.log.info(f"Training for {num_epochs} epoch(s).")
        for epoch in tqdm(range(0, num_epochs)):
            self.log.info(f"Epoch #{epoch}")
            self.training_epoch = epoch
            train_loss = self.train()
            test_loss, test_accuracy = self.test()

            print(f"Epoch [{epoch+1}/{num_epochs}]: "
                  f"Train Loss: {train_loss:.4f}, Test Loss: {test_loss:.4f}, Test Accuracy: {test_accuracy:.2f}%")

