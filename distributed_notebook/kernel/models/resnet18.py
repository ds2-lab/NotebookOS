import gc
import time

import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import models

from distributed_notebook.kernel.models.model import DeepLearningModel


class RESNET18(DeepLearningModel):
    def __init__(self, out_features: int = 10, optimizer = None, criterion = None):
        super().__init__(criterion = criterion)

        self.model = models.resnet18(pretrained=False)
        self.model.fc = nn.Linear(self.model.fc.in_features, out_features)  # Modify the fully connected layer for 10 classes

        if optimizer is not None:
            self.optimizer = optimizer
        else:
            self.optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4)

    def train(self, loader, training_duration_millis: int|float = 0.0)->tuple[float, float, float]:
        """
        Train for a target amount of time.
        :return: a tuple where the first element is the actual training time, the second is the time copying the model
                 from the CPU to the GPU, and the third is the time spent copying the model from the GPU to the CPU.
        """
        copy_cpu2gpu_millis:float = 0.0
        copy_gpu2cpu_millis:float = 0.0
        training_time_millis: float = 0.0

        if self.gpu_available:
            st: float = time.time()
            self.to_gpu()
            et: float = time.time()
            copy_cpu2gpu_millis:float = (et - st) * 1.0e3
            self.log.debug(f"Copied model from CPU to GPU in {copy_cpu2gpu_millis} ms.")

        if training_duration_millis <= 0:
            return training_time_millis, copy_cpu2gpu_millis, copy_gpu2cpu_millis

        self.log.debug(f"Training for {training_duration_millis} milliseconds.")

        self.model.train()
        start_time: float = time.time()

        running_loss = 0.0
        num_minibatches_processed: int = 0
        num_images_processed:int = 0
        while ((time.time() - start_time) * 1.0e3) < training_duration_millis:
            for images, labels in loader:
                if self.gpu_available:
                    images, labels = images.to(self.gpu_device), labels.to(self.gpu_device)
                    torch.cuda.synchronize()

                # Zero the parameter gradients
                self.optimizer.zero_grad()

                # Forward pass
                outputs = self.model(images)
                loss = self.criterion(outputs, labels)

                # Backward pass and optimization
                loss.backward()
                self.optimizer.step()

                # Add this line to clear grad tensors
                self.optimizer.zero_grad(set_to_none=True)

                running_loss += loss.item()

                num_minibatches_processed += 1
                num_images_processed += len(images)

                if self.gpu_available:
                    del images
                    del labels
                    del loss
                    del outputs
                    torch.cuda.synchronize()

                if ((time.time() - start_time) * 1.0e3) > training_duration_millis:
                    break

            print(f"Completed iteration through training dataset. Time elapsed: {time.time() - start_time} seconds.")

        training_time_millis: float = (time.time() - start_time) * 1.0e3
        self.log.debug(f"Training completed. Target time: {training_duration_millis} ms. "
                       f"Time elapsed: {training_time_millis} ms. "
                       f"Processed {num_minibatches_processed} mini-batches ({num_images_processed} individual samples).")

        if self.gpu_available:
            self.log.debug("Copying model from GPU to CPU.")
            copy_start:float = time.time()
            self.to_cpu()

            gc.collect()
            with torch.no_grad():
                torch.cuda.empty_cache()
            torch.cuda.synchronize()
            copy_end:float = time.time()
            copy_gpu2cpu_millis = (copy_end - copy_start) * 1.0e3
            self.log.debug(f"Copied model from GPU to CPU in {copy_gpu2cpu_millis} ms.")

        return training_time_millis, copy_cpu2gpu_millis, copy_gpu2cpu_millis

    def __test(self, loader):
        if self.gpu_available:
            self.to_gpu()

        self.model.eval()
        correct = 0
        total = 0
        test_loss = 0.0
        with torch.no_grad():
            for images, labels in loader:
                if self.gpu_available:
                    images, labels = images.to(self.gpu_device), labels.to(self.gpu_device)
                torch.cuda.synchronize()

                outputs = self.model(images)
                loss = self.criterion(outputs, labels)
                test_loss += loss.item()

                # Calculate accuracy
                _, predicted = outputs.max(1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()

                if self.gpu_available:
                    del images
                    del labels
                    del loss
                    del outputs
                    del predicted
                    torch.cuda.synchronize()

        accuracy = 100.0 * correct / total

        if self.gpu_available:
            self.to_cpu()

            gc.collect()
            with torch.no_grad():
                torch.cuda.empty_cache()
            torch.cuda.synchronize()

        return test_loss / len(loader), accuracy