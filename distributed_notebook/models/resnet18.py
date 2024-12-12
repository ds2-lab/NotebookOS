import gc
import time
from typing import Optional, Dict, Any

import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import models

from distributed_notebook.models.model import DeepLearningModel

ResNet18Name = "ResNet-18"

class ResNet18(DeepLearningModel):
    def __init__(
            self,
            out_features: int = 10,
            optimizer: Optional[nn.Module] = None,
            optimizer_state_dict: Optional[Dict[str, Any]] = None,
            criterion: Optional[nn.Module] = None,
            criterion_state_dict: Optional[Dict[str, Any]] = None,
            model_state_dict: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name=ResNet18Name,
            criterion = criterion,
            criterion_state_dict = criterion_state_dict,
            out_features=out_features,
        )

        self.model = models.resnet18(pretrained=False)
        self.model.fc = nn.Linear(self.model.fc.in_features, out_features)  # Modify the fully connected layer for 10 classes

        if model_state_dict is not None:
            self.model.load_state_dict(model_state_dict)

        if optimizer is not None:
            self._optimizer = optimizer
        else:
            self._optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4)

        if optimizer_state_dict is not None:
            self._optimizer.load_state_dict(optimizer_state_dict)

    def apply_model_state_dict(self, model_state_dict: Dict[str, Any]):
        try:
            self.model.load_state_dict(model_state_dict)
        except Exception as ex:
            self.log.error(f"Failed to apply model state dictionary to model because: {ex}")
            raise ex # re-raise

    def apply_optimizer_state_dict(self, optimizer_state_dict: Dict[str, Any]):
        try:
            self._optimizer.load_state_dict(optimizer_state_dict)
        except Exception as ex:
            self.log.error(f"Failed to apply optimizer state dictionary to model because: {ex}")
            raise ex # re-raise

    def apply_criterion_state_dict(self, criterion_state_dict: Dict[str, Any]):
        try:
            self._criterion.load_state_dict(criterion_state_dict)
        except Exception as ex:
            self.log.error(f"Failed to apply criterion state dictionary to model because: {ex}")
            raise ex # re-raise

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
                self._optimizer.zero_grad()

                # Forward pass
                outputs = self.model(images)
                loss = self._criterion(outputs, labels)

                # Backward pass and optimization
                loss.backward()
                self._optimizer.step()

                # Add this line to clear grad tensors
                self._optimizer.zero_grad(set_to_none=True)

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
                loss = self._criterion(outputs, labels)
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