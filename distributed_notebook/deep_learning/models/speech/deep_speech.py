import gc
import time
from typing import Optional, Dict, Any, Type

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim

import torchaudio.models as models

from distributed_notebook.deep_learning.configuration import Speech
from distributed_notebook.deep_learning.models.model import DeepLearningModel

# NOT REALLY WORKING AS OF RIGHT NOW
class DeepSpeech(DeepLearningModel):
    """
    NOT REALLY WORKING AS OF RIGHT NOW
    """

    def __init__(
            self,
            in_features: int = 128,
            out_features: int = 29,
            optimizer: Optional[nn.Module] = None,
            optimizer_state_dict: Optional[Dict[str, Any]] = None,
            criterion: Optional[nn.Module] = None,
            criterion_state_dict: Optional[Dict[str, Any]] = None,
            model_state_dict: Optional[Dict[str, Any]] = None,
            created_for_first_time: bool = False,
            **kwargs,
    ):
        if criterion is None:
            criterion = nn.CTCLoss(blank=28)

        super().__init__(
            criterion=criterion,
            criterion_state_dict=criterion_state_dict,
            out_features=out_features,
            created_for_first_time=created_for_first_time,
            **kwargs,
        )

        self._in_features: int = in_features

        self.model = models.DeepSpeech(n_feature = in_features, n_class = out_features)
        self._output_layer: nn.Module = self.model.out

        if model_state_dict is not None:
            self.model.load_state_dict(model_state_dict)

        if optimizer is not None:
            self._optimizer = optimizer
        else:
            self._optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4)

        if optimizer_state_dict is not None:
            self._optimizer.load_state_dict(optimizer_state_dict)

    @staticmethod
    def model_name() -> str:
        return "Deep Speech"

    @staticmethod
    def category() -> str:
        return Speech

    @staticmethod
    def expected_model_class() -> Type:
        return models.DeepSpeech

    def train(self, loader, target_training_duration_millis: int | float = 0.0) -> tuple[float, float, float]:
        """
        Train for a target amount of time.
        :return: a tuple where the first element is the actual training time, the second is the time copying the model
                 from the CPU to the GPU, and the third is the time spent copying the model from the GPU to the CPU.
        """
        copy_cpu2gpu_millis: float = 0.0
        copy_gpu2cpu_millis: float = 0.0
        actual_training_time_millis: float = 0.0

        if self.gpu_available:
            st: float = time.time()
            self.to_gpu()
            et: float = time.time()
            copy_cpu2gpu_millis: float = (et - st) * 1.0e3
            self.log.debug(f"Copied model from CPU to GPU in {copy_cpu2gpu_millis} ms.")

        if target_training_duration_millis <= 0:
            return actual_training_time_millis, copy_cpu2gpu_millis, copy_gpu2cpu_millis

        self.log.debug(f"Training for {target_training_duration_millis} milliseconds.")

        self.model.train()
        start_time: float = time.time()

        running_loss = 0.0
        num_minibatches_processed: int = 0
        num_samples_processed: int = 0
        while ((time.time() - start_time) * 1.0e3) < target_training_duration_millis:
            for waveforms, labels, input_lengths, label_lengths in loader:
                if self.gpu_available:
                    waveforms, labels = waveforms.to(self.gpu_device), labels.to(self.gpu_device)
                    torch.cuda.synchronize()

                # Zero the parameter gradients
                self._optimizer.zero_grad()

                # Forward pass
                outputs = self.model(waveforms)
                outputs = F.log_softmax(outputs, dim=2)
                outputs = outputs.transpose(0, 1)  # (time, batch, n_class)

                # Compute loss
                loss = self._criterion(outputs, labels, input_lengths, label_lengths)
                loss.backward()

                self._optimizer.step()

                # Add this line to clear grad tensors
                self._optimizer.zero_grad(set_to_none=True)

                running_loss += loss.item()

                num_minibatches_processed += 1
                num_samples_processed += len(waveforms)

                if self.gpu_available:
                    del waveforms
                    del labels
                    del loss
                    del outputs
                    del input_lengths
                    del label_lengths

                    torch.cuda.synchronize()

                if ((time.time() - start_time) * 1.0e3) > target_training_duration_millis:
                    break

            self.total_num_epochs += 1
            print(f"Completed iteration through training dataset. Time elapsed: {time.time() - start_time} seconds.")

        time_spent_training_sec: float = (time.time() - start_time)
        self.total_training_time_seconds += time_spent_training_sec
        actual_training_time_millis: float = time_spent_training_sec * 1.0e3

        if actual_training_time_millis > target_training_duration_millis:
            self.log.debug(f"Training completed. Target time: {target_training_duration_millis:,} ms. "
                           f"Time elapsed: {actual_training_time_millis:,} ms. Trained for "
                           f"{actual_training_time_millis - target_training_duration_millis } ms too long. "
                           f"Processed {num_minibatches_processed} mini-batches ({num_samples_processed} samples).")
        else:
            self.log.debug(f"Training completed. Target time: {target_training_duration_millis:,} ms. "
                           f"Time elapsed: {actual_training_time_millis:,} ms. "
                           f"Processed {num_minibatches_processed} mini-batches ({num_samples_processed} samples).")

        if self.gpu_available:
            self.log.debug("Copying model from GPU to CPU.")
            copy_start: float = time.time()
            self.to_cpu()

            gc.collect()
            with torch.no_grad():
                torch.cuda.empty_cache()
            torch.cuda.synchronize()
            copy_end: float = time.time()
            copy_gpu2cpu_millis = (copy_end - copy_start) * 1.0e3
            self.log.debug(f"Copied model from GPU to CPU in {copy_gpu2cpu_millis} ms.")

        self._requires_checkpointing = True

        return actual_training_time_millis, copy_cpu2gpu_millis, copy_gpu2cpu_millis

    @property
    def in_features(self) -> int:
        return self._in_features

    @property
    def name(self) -> str:
        return DeepSpeech.model_name()

    @property
    def output_layer(self) -> nn.Module:
        return self._output_layer

    @property
    def constructor_args(self) -> dict[str, Any]:
        base_args: dict[str, Any] = super("Deep Speech v1").constructor_args
        args: dict[str, Any] = {
            "in_features": self.in_features
        }
        base_args.update(args)
        return base_args

    def __str__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"

    def __repr__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"
