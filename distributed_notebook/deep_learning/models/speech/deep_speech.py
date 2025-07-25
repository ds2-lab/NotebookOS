import math
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
            num_features: int = 128,
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

        self._num_features: int = num_features

        self.model = models.DeepSpeech(n_feature = num_features, n_class = out_features)
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

        target_training_duration_sec: float = target_training_duration_millis / 1.0e3

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
                if self.gpu_available:
                    output_st: float = time.time()

                    outputs = self.model(waveforms)
                    outputs = F.log_softmax(outputs, dim=2)
                    outputs = outputs.transpose(0, 1)  # (time, batch, n_class)

                    self.log.debug(f"\tComputed outputs in {round((time.time() - output_st)*1.0e3, 3):,} ms. "
                                   f"Computing loss now.")

                    loss_st: float = time.time()

                    # Compute loss
                    loss = self._criterion(outputs, labels, input_lengths, label_lengths)
                else:
                    sleep_interval_sec: float = target_training_duration_sec * 0.1

                    self.log.warning(f"\tSimulating training because CUDA is unavailable. "
                                     f"Sleeping for {round(sleep_interval_sec, 3):,f} seconds.")

                    # Simulate computation time for a batch
                    time.sleep(sleep_interval_sec)

                    update_param_start: float = time.time()

                    with torch.no_grad():  # Manually modify weights
                        for param in self.model.parameters():
                            # Random weight updates
                            param.data = param.data + 0.01 * torch.randn_like(param)

                    self.log.debug(f"\tUpdated parameters in {round((time.time() - update_param_start)*1.0e3, 3):,} ms.")

                    last_dim: int = waveforms.shape[-1]

                    # Simulate model outputs and labels
                    dim: int = int(math.ceil(last_dim / 2)) # Fake predictions
                    outputs = torch.randn(len(waveforms), dim, self._out_features, requires_grad=True)
                    outputs = F.log_softmax(outputs, dim=2)
                    outputs = outputs.transpose(0, 1)  # (time, batch, n_class)

                    loss_st: float = time.time()

                    # Compute simulated loss
                    loss = self._criterion(outputs, labels, input_lengths, label_lengths)

                loss.backward()
                self.log.debug(f"\tComputed loss in {round((time.time() - loss_st) * 1.0e3, 3):,} ms: {loss.item()}")

                self._optimizer.step()
                forward_pass_end: float = time.time()

                # Add this line to clear grad tensors
                self._optimizer.zero_grad(set_to_none=True)

                running_loss += loss.item()

                num_minibatches_processed += 1
                num_samples_processed += len(waveforms)

                self.log.debug(f"Processed {len(waveforms)} sample(s) in "
                               f"{round((forward_pass_end - forward_pass_start) * 1.0e3, 9):,} milliseconds. "
                               f"Total time elapsed so far: {round((time.time() - start_time) * 1.0e3, 9):,} milliseconds.")

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
            self.log.debug(f"Completed iteration through training dataset. "
                           f"Time elapsed: {round(time.time() - start_time, 9)} seconds.")

        time_spent_training_sec: float = (time.time() - start_time)
        self.total_training_time_seconds += time_spent_training_sec
        actual_training_time_millis: float = time_spent_training_sec * 1.0e3

        if actual_training_time_millis > target_training_duration_millis:
            self.log.debug(f"Training completed. Target time: {target_training_duration_millis:,} ms. "
                           f"Time elapsed: {round(actual_training_time_millis, 9):,} ms. Trained for "
                           f"{round(actual_training_time_millis - target_training_duration_millis, 9):,} ms too long. "
                           f"Processed {num_minibatches_processed} mini-batch(es); {num_samples_processed} sample(s).")
        else:
            self.log.debug(f"Training completed. Target time: {target_training_duration_millis:,} ms. "
                           f"Time elapsed: {round(actual_training_time_millis, 9):,} ms. "
                           f"Processed {num_minibatches_processed} mini-batch(es); {num_samples_processed} sample(s).")

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
    def num_features(self) -> int:
        return self._num_features

    @property
    def name(self) -> str:
        return DeepSpeech.model_name()

    @property
    def output_layer(self) -> nn.Module:
        return self._output_layer

    @property
    def constructor_args(self) -> dict[str, Any]:
        base_args: dict[str, Any] = super().constructor_args
        args: dict[str, Any] = {
            "num_features": self.num_features
        }
        base_args.update(args)
        return base_args

    def __str__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"

    def __repr__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"
