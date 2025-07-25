import gc
import logging
import time
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Type, List, Collection

import torch
import torch.nn as nn
from torch.nn import Module

from distributed_notebook.logs import ColoredLogFormatter, colors


class DeepLearningModel(ABC):
    def __init__(
            self,
            criterion: Module = None,
            criterion_state_dict: Optional[Dict[str, Any]] = None,
            out_features: int = 10,
            total_training_time_seconds: int = 0,
            total_num_epochs: int = 0,
            created_for_first_time: bool = False,
            gpu_device_ids: Optional[List[int]] = None,
            **kwargs,
    ):
        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)
        self.log.setLevel(logging.DEBUG)
        self.log.handlers.clear()
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        self._last_gpu_device_ids_used: List[int] = []
        """ The device IDs that were used by this model before the current device IDs. """

        self._device_ids: list[int] = []
        self.gpu_device = None
        self.cpu_device = torch.device('cpu')
        if torch.cuda.is_available():
            self.log.debug("CUDA is available.")

            if gpu_device_ids is not None and len(gpu_device_ids) > 0:
                self.log.debug(f"Creating initial cuda device using GPU {gpu_device_ids[0]}")
                self.gpu_device = torch.device(f'cuda:{gpu_device_ids[0]}')
                self._device_ids = list(gpu_device_ids)
            else:
                self.log.debug("Creating initial cuda device with non-specific GPU")
                self.gpu_device = torch.device('cuda')

            self.gpu_available: bool = True
        else:
            self.log.warning("CUDA is NOT available.")
            self.gpu_available: bool = False

        if criterion is not None:
            self._criterion: Optional[Module] = criterion
        else:
            self._criterion: Optional[Module] = nn.CrossEntropyLoss()

        if criterion_state_dict is not None:
            self._criterion.load_state_dict(criterion_state_dict)

        self.total_training_time_seconds: int = total_training_time_seconds
        self.total_num_epochs: int = total_num_epochs
        self.model: Optional[Module] = None
        self._optimizer: Optional[Module] = None
        self._out_features: int = out_features

        # Flag that is set to True everytime we train and set to False everytime we write the model's parameters or
        # state dictionary to remote storage.
        self._requires_checkpointing: bool = created_for_first_time

        # List of the times, in seconds, spent copying data from the CPU to the GPU.
        # IMPORTANT: These are NOT checkpointed as of right now.
        self._cpu2gpu_times: list[float] = []

        # List of the times, in seconds, spent copying data from the GPU to the CPU.
        # IMPORTANT: These are NOT checkpointed as of right now.
        self._gpu2cpu_times: list[float] = []

        for argument in kwargs:
            self.log.warning(f"Received unexpected key-word argument: '{argument}'")

    @staticmethod
    @abstractmethod
    def expected_model_class() -> Type:
        pass

    @staticmethod
    @abstractmethod
    def category() -> str:
        pass

    @staticmethod
    @abstractmethod
    def model_name() -> str:
        pass

    @property
    def last_gpu_device_ids_used(self) -> List[int]:
        """
        Returns the device IDs that were used by this model before the current device IDs.
        """
        return self._last_gpu_device_ids_used

    def checkpointed(self):
        """
        This should be called whenever the model's state dictionary is written to remote storage.
        """
        if not self._requires_checkpointing:
            raise ValueError(f"model '{self.name}' does not require checkpointing")

        self._requires_checkpointing = False

    @property
    @abstractmethod
    def constructor_args(self) -> dict[str, Any]:
        return {
            "out_features": self.out_features
        }

    @property
    def requires_checkpointing(self) -> bool:
        """
        Return a bool indicating whether this model has updated state that needs to be checkpointed.
        """
        return self._requires_checkpointing

    @requires_checkpointing.setter
    def requires_checkpointing(self, requires_checkpointing: bool):
        """
        Return a bool indicating whether this model has updated state that needs to be checkpointed.
        """
        self._requires_checkpointing = requires_checkpointing

    @property
    def cpu_to_gpu_times(self) -> list[float]:
        """
        :return: the list of the times, in seconds, spent copying data from the CPU to the GPU.
        """
        return self._cpu2gpu_times

    @property
    def gpu_to_cpu_times(self) -> list[float]:
        """
        :return: the list of the times, in seconds, spent copying data from the GPU to the CPU.
        """
        return self._gpu2cpu_times

    @property
    def optimizer(self) -> Optional[Module]:
        return self._optimizer

    @property
    def criterion(self) -> Optional[Module]:
        return self._criterion

    def apply_model_state_dict(self, model_state_dict: Dict[str, Any]):
        try:
            self.model.load_state_dict(model_state_dict)
        except Exception as ex:
            self.log.error(f"Failed to apply model state dictionary to model because: {ex}")
            raise ex  # re-raise

    def apply_optimizer_state_dict(self, optimizer_state_dict: Dict[str, Any]):
        try:
            self._optimizer.load_state_dict(optimizer_state_dict)
        except Exception as ex:
            self.log.error(f"Failed to apply optimizer state dictionary to model because: {ex}")
            raise ex  # re-raise

    def apply_criterion_state_dict(self, criterion_state_dict: Dict[str, Any]):
        try:
            self._criterion.load_state_dict(criterion_state_dict)
        except Exception as ex:
            self.log.error(f"Failed to apply criterion state dictionary to model because: {ex}")
            raise ex  # re-raise

    def test(self, loader):
        if self.gpu_available:
            self.to_gpu()
        else:
            self.to_gpu_simulated()

        self.model.eval()
        correct = 0
        total = 0
        test_loss = 0.0
        with torch.no_grad():
            for samples, labels in loader:
                if self.gpu_available:
                    samples, labels = samples.to(self.gpu_device), labels.to(self.gpu_device)
                    torch.cuda.synchronize()

                outputs = self.model(samples)
                loss = self._criterion(outputs, labels)
                test_loss += loss.item()

                # Calculate accuracy
                _, predicted = outputs.max(1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()

                if self.gpu_available:
                    del samples
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

    def train_epochs(self, loader, num_epochs: int = 1):
        """
        Train for a certain number of epochs.

        :return: a tuple where the first element is the actual training time, the second is the time copying the model
                 from the CPU to the GPU, and the third is the time spent copying the model from the GPU to the CPU.
        """
        copy_cpu2gpu_millis: float = 0.0
        copy_gpu2cpu_millis: float = 0.0
        training_time_millis: float = 0.0

        if self.gpu_available:
            self.to_gpu()
        else:
            self.to_gpu_simulated()

        if num_epochs <= 0:
            return training_time_millis, copy_cpu2gpu_millis, copy_gpu2cpu_millis

        self.log.debug(f"Training for {num_epochs} epoch(s).")

        self.model.train()
        start_time: float = time.time()

        running_loss = 0.0
        num_minibatches_processed: int = 0
        num_samples_processed: int = 0
        for epoch in range(0, num_epochs):
            self.log.debug(f"Training -- Epoch #{epoch + 1}/{num_epochs}")
            for elem in loader:
                if len(elem) == 2:
                    samples, labels = elem
                    attention_mask = None
                elif len(elem) == 3:
                    samples, attention_mask, labels = elem
                else:
                    raise ValueError(f"Unexpectedly received {len(elem)} item(s) from DataLoader: {elem}")

                if self.gpu_available:
                    samples, labels = samples.to(self.gpu_device), labels.to(self.gpu_device)

                    if attention_mask is not None:
                        attention_mask = attention_mask.to(self.gpu_device)

                    torch.cuda.synchronize()

                # Zero the parameter gradients
                self._optimizer.zero_grad()

                # Forward pass
                if attention_mask is not None:
                    outputs = self.model(samples, attention_mask=attention_mask, labels=labels)
                    loss = outputs.loss
                else:
                    outputs = self.model(samples)
                    loss = self._criterion(outputs, labels)

                # Backward pass and optimization
                loss.backward()
                self._optimizer.step()

                # Add this line to clear grad tensors
                self._optimizer.zero_grad(set_to_none=True)

                running_loss += loss.item()

                num_minibatches_processed += 1
                num_samples_processed += len(samples)

                if self.gpu_available:
                    del samples
                    del labels
                    del loss
                    del outputs

                    if attention_mask is not None:
                        del attention_mask

                    torch.cuda.synchronize()

            self.total_num_epochs += 1
            self.log.debug(
                f"Epoch {epoch + 1}/{num_epochs} finished. Time elapsed: {time.time() - start_time} seconds.")

        time_spent_training_sec: float = (time.time() - start_time)
        self.total_training_time_seconds += time_spent_training_sec
        training_time_millis: float = time_spent_training_sec * 1.0e3
        self.log.debug(f"Training completed. Number of epochs: {num_epochs}. "
                       f"Time elapsed: {training_time_millis:,} ms. "
                       f"Processed {num_minibatches_processed} mini-batches "
                       f"({num_samples_processed} individual samples).")

        if self.gpu_available:
            self.log.debug(f"Copying model from GPU device {self.gpu_device} to CPU.")
            copy_start: float = time.time()
            self.to_cpu()
            torch.cuda.synchronize()
            copy_end: float = time.time()
            self.log.debug(f"Copied model from GPU device {self.gpu_device} to CPU in {copy_gpu2cpu_millis} ms.")

            gc_start: float = time.time()
            gc.collect()
            with torch.no_grad():
                torch.cuda.empty_cache()
            torch.cuda.synchronize()
            gc_end: float = time.time()
            copy_gpu2cpu_millis = (copy_end - copy_start) * 1.0e3
            self.log.debug(f"Performed GPU garbage collection in {round((gc_end - gc_start) * 1.0e3, 3)} ms.")
        else:
            self.to_cpu_simulated()

        self._requires_checkpointing = True

        return training_time_millis, copy_cpu2gpu_millis, copy_gpu2cpu_millis

    def train(self, loader, target_training_duration_millis: int | float = 0.0) -> tuple[float, float, float]:
        """
        Train for a target amount of time.
        :return: a tuple where the first element is the actual training time, the second is the time copying the model
                 from the CPU to the GPU, and the third is the time spent copying the model from the GPU to the CPU.
        """
        copy_cpu2gpu_millis: float = 0.0
        copy_gpu2cpu_millis: float = 0.0
        true_training_time_ms: float = 0.0

        if self.gpu_available:
            self.to_gpu()
        else:
            self.to_gpu_simulated()

        if target_training_duration_millis <= 0:
            return true_training_time_ms, copy_cpu2gpu_millis, copy_gpu2cpu_millis

        target_training_duration_sec: float = target_training_duration_millis / 1.0e3

        self.log.debug(f"Training for {target_training_duration_millis} milliseconds.")

        self.model.train()
        start_time: float = time.time()

        running_loss = 0.0
        num_minibatches_processed: int = 0
        num_samples_processed: int = 0

        dataset_name: str = "N/A"
        if hasattr(loader, "dataset_name"):
            dataset_name = loader.dataset_name

        self.log.debug(f"Model '{self.name}' has {colors.LIGHT_GREEN}started training{colors.END} on dataset "
                       f"'{dataset_name}' on GPU(s): {colors.LIGHT_PURPLE}{self._device_ids}.{colors.END}")
        while ((time.time() - start_time) * 1.0e3) < target_training_duration_millis:
            for elem in loader:
                if len(elem) == 2:
                    samples, labels = elem
                    attention_mask = None
                elif len(elem) == 3:
                    samples, attention_mask, labels = elem
                else:
                    raise ValueError(f"Unexpectedly received {len(elem)} item(s) from DataLoader: {elem}")

                if self.gpu_available:
                    copy_st: float = time.time()
                    samples, labels = samples.to(self.gpu_device), labels.to(self.gpu_device)

                    if attention_mask is not None:
                        attention_mask = attention_mask.to(self.gpu_device)

                    torch.cuda.synchronize()
                    copy_et: float = time.time()

                    self.log.debug("Copied samples, labels, and possibly the attention_mask from CPU to "
                                   f"GPU device {self.gpu_device} in {round((copy_et - copy_st) * 1.0e3, 3):,} ms.")

                # Zero the parameter gradients
                self._optimizer.zero_grad()

                self.log.debug("Zeroed optimizer gradients. Beginning forward pass now.")

                forward_pass_start: float = time.time()

                # Forward pass
                if self.gpu_available:
                    outputs_st: float = time.time()
                    if attention_mask is not None:
                        outputs = self.model(samples, attention_mask=attention_mask, labels=labels)

                        self.log.debug(f"\tComputed outputs in {round((time.time() - outputs_st)*1.0e3, 3):,} ms. "
                                       f"Computing loss now.")

                        loss_st: float = time.time()
                        loss = outputs.loss
                    else:
                        outputs = self.model(samples)

                        self.log.debug(f"\tComputed outputs in {round((time.time() - outputs_st)*1.0e3, 3):,} ms. "
                                       f"Computing loss now.")

                        loss_st: float = time.time()
                        loss = self._criterion(outputs, labels)
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

                    # Simulate model outputs and labels
                    outputs = torch.randn(len(samples), self._out_features, requires_grad=True)  # Fake predictions

                    loss_st: float = time.time()

                    # Compute simulated loss
                    loss = self._criterion(outputs, labels)

                # Backward pass and optimization
                loss.backward()
                loss_et: float = time.time()
                self.log.debug(f"\tComputed loss in {round((loss_et - loss_st) * 1.0e3, 3):,} ms: {round(loss.item(), 3):,}")

                opt_step_st: float = time.time()
                self._optimizer.step()
                forward_pass_end: float = time.time()

                self.log.debug(f"\tPerformed optimizer step in "
                               f"{round((forward_pass_end - opt_step_st) * 1.0e3, 3):,} ms.")

                # Add this line to clear grad tensors
                self._optimizer.zero_grad(set_to_none=True)

                running_loss += loss.item()

                num_minibatches_processed += 1
                num_samples_processed += len(samples)
                now: float = time.time()
                time_elapsed_ms: float = (now - start_time) * 1.0e3

                self.log.debug(f"Processed {len(samples)} samples in "
                               f"{round((forward_pass_end - forward_pass_start) * 1.0e3, 9):,} ms. "
                               f"Time elapsed: {round(time_elapsed_ms, 3):,} / "
                               f"{round(target_training_duration_millis, 3)} ms "
                               f"({round((time_elapsed_ms / target_training_duration_millis) * 100.0, 2):,}%).\n")

                if self.gpu_available:
                    del samples
                    del labels
                    del loss
                    del outputs

                    if attention_mask is not None:
                        del attention_mask

                    gc.collect()
                    torch.cuda.empty_cache()
                    with torch.no_grad():
                        torch.cuda.empty_cache()

                    torch.cuda.synchronize()

                if ((time.time() - start_time) * 1.0e3) > target_training_duration_millis:
                    break

            self.total_num_epochs += 1
            self.log.debug(f"Completed iteration through training dataset. "
                           f"Time elapsed: {round(time.time() - start_time, 9)} seconds.")

        time_spent_training_sec: float = (time.time() - start_time)
        self.total_training_time_seconds += time_spent_training_sec
        true_training_time_ms: float = time_spent_training_sec * 1.0e3

        if self.gpu_available:
            self.log.debug(f"Copying model from GPU device {self.gpu_device} to CPU.")
            copy_start: float = time.time()
            self.to_cpu()
            torch.cuda.synchronize()
            copy_end: float = time.time()
            self.log.debug(f"Copied model from GPU device {self.gpu_device} to CPU in {copy_gpu2cpu_millis} ms.")

            gc_start: float = time.time()
            gc.collect()
            with torch.no_grad():
                torch.cuda.empty_cache()
            torch.cuda.synchronize()
            gc_end: float = time.time()

            copy_gpu2cpu_millis = (copy_end - copy_start) * 1.0e3
            self.log.debug(f"Performed GPU garbage collection in {round((gc_end - gc_start) * 1.0e3, 3)} ms.")
        else:
            self.to_cpu_simulated()

        self._requires_checkpointing = True

        if true_training_time_ms > target_training_duration_millis:
            self.log.debug(f"{colors.LIGHT_GREEN}Training completed.{colors.END} "
                           f"Target time: {target_training_duration_millis:,} ms. "
                           f"Time elapsed: {round(true_training_time_ms, 9):,} ms. "
                           f"{colors.YELLOW}Trained for "
                           f"{round(true_training_time_ms - target_training_duration_millis, 9)} "
                           f"ms too long.{colors.END} "
                           f"Processed {num_minibatches_processed} mini-batches ({num_samples_processed} samples).\n\n")
        else:
            self.log.debug(f"{colors.LIGHT_GREEN}Training completed.{colors.END} "
                           f"Target time: {target_training_duration_millis:,} ms. "
                           f"Time elapsed: {round(true_training_time_ms, 9):,} ms. "
                           f"Processed {num_minibatches_processed} mini-batches ({num_samples_processed} samples).\n\n")

        return true_training_time_ms, copy_cpu2gpu_millis, copy_gpu2cpu_millis

    @property
    def out_features(self) -> int:
        return self._out_features

    @property
    def state_dict(self) -> Optional[Dict[str, Any]]:
        if self.model is None:
            return None

        return self.model.state_dict()

    @property
    def optimizer_state_dict(self) -> Optional[Dict[str, Any]]:
        if self._optimizer is None:
            return None

        return self._optimizer.state_dict()

    @property
    def criterion_state_dict(self) -> Optional[Dict[str, Any]]:
        if self._criterion is None:
            return None

        return self._criterion.state_dict()

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def output_layer(self) -> nn.Module:
        pass

    @property
    def size_bytes(self) -> int:
        size_all_mb = self.size_mb
        return int(size_all_mb * 1.0e6)

    @property
    def size_mb(self) -> float:
        param_size = 0
        for param in self.model.parameters():
            param_size += param.nelement() * param.element_size()
        buffer_size = 0
        for buffer in self.model.buffers():
            buffer_size += buffer.nelement() * buffer.element_size()

        size_all_mb = (param_size + buffer_size) / 1024 ** 2
        return size_all_mb

    def to_gpu_simulated(self):
        simulated_copy_sec: float = self.size_mb / 1.3e3 # Based on empirical benchmarking
        time.sleep(simulated_copy_sec)
        self.log.debug(f"Finished moving {self.name} model, optimizer, and criterion from CPU to GPU 'simulated-cuda' "
                       f"in {round(simulated_copy_sec * 1.0e3, 3)} ms. Model size: {round(self.size_mb, 6)} MB.")
        self.cpu_to_gpu_times.append(simulated_copy_sec)

    def to_gpu(self) -> float:  # Return the total time elapsed in seconds.
        if self.gpu_device is None or not self.gpu_available:
            raise ValueError("GPU is unavailable. Cannot move {self.name} model, optimizer, and criterion to the GPU.")

        size_mb = self.size_mb
        self.log.debug(f"Moving {self.name} model, optimizer, and criterion to GPU device {self.gpu_device}. "
                       f"Model size: {round(size_mb, 6)} MB.")

        st: float = time.time()
        # Move the model to the GPU.
        self.model = self.model.to(self.gpu_device)
        et_model: float = time.time()

        # Move the optimizer back to GPU
        for state in self._optimizer.state.values():
            for k, v in state.items():
                if isinstance(v, torch.Tensor):
                    state[k] = v.to(self.gpu_device)

        et_optimizer: float = time.time()

        self._criterion = self._criterion.to(self.gpu_device)

        et_criterion: float = time.time()

        total_time_elapsed: float = et_criterion - st
        self.log.debug(f"Finished moving {self.name} model, optimizer, and criterion to GPU device {self.gpu_device} "
                       f"in {round(total_time_elapsed * 1.0e3, 9)} ms. Model size: {round(size_mb, 6)} MB.")
        self.log.debug(f"\t\tCopied model in {round((et_model - st) * 1.0e3, 9)} ms.")
        self.log.debug(f"\t\tCopied optimizer in {round((et_optimizer - et_model) * 1.0e3, 9)} ms.")
        self.log.debug(f"\t\tCopied criterion in {round((et_criterion - et_optimizer) * 1.0e3, 9)} ms.")

        self.cpu_to_gpu_times.append(total_time_elapsed)

        return total_time_elapsed

    def to_cpu_simulated(self):
        simulated_copy_sec: float = self.size_mb / 3.6e3 # Based on empirical benchmarking
        time.sleep(simulated_copy_sec)
        self.log.debug(f"Finished moving {self.name} model, optimizer, and criterion from GPU to CPU in "
                       f"{round(simulated_copy_sec * 1.0e3, 3)} ms. Model size: {round(self.size_mb, 6)} MB.")
        self.gpu_to_cpu_times.append(simulated_copy_sec)

    def to_cpu(self) -> float:  # Return the total time elapsed in seconds.
        size_mb: float = self.size_mb
        self.log.debug(f"Moving {self.name} model, optimizer, and criterion to the CPU device {self.cpu_device}. "
                       f"Model size: {round(size_mb, 6)} MB.")

        st: float = time.time()
        # Move the model to the CPU.
        self.model = self.model.to(self.cpu_device)
        et_model: float = time.time()

        # Move the optimizer back to CPU
        for state in self._optimizer.state.values():
            for k, v in state.items():
                if isinstance(v, torch.Tensor):
                    state[k] = v.cpu()

        et_optimizer: float = time.time()

        self._criterion = self._criterion.cpu()

        et_criterion: float = time.time()

        total_time_elapsed: float = et_criterion - st
        self.log.debug(f"Finished moving {self.name} model, optimizer, and criterion to CPU device {self.cpu_device} "
                       f"in {round(total_time_elapsed * 1.0e3, 9)} ms. Model size: {round(size_mb, 6)} MB.")
        self.log.debug(f"\t\tCopied model in {round((et_model - st) * 1.0e3, 9)} ms.")
        self.log.debug(f"\t\tCopied optimizer in {round((et_optimizer - et_model) * 1.0e3, 9)} ms.")
        self.log.debug(f"\t\tCopied criterion in {round((et_criterion - et_optimizer) * 1.0e3, 9)} ms.")

        self.gpu_to_cpu_times.append(total_time_elapsed)

        return total_time_elapsed

    def set_gpu_device_ids(self, device_ids: Collection[int] = None):
        """
        Change the GPU device IDs used by the model.

        If the model is wrapped in torch.nn.DataParallel, then this will first unwrap the model before
        either re-wrapping it in torch.nn.DataParallel, or just specifying a new PyTorch device with the
        specified GPU device ID.
        """
        if len(device_ids) == 0:
            raise ValueError("device IDs list is empty")

        if not isinstance(device_ids, list):
            device_ids = list(device_ids)

        st: float = time.time()

        # Unwrap from DataParallel (if in DataParallel).
        if isinstance(self.model, torch.nn.DataParallel):
            old_device_ids: list[int] = self.model.device_ids
            self.log.debug(f"Unwrapping model from DataParallel. Old GPU device IDs: {old_device_ids}.")
            self.model = self.model.module
        else:
            old_device_ids: list[int] = [0]

        # We only do this part if CUDA is actually available.
        if self.gpu_available:
            if len(device_ids) == 1:
                gpu_device_id: int = device_ids[0]
                self.log.debug(f"Using GPU #{gpu_device_id}")
                self.gpu_device = torch.device(f'cuda:{gpu_device_id}')

                self.model = self.model.to(self.gpu_device)
            else:
                self.log.debug(f"{colors.LIGHT_CYAN}Wrapping model in DataParallel.{colors.END} "
                               f"GPU device IDs: {colors.LIGHT_PURPLE}{device_ids}.{colors.END}")

                self.gpu_device = torch.device(f"cuda:{device_ids[0]}")
                self.model = torch.nn.DataParallel(self.model, device_ids=device_ids)

                self.model = self.model.to(self.gpu_device)

        et: float = time.time()
        self.log.debug(f"{colors.LIGHT_BLUE}Changed GPU device IDs{colors.END} from "
                       f"{colors.LIGHT_GRAY}{old_device_ids}{colors.END} to "
                       f"{colors.LIGHT_PURPLE}{device_ids}{colors.END} in {et - st} seconds. "
                       f"Current GPU device: {self.gpu_device}.")

        self._last_gpu_device_ids_used = self._device_ids.copy()
        self._device_ids = device_ids
