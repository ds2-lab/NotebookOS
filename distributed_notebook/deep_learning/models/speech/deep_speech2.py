import gc
import time
from typing import Optional, Dict, Any, Type

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim

from distributed_notebook.deep_learning.configuration import Speech
from distributed_notebook.deep_learning.models.model import DeepLearningModel


class CNNLayerNorm(nn.Module):
    """Layer normalization built for cnns input"""

    def __init__(self, n_feats):
        super(CNNLayerNorm, self).__init__()
        self.layer_norm = nn.LayerNorm(n_feats)

    def forward(self, x):
        # x (batch, channel, feature, time)
        x = x.transpose(2, 3).contiguous()  # (batch, channel, time, feature)
        x = self.layer_norm(x)
        return x.transpose(2, 3).contiguous()  # (batch, channel, feature, time)


class ResidualCNN(nn.Module):
    """Residual CNN inspired by https://arxiv.org/pdf/1603.05027.pdf
        except with layer norm instead of batch norm
    """

    def __init__(self, in_channels, out_channels, kernel, stride, dropout, n_feats):
        super(ResidualCNN, self).__init__()

        self.cnn1 = nn.Conv2d(in_channels, out_channels, kernel, stride, padding=kernel // 2)
        self.cnn2 = nn.Conv2d(out_channels, out_channels, kernel, stride, padding=kernel // 2)
        self.dropout1 = nn.Dropout(dropout)
        self.dropout2 = nn.Dropout(dropout)
        self.layer_norm1 = CNNLayerNorm(n_feats)
        self.layer_norm2 = CNNLayerNorm(n_feats)

    def forward(self, x):
        residual = x  # (batch, channel, feature, time)
        x = self.layer_norm1(x)
        x = F.gelu(x)
        x = self.dropout1(x)
        x = self.cnn1(x)
        x = self.layer_norm2(x)
        x = F.gelu(x)
        x = self.dropout2(x)
        x = self.cnn2(x)
        x += residual
        return x  # (batch, channel, feature, time)


class BidirectionalGRU(nn.Module):

    def __init__(self, rnn_dim, hidden_size, dropout, batch_first):
        super(BidirectionalGRU, self).__init__()

        self.BiGRU = nn.GRU(
            input_size=rnn_dim, hidden_size=hidden_size,
            num_layers=1, batch_first=batch_first, bidirectional=True)
        self.layer_norm = nn.LayerNorm(rnn_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        x = self.layer_norm(x)
        x = F.gelu(x)
        x, _ = self.BiGRU(x)
        x = self.dropout(x)
        return x


class SpeechRecognitionModel(nn.Module):
    """Speech Recognition Model Inspired by DeepSpeech 2"""

    def __init__(self, n_cnn_layers, n_rnn_layers, rnn_dim, n_class, n_feats, stride=2, dropout=0.1):
        super(SpeechRecognitionModel, self).__init__()
        n_feats = n_feats // 2

        # cnn for extracting hierarchical features
        self.cnn = nn.Conv2d(1, 32, 3, stride=stride, padding=3 // 2)

        # n residual cnn layers with filter size of 32
        self.rescnn_layers = nn.Sequential(*[
            ResidualCNN(32, 32, kernel=3, stride=1, dropout=dropout, n_feats=n_feats)
            for _ in range(n_cnn_layers)
        ])
        self.fully_connected = nn.Linear(n_feats * 32, rnn_dim)
        self.birnn_layers = nn.Sequential(*[
            BidirectionalGRU(rnn_dim=rnn_dim if i == 0 else rnn_dim * 2,
                             hidden_size=rnn_dim, dropout=dropout, batch_first=i == 0)
            for i in range(n_rnn_layers)
        ])
        self.out = nn.Linear(rnn_dim, n_class)
        self.classifier = nn.Sequential(
            nn.Linear(rnn_dim * 2, rnn_dim),  # birnn returns rnn_dim*2
            nn.GELU(),
            nn.Dropout(dropout),
            self.out,
        )

    def forward(self, x):
        x = self.cnn(x)
        x = self.rescnn_layers(x)
        sizes = x.size()
        x = x.view(sizes[0], sizes[1] * sizes[2], sizes[3])  # (batch, feature, time)
        x = x.transpose(1, 2)  # (batch, time, feature)
        x = self.fully_connected(x)
        x = self.birnn_layers(x)
        x = self.classifier(x)
        return x


class DeepSpeech2(DeepLearningModel):
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

        learning_rate = 5e-4
        batch_size = 20
        epochs = 10,
        hparams = {
            "n_cnn_layers": 3,
            "n_rnn_layers": 5,
            "rnn_dim": 512,
            "n_class": out_features,
            "n_feats": num_features,
            "stride": 2,
            "dropout": 0.1,
            "learning_rate": learning_rate,
            "batch_size": batch_size,
            "epochs": epochs
        }

        self.model = SpeechRecognitionModel(hparams['n_cnn_layers'], hparams['n_rnn_layers'], hparams['rnn_dim'],
                                            hparams['n_class'], hparams['n_feats'], hparams['stride'],
                                            hparams['dropout'])

        if model_state_dict is not None:
            self.model.load_state_dict(model_state_dict)

        if optimizer is not None:
            self._optimizer = optimizer
        else:
            self._optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4)

        if optimizer_state_dict is not None:
            self._optimizer.load_state_dict(optimizer_state_dict)

    @staticmethod
    def category() -> str:
        return Speech

    @staticmethod
    def expected_model_class() -> Type:
        return SpeechRecognitionModel

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
                forward_pass_start: float = time.time()
                outputs = self.model(waveforms)
                outputs = F.log_softmax(outputs, dim=2)
                outputs = outputs.transpose(0, 1)  # (time, batch, n_class)

                # Compute loss
                loss = self._criterion(outputs, labels, input_lengths, label_lengths)
                loss.backward()

                self._optimizer.step()
                forward_pass_end: float = time.time()

                # Add this line to clear grad tensors
                self._optimizer.zero_grad(set_to_none=True)

                running_loss += loss.item()

                num_minibatches_processed += 1
                num_samples_processed += len(waveforms)

                self.log.debug(f"Processed {len(waveforms)} samples in {(forward_pass_end - forward_pass_start) * 1.0e3} milliseconds.")

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
            self.log.debug(f"Completed iteration through training dataset. Time elapsed: {time.time() - start_time} seconds.")

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
    def num_features(self) -> int:
        return self._num_features

    @property
    def name(self) -> str:
        return DeepSpeech2.model_name()

    @property
    def output_layer(self) -> nn.Module:
        return self.model.out

    @staticmethod
    def model_name() -> str:
        return "Deep Speech 2"

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
