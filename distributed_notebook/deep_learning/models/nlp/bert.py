import tarfile
from typing import Optional, Dict, Any, Type

import time
import boto3
import os
import torch.nn as nn
import torch.optim as optim
from transformers import BertForSequenceClassification, BertLMHeadModel, BertPreTrainedModel

from distributed_notebook.deep_learning.models.model import DeepLearningModel
from distributed_notebook.deep_learning.configuration import NaturalLanguageProcessing
from .tasks import ClassificationTask, NLPTasks, LanguageModeling


class Bert(DeepLearningModel):
    s3_key: str = "models/models--bert-base-uncased.tar.gz"
    download_directory_path: str = os.path.expanduser("~/.cache/huggingface/hub")
    model_directory_name: str = "models--bert-base-uncased"

    def __init__(
            self,
            out_features: int = 10,
            optimizer: Optional[nn.Module] = None,
            optimizer_state_dict: Optional[Dict[str, Any]] = None,
            criterion: Optional[nn.Module] = None,
            criterion_state_dict: Optional[Dict[str, Any]] = None,
            model_state_dict: Optional[Dict[str, Any]] = None,
            created_for_first_time: bool = False,
            s3_bucket_name:str = "distributed-notebook-public",
            force_s3_download:bool = True,
            aws_region:str = "us-east-1",
            task: Optional[str] = ClassificationTask,
            **kwargs,
    ):
        super().__init__(
            criterion=criterion,
            criterion_state_dict=criterion_state_dict,
            out_features=out_features,
            created_for_first_time=created_for_first_time,
            **kwargs,
        )

        assert task in NLPTasks
        self._task: str = task

        if force_s3_download:
            self.download_from_s3(s3_bucket_name = s3_bucket_name, aws_region = aws_region)

        if self._task == ClassificationTask:
            self.model: BertForSequenceClassification = BertForSequenceClassification.from_pretrained("bert-base-uncased", num_labels=out_features)
            self._output_layer: nn.Module = self.model.classifier
        elif self._task == LanguageModeling:
            self.model: BertLMHeadModel = BertLMHeadModel.from_pretrained("bert-base-uncased")
            self._output_layer: nn.Module = self.model.cls
        else:
            raise ValueError(f'Unknown or unsupported task specified for GPT-2 model: "{self._task}"')

        if model_state_dict is not None:
            self.model.load_state_dict(model_state_dict)

        if optimizer is not None:
            self._optimizer = optimizer
        else:
            self._optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4)

        if optimizer_state_dict is not None:
            self._optimizer.load_state_dict(optimizer_state_dict)

    def download_from_s3(self, s3_bucket_name:str = "distributed-notebook-public", aws_region = "us-east-1"):
        # Check if already downloaded.
        if os.path.exists(os.path.join(self.download_directory_path, self.model_directory_name)):
            self.log.debug(f'"{self.model_name()}" model is already downloaded. No need to download from S3.')
            return

        self.log.debug(f'Downloading "{self.model_name()}" model from AWS S3 '
                       f'bucket "{s3_bucket_name}" at key "{self.s3_key}".')

        download_path:str = os.path.join(self.download_directory_path, self.s3_key)

        # Ensure the download directory exists
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        # Initialize the S3 client
        s3_client = boto3.client('s3', region_name=aws_region)

        # Download the file from S3
        self.log.debug(f'Downloading object with key "{self.s3_key}" from S3 bucket "{s3_bucket_name}"...')
        download_start: float = time.time()

        s3_client.download_file(s3_bucket_name, self.s3_key, download_path)

        self.log.debug(f'Downloaded object with key "{self.s3_key}" from S3 bucket "{s3_bucket_name}" '
                       f'in {time.time() - download_start:,} seconds.')

        # Extract the tar.gz file
        self.log.debug(f'Extracting downloaded file "{download_path}" to path "{self.download_directory_path}"...')
        extract_start_time: float = time.time()
        with tarfile.open(download_path, 'r:gz') as tar:
            tar.extractall(path=self.download_directory_path)

        extract_duration: float = time.time() - extract_start_time
        self.log.debug(f'Extracted downloaded file "{download_path}" to path '
                       f'"{self.download_directory_path}" in {extract_duration:,} seconds.')

        assert os.path.exists(download_path)

        # Optionally, remove the downloaded tar.gz file after extraction
        os.remove(download_path)
        self.log.debug(f'Removed downloaded file "{download_path}".')

        assert not os.path.exists(download_path)

    @staticmethod
    def expected_model_class() -> Type:
        return BertPreTrainedModel

    @staticmethod
    def category() -> str:
        return NaturalLanguageProcessing

    @staticmethod
    def model_name() -> str:
        return "BERT"

    @property
    def output_layer(self)->nn.Module:
        return self._output_layer

    @property
    def task(self) -> str:
        return self._task

    @property
    def constructor_args(self) -> dict[str, Any]:
        base_args: dict[str, Any] = super().constructor_args
        args: dict[str, Any] = {
            "task": self.task
        }
        base_args.update(args)
        return base_args

    @property
    def name(self) -> str:
        return Bert.model_name()

    def __str__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"

    def __repr__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"
