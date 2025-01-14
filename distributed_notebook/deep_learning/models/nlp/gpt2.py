from typing import Optional, Dict, Any, Type

import torch.nn as nn
import torch.optim as optim
from transformers import GPT2ForSequenceClassification, GPT2LMHeadModel, GPT2PreTrainedModel

from distributed_notebook.deep_learning.models.model import DeepLearningModel
from distributed_notebook.deep_learning.configuration import NaturalLanguageProcessing
from .tasks import ClassificationTask, NLPTasks, LanguageModeling


class GPT2(DeepLearningModel):
    def __init__(
            self,
            out_features: int = 10,
            optimizer: Optional[nn.Module] = None,
            optimizer_state_dict: Optional[Dict[str, Any]] = None,
            criterion: Optional[nn.Module] = None,
            criterion_state_dict: Optional[Dict[str, Any]] = None,
            model_state_dict: Optional[Dict[str, Any]] = None,
            created_for_first_time: bool = False,
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

        if self._task == ClassificationTask:
            self.model: GPT2ForSequenceClassification = GPT2ForSequenceClassification.from_pretrained("gpt2", num_labels=out_features)
            self._output_layer: nn.Module = self.model.score
        elif self._task == LanguageModeling:
            self.model: GPT2LMHeadModel = GPT2LMHeadModel.from_pretrained("gpt2")
            self._output_layer: nn.Module = self.model.lm_head
        else:
            raise ValueError(f'Unknown or unsupported task specified for GPT-2 model: "{self._task}"')

        # Set the pad token of the model's configuration.
        self.model.config.pad_token_id = self.model.config.eos_token_id

        if model_state_dict is not None:
            self.model.load_state_dict(model_state_dict)

        if optimizer is not None:
            self._optimizer = optimizer
        else:
            self._optimizer = optim.SGD(self.model.parameters(), lr=0.01, momentum=0.9, weight_decay=5e-4)

        if optimizer_state_dict is not None:
            self._optimizer.load_state_dict(optimizer_state_dict)

    @staticmethod
    def expected_model_class() -> Type:
        return GPT2PreTrainedModel

    @staticmethod
    def category() -> str:
        return NaturalLanguageProcessing

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
        return GPT2.model_name()

    @staticmethod
    def model_name() -> str:
        return "GPT-2"

    def __str__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"

    def __repr__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"
