from typing import Optional, Dict, Any

import torch.nn as nn
import torch.optim as optim
from transformers import BertForSequenceClassification, BertLMHeadModel

from distributed_notebook.models.model import DeepLearningModel
from .tasks import ClassificationTask, NLPTasks, LanguageModeling

class Bert(DeepLearningModel):
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
            self.model = BertForSequenceClassification.from_pretrained("bert-base-uncased", num_labels=out_features)
        elif self._task == LanguageModeling:
            self.model = BertLMHeadModel.from_pretrained("bert-base-uncased")
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

    @property
    def task(self) -> str:
        return self._task

    @property
    def constructor_args(self) -> dict[str, Any]:
        base_args: dict[str, Any] = super(Bert).constructor_args
        args: dict[str, Any] = {
            "task": self.task
        }
        base_args.update(args)
        return base_args

    @property
    def name(self) -> str:
        return "BERT"

    def __str__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"

    def __repr__(self) -> str:
        return f"{self.name}[TotalTrainingTime={self.total_training_time_seconds}sec,TotalNumEpochs={self.total_num_epochs}]"
