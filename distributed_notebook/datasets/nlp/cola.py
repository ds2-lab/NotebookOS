from typing import Dict, Union, Optional

from distributed_notebook.datasets.nlp.base import NLPDataset
from distributed_notebook.datasets.nlp.util import get_username

def cola_postprocess_tokenized_dataset(tokenized_datasets):
    tokenized_datasets = tokenized_datasets.remove_columns(["sentence", "idx"])
    tokenized_datasets = tokenized_datasets.rename_column("label", "labels")

    return tokenized_datasets

class CoLA(NLPDataset):
    root_directory: str = f"/home/{get_username()}/.cache/huggingface/datasets/glue/cola"

    # https://huggingface.co/datasets/nyu-mll/glue
    hugging_face_dataset_name: str = "glue"

    text_feature_column_name: str = "sentence"

    hugging_face_dataset_config_name: Optional[str] = "cola"

    def __init__(
            self,
            shuffle: bool = True,
            num_workers: int = 2,
            model_name: Optional[str] = None,
            max_token_length: int = 128,
            **kwargs,
    ):
        super().__init__(
            root_dir = CoLA.root_directory,
            model_name = model_name,
            shuffle = shuffle,
            num_workers = num_workers,
            hugging_face_dataset_name = CoLA.hugging_face_dataset_name,
            hugging_face_dataset_config_name = CoLA.hugging_face_dataset_config_name,
            text_feature_column_name = CoLA.text_feature_column_name,
            postprocess_tokenized_dataset = cola_postprocess_tokenized_dataset,
            max_token_length = max_token_length,
            tokenized_dataset_directory = CoLA.get_tokenized_dataset_directory(model_name),
        )

    @staticmethod
    def get_tokenized_dataset_directory(model_name: str)->str:
        return f'/home/{get_username()}/tokenized_datasets/glue/{model_name}'

    @property
    def description(self)->Dict[str, Union[str, int, bool]]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["hugging_face_dataset_name"] = CoLA.hugging_face_dataset_name
        return desc

    @property
    def name(self)->str:
        return "Corpus of Linguistic Acceptability (CoLA)"