from typing import Dict, Union, Optional

from distributed_notebook.datasets.nlp.base import NLPDataset

CoLAName:str = "Corpus of Linguistic Acceptability (CoLA)"

def cola_postprocess_tokenized_dataset(tokenized_datasets):
    tokenized_datasets = tokenized_datasets.remove_columns(["sentence", "idx"])
    tokenized_datasets = tokenized_datasets.rename_column("label", "labels")

    return tokenized_datasets

class CoLA(NLPDataset):
    root_directory: str = "~/.cache/huggingface/datasets/cola"

    # https://huggingface.co/datasets/nyu-mll/glue
    hugging_face_dataset_name: str = "glue"

    text_feature_column_name: str = "sentence"

    def __init__(
            self,
            shuffle: bool = True,
            num_workers: int = 2,
            model_name: Optional[str] = None,
            max_token_length: int = 128,
            **kwargs,
    ):
        super().__init__(
            name = CoLAName,
            root_dir = CoLA.root_directory,
            model_name = model_name,
            shuffle = shuffle,
            num_workers = num_workers,
            hugging_face_dataset_name = CoLA.hugging_face_dataset_name,
            hugging_face_dataset_config_name = "cola",
            text_feature_column_name = CoLA.text_feature_column_name,
            postprocess_tokenized_dataset = cola_postprocess_tokenized_dataset,
            max_token_length = max_token_length,
        )

    @property
    def description(self)->Dict[str, Union[str, int, bool]]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["hugging_face_dataset_name"] = CoLA.hugging_face_dataset_name
        return desc