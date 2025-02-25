import os
from typing import Dict, Union, Optional, Callable, Any

from distributed_notebook.deep_learning.data.nlp.base import NLPDataset
from distributed_notebook.deep_learning.configuration import NaturalLanguageProcessing

def imdb_truncated_postprocess_tokenized_dataset(tokenized_datasets, text_feature_column_name: str):
    tokenized_datasets = tokenized_datasets.remove_columns([text_feature_column_name])
    tokenized_datasets = tokenized_datasets.rename_column("label", "labels")

    return tokenized_datasets


class IMDbLargeMovieReviewTruncated(NLPDataset):
    default_root_directory: str = os.path.expanduser("~/.cache/huggingface/datasets/shawhin___imdb-truncated")

    # https://huggingface.co/datasets/shawhin/imdb-truncated
    hugging_face_dataset_name: str = "shawhin/imdb-truncated"

    text_feature_column_name: str = "text"

    hugging_face_dataset_config_name: Optional[str] = None

    def __init__(
            self,
            root_dir: str = default_root_directory,
            max_token_length: int = 256,
            shuffle: bool = True,
            num_workers: int = 2,
            model_name: Optional[str] = None,
            batch_size: int = 1,
            hugging_face_dataset_name: str = hugging_face_dataset_name,
            hugging_face_dataset_config_name: str = hugging_face_dataset_config_name,
            text_feature_column_name: str = text_feature_column_name,
            simulate_tokenization_overhead: float = 60.0,
            postprocess_tokenized_dataset: Callable[[Any, str], Any] = imdb_truncated_postprocess_tokenized_dataset,
            **kwargs
    ):
        super().__init__(
            root_dir=root_dir,
            model_name=model_name,
            shuffle=shuffle,
            num_workers=num_workers,
            hugging_face_dataset_name=hugging_face_dataset_name,
            hugging_face_dataset_config_name=hugging_face_dataset_config_name,
            text_feature_column_name=text_feature_column_name,
            postprocess_tokenized_dataset=postprocess_tokenized_dataset,
            max_token_length=max_token_length,
            tokenized_dataset_directory=IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory(model_name),
            batch_size=batch_size,
            force_s3_download=False,
            **kwargs,
        )

    @staticmethod
    def category() -> str:
        return NaturalLanguageProcessing

    @staticmethod
    def get_tokenized_dataset_directory(model_name: str) -> str:
        return os.path.expanduser(
            f"~/.cache/distributed_notebook/tokenized_datasets/shawhin___imdb-truncated/{model_name}")

    @property
    def description(self) -> Dict[str, str | int | bool]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["hugging_face_dataset_name"] = IMDbLargeMovieReviewTruncated.hugging_face_dataset_name
        desc["max_token_length"] = self._max_token_length
        return desc

    @staticmethod
    def dataset_name() -> str:
        return "Truncated IMDb Large Movie Review Dataset (Truncated IMDb)"

    @property
    def name(self) -> str:
        return IMDbLargeMovieReviewTruncated.dataset_name()

    @staticmethod
    def dataset_shortname()->str:
        return "imdb-truncated"

    @staticmethod
    def tokenized_dataset_root_directory()->str:
        return "shawhin___imdb-truncated"

    @staticmethod
    def huggingface_directory_name()->str:
        return "shawhin___imdb-truncated"