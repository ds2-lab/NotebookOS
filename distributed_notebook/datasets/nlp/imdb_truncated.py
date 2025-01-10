from typing import Dict, Union, Optional

from distributed_notebook.datasets.nlp.base import NLPDataset
from distributed_notebook.datasets.nlp.util import get_username

def imdb_truncated_postprocess_tokenized_dataset(tokenized_datasets, text_feature_column_name: str):
    tokenized_datasets = tokenized_datasets.remove_columns([text_feature_column_name])
    tokenized_datasets = tokenized_datasets.rename_column("label", "labels")

    return tokenized_datasets

class IMDbLargeMovieReviewTruncated(NLPDataset):
    root_directory: str = f"/home/{get_username()}/.cache/huggingface/datasets/shawhin___imdb-truncated"

    # https://huggingface.co/datasets/shawhin/imdb-truncated
    hugging_face_dataset_name: str = "shawhin/imdb-truncated"

    text_feature_column_name: str = "text"

    hugging_face_dataset_config_name: Optional[str] = None

    def __init__(
            self,
            max_token_length: int = 256,
            shuffle: bool = True,
            num_workers: int = 2,
            model_name:Optional[str] = None,
            batch_size = 32,
    ):
        super().__init__(
            root_dir = IMDbLargeMovieReviewTruncated.root_directory,
            model_name = model_name,
            shuffle = shuffle,
            num_workers = num_workers,
            hugging_face_dataset_name = IMDbLargeMovieReviewTruncated.hugging_face_dataset_name,
            hugging_face_dataset_config_name = IMDbLargeMovieReviewTruncated.hugging_face_dataset_config_name,
            text_feature_column_name = IMDbLargeMovieReviewTruncated.text_feature_column_name,
            postprocess_tokenized_dataset = imdb_truncated_postprocess_tokenized_dataset,
            max_token_length = max_token_length,
            tokenized_dataset_directory = IMDbLargeMovieReviewTruncated.get_tokenized_dataset_directory(model_name),
            batch_size = batch_size,
        )

    @staticmethod
    def get_tokenized_dataset_directory(model_name: str)->str:
        return f'/home/{get_username()}/tokenized_datasets/shawhin___imdb-truncated/{model_name}'

    @property
    def description(self)->Dict[str, str|int|bool]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["hugging_face_dataset_name"] = IMDbLargeMovieReviewTruncated.hugging_face_dataset_name
        desc["max_token_length"] = self._max_token_length
        return desc

    @property
    def name(self)->str:
        return "IMDb Large Movie Review Dataset (Truncated)"