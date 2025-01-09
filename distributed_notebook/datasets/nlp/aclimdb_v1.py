from typing import Dict, Union, Optional

from distributed_notebook.datasets.nlp.base import NLPDataset
from distributed_notebook.datasets.nlp.util import get_username

IMDbName:str = "IMDb Large Movie Review Dataset"

def imdb_postprocess_tokenized_dataset(tokenized_datasets):
    tokenized_datasets = tokenized_datasets.remove_columns(["sentence", "idx"])
    tokenized_datasets = tokenized_datasets.rename_column("label", "labels")

    return tokenized_datasets

class IMDbLargeMovieReview(NLPDataset):
    root_directory: str = f"/home/{get_username()}/.cache/huggingface/datasets/stanfordnlp___imdb"

    # https://huggingface.co/datasets/stanfordnlp/imdb
    hugging_face_dataset_name: str = "stanfordnlp/imdb"

    text_feature_column_name: str = "sentence"

    def __init__(
            self,
            max_token_length: int = 256,
            shuffle: bool = True,
            num_workers: int = 2,
            model_name:Optional[str] = None,
            **kwargs,
    ):
        super().__init__(
            dataset_name = IMDbName,
            root_dir = IMDbLargeMovieReview.root_directory,
            model_name = model_name,
            shuffle = shuffle,
            num_workers = num_workers,
            hugging_face_dataset_name = IMDbLargeMovieReview.hugging_face_dataset_name,
            hugging_face_dataset_config_name = None,
            text_feature_column_name = IMDbLargeMovieReview.text_feature_column_name,
            postprocess_tokenized_dataset = imdb_postprocess_tokenized_dataset,
            max_token_length = max_token_length,
        )

    @property
    def description(self)->Dict[str, str|int|bool]:
        desc: Dict[str, Union[str, int, bool]] = super().description
        desc["hugging_face_dataset_name"] = IMDbLargeMovieReview.hugging_face_dataset_name
        return desc