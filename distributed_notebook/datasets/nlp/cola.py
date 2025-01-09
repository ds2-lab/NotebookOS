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
            model_name:str = "",
            max_token_length: int = 128,
            **kwargs
    ):
        assert model_name is not None and model_name != ""

        self._model_name: str = model_name.lower()

        super().__init__(
            name = CoLAName,
            root_dir = CoLA.root_directory,
            shuffle = shuffle,
            num_workers = num_workers,
            hugging_face_dataset_name = CoLA.hugging_face_dataset_name,
            text_feature_column_name = CoLA.text_feature_column_name,
            postprocess_tokenized_dataset = cola_postprocess_tokenized_dataset,
            max_token_length = max_token_length,
        )

    @property
    def description(self)->dict[str, str|int|bool]:
        return {
            "name": self._name,
            "root_dir": self._root_dir,
            "shuffle": self._shuffle,
            "num_workers": self._num_workers,
            "model_name": self._model_name,
            "hugging_face_dataset_name": "glue",
        }