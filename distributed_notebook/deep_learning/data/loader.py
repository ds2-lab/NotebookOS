from torch.utils.data import DataLoader

value_error_contents = "cannot load dataset because dataset description does not contain a '%s' key"


class WrappedLoader(DataLoader):
    """
    Wrapper around torch.utils.data.DataLoader that also has a field for the name of the associated dataset.
    """
    def __init__(self, *args, **kwargs):
        if "dataset_name" in kwargs:
            self._dataset_name: str = kwargs.pop("dataset_name")
        else:
            self._dataset_name: str = "N/A"

        super().__init__(*args, **kwargs)

    @property
    def dataset_name(self) -> str:
        return self._dataset_name
