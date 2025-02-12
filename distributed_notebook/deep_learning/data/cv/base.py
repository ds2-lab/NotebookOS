import shutil
import time
from abc import ABC

from distributed_notebook.deep_learning.data.custom_dataset import CustomDataset


class ComputerVisionDataset(CustomDataset, ABC):
    """
    Provides shared implementations of the following methods for all Computer Vision (CV) datasets:
    - recorded_tokenization_overhead
    - set_recorded_tokenization_overhead
    - requires_tokenization
    - tokenization_start
    - tokenization_end
    - tokenization_duration_sec
    """
    def __init__(
            self,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)

    def remove_local_files(self):
        """
        Remove any local files on disk.
        """
        self.log.debug(f'Cleaning up cache files for "{self.name}" dataset. Removing directory "{self.root_directory}".')

        st: float = time.time()

        shutil.rmtree(self.root_directory, ignore_errors=True)

        self.log.debug(f'Successfully cleaned-up cache files for "{self.name}" '
                       f'dataset in {round(time.time() - st, 3):,} seconds.')

    @property
    def recorded_tokenization_overhead(self) -> bool:
        """
        Computer vision datasets do not need to be tokenized, so we always return True.
        :return:
        """
        return True

    def set_recorded_tokenization_overhead(self, val: bool = True):
        """
        This should be called by the kernel when it retrieves the tokenization overhead, as we only
        tokenize the dataset once. This flag lets us know that we've already recorded the tokenization
        overhead and should not re-record it again in the future.
        """
        # No-op
        pass

    @property
    def requires_tokenization(self) -> bool:
        return False

    @property
    def tokenization_start(self) -> float:
        return -1.0

    @property
    def tokenization_end(self) -> float:
        return -1.0

    @property
    def tokenization_duration_sec(self) -> float:
        return -1.0
