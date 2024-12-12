from abc import ABC

from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer

class S3Checkpointer(RemoteCheckpointer, ABC):
    def __init__(self):
        super().__init__()

        raise ValueError("S3 is not yet supported.")