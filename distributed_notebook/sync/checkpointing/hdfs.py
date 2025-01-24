from abc import ABC

from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer


class HdfsCheckpointer(RemoteCheckpointer, ABC):
    def __init__(self):
        super().__init__()

        raise ValueError("HDFS is not yet supported.")

    def storage_name(self)->str:
        return f"HDFS"