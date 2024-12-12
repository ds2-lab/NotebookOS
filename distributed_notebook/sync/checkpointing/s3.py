from distributed_notebook.sync.checkpointing.checkpointer import RemoteCheckpointer

class S3Checkpointer(RemoteCheckpointer):
    def __init__(self):
        super().__init__()