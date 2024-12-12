from distributed_notebook.sync.checkpointing.checkpointer import RemoteCheckpointer

class HdfsCheckpointer(RemoteCheckpointer):
    def __init__(self):
        super().__init__()