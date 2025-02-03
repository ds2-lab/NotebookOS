import logging

from distributed_notebook.logs import ColoredLogFormatter

class RedisLog(object):
    """
    RedisLog is an implementation of SyncLog -- just like RaftLog -- but S3Log uses Redis to persist checkpointed state.

    RedisLog only supports single-replica scheduling policies. Scheduling policies with > 1 replica (e.g., static or
    dynamic) should use the RaftLog class.
    """
    def __init__(
            self,
            node_id: int = -1,
            hostname: str = "",
            port: int = -1,
    ):
        assert node_id > 0
        assert port > 0
        assert hostname != ""

        self.log: logging.Logger = logging.getLogger(
            __class__.__name__ + str(node_id)
        )
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)