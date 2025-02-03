import asyncio
import logging
from typing import Tuple, Optional, Any

from distributed_notebook.logs import ColoredLogFormatter
from distributed_notebook.sync.log import SynchronizedValue, ExecutionCompleteNotification


class RedisLog(object):
    """
    RedisLog is an implementation of SyncLog -- just like RaftLog -- but RedisLog uses Redis to persist checkpointed state.

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

    @property
    def num_changes(self) -> int:  # type: ignore
        """The number of incremental changes since first set or the latest checkpoint."""

    @property
    def term(self) -> int:  # type: ignore
        """Current term."""

    @property
    def current_election(self)->Any:
        """
        :return: the current election, if one exists
        """
        self.log.warning("Current election requested from RedisLog. RedisLog does not support elections.")
        return None

    @property
    def created_first_election(self)->bool:
        """
        :return: return a boolean indicating whether we've created the first election yet.
        """
        return False

    def get_election(self, term_number: int)->Any:
        """
        :return: the current election with the specified term number, if one exists.
        """
        self.log.warning(f"Election with term={term_number} requested from RedisLog. RedisLog does not support elections.")
        return None

    def get_known_election_terms(self)->Optional[list[int]]:
        """
        :return: a list of term numbers for which we have an associated Election object
        """
        self.log.warning(f"Known elections requested from RedisLog. RedisLog does not support elections.")
        return []

    def start(self, handler):
        """Register change handler, restore internel states, and start monitoring changes.
          handler will be in the form listerner(key, val: SyncValue)"""

    def set_should_checkpoint_callback(self, callback):
        """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
          callback will be in the form callback(SyncLog) bool"""

    def set_checkpoint_callback(self, callback):
        """Set the callback that will be called when the SyncLog decides to checkpoint.
          callback will be in the form callback(Checkpointer)."""

    async def try_yield_execution(self, jupyter_message_id: str, term_number: int) -> bool:
        """Request yield the update of a term to another replica."""

    async def try_lead_execution(self, jupyter_message_id: str, term_number: int) -> bool:
        """Request to lead the update of a term. A following append call
           without leading status will fail."""

    async def set_election_waiter_ioloop(self, io_loop: asyncio.AbstractEventLoop, term_number: int):
        """
        Set the asyncio IOLoop that will be used when notifying the calling thread that the election of the
        specified term number has completed.
        """
        # No-op.
        self.log.warning("Attempted to set election waiter IO loop for RedisLog. RedisLog does not support elections.")

    async def wait_for_election_to_end(self, term_number: int):
        """
        Wait until the leader of the specified election finishes executing the code,
        or until we know that all replicas yielded.

        :param term_number: the term number of the election
        """
        raise ValueError(f"Cannot wait for election with term={term_number} to end; RedisLog does not support elections.")

    async def append_execution_end_notification(self, notification: ExecutionCompleteNotification):
        """
        Explicitly propose and append (to the synchronized Raft log) a ExecutionCompleteNotification object to
        signify that we've finished executing code in the current election.

        This function exists so that we can mock proposals of ExecutionCompleteNotification objects specifically,
        rather than mocking the more generic _serialize_and_append_value method.

        :param notification: the notification to be appended to the sync log
        """
        self.log.warning("Attempted to append ExecutionCompleteNotification to RedisLog. RedisLog does not support elections.")

    async def notify_execution_complete(self, term_number: int):
        """
        Notify our peer replicas that we have finished executing the code for the specified election.

        :param term_number: the term of the election for which we served as leader and executed
        the user-submitted code.
        """

    async def append(self, val: SynchronizedValue):
        """Append the difference of the value of specified key to the synchronization queue."""

    def sync(self, term):
        """Manually trigger the synchronization of changes since specified term."""

    def reset(self, term, logs: Tuple[SynchronizedValue]):
        """Clear logs equal and before specified term and replaced with specified logs"""

    def close(self):
        """Ensure all async coroutines end and clean up."""