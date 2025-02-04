import asyncio
import logging
import os
import pickle
import time
from typing import Dict
from typing import Optional, Any

from prometheus_client import Histogram

from distributed_notebook.logs import ColoredLogFormatter
from distributed_notebook.sync.log import SynchronizedValue, ExecutionCompleteNotification
from distributed_notebook.sync.storage.s3_provider import DEFAULT_S3_BUCKET_NAME, DEFAULT_AWS_S3_REGION, S3Provider
from distributed_notebook.sync.util import get_size


class S3Log(object):
    # This is the key at which we store the metadata for the S3Log so that we can recover it in the future.
    _metadata_key: str = "S3Log_Metadata"

    """
    S3Log is an implementation of SyncLog -- just like RaftLog -- but S3Log uses AWS S3 to persist checkpointed state.

    S3Log only supports single-replica scheduling policies. Scheduling policies with > 1 replica (e.g., static or
    dynamic) should use the RaftLog class.
    """

    def __init__(
            self,
            node_id: int = -1,
            bucket_name: str = DEFAULT_S3_BUCKET_NAME,
            aws_region: str = DEFAULT_AWS_S3_REGION,
            base_path: str = "/store",
            prometheus_enabled: bool = True,
            kernel_id: Optional[str] = None,
            workload_id: str = None,
            remote_storage_write_latency_milliseconds: Optional[Histogram] = None,
    ):
        assert node_id > 0
        assert aws_region != ""
        assert bucket_name != ""

        self.log: logging.Logger = logging.getLogger(
            __class__.__name__ + str(node_id)
        )
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        self._bucket_name: str = bucket_name
        self._aws_region: str = aws_region

        self._kernel_id: str = kernel_id
        self._node_id: int = node_id
        self._prometheus_enabled: bool = prometheus_enabled
        self._remote_storage_write_latency_ms: Optional[Histogram] = remote_storage_write_latency_milliseconds
        self._workload_id: Optional[str] = workload_id

        self.storage_provider: S3Provider = S3Provider(
            bucket_name=bucket_name,
            aws_region=aws_region,
        )

        # The full keys that we've written.
        self._keys_written: set[str] = set()

        # The names of the variables that we've written.
        self._variables_written: set[str] = set()

        self._base_path: str = base_path

        # Basically just the number of times we've written something to Redis.
        self._num_changes: int = 0
        self._term: int = 0

    @property
    def num_changes(self) -> int:  # type: ignore
        """The number of incremental changes since first set or the latest checkpoint."""
        return self._num_changes

    @property
    def term(self) -> int:  # type: ignore
        """Current term."""
        return self._term

    @property
    def current_election(self) -> Any:
        """
        :return: the current election, if one exists
        """
        return None

    @property
    def created_first_election(self) -> bool:
        """
        :return: return a boolean indicating whether we've created the first election yet.
        """
        return False

    def get_election(self, term_number: int) -> Any:
        """
        :return: the current election with the specified term number, if one exists.
        """
        return None

    def get_known_election_terms(self) -> Optional[list[int]]:
        """
        :return: a list of term numbers for which we have an associated Election object
        """
        return []

    def start(self, handler):
        """Register change handler, restore internel states, and start monitoring changes.
          handler will be in the form listerner(key, val: SyncValue)"""
        raise NotImplemented("Need to implement this.")

    def set_should_checkpoint_callback(self, callback):
        """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
          callback will be in the form callback(SyncLog) bool"""
        raise NotImplemented("Need to implement this.")

    def set_checkpoint_callback(self, callback):
        """Set the callback that will be called when the SyncLog decides to checkpoint.
          callback will be in the form callback(Checkpointer)."""
        raise NotImplemented("Need to implement this.")

    async def try_yield_execution(self, jupyter_message_id: str, term_number: int) -> bool:
        """Request yield the update of a term to another replica."""
        return False

    async def try_lead_execution(self, jupyter_message_id: str, term_number: int) -> bool:
        """Request to lead the update of a term. A following append call
           without leading status will fail."""
        return True

    async def set_election_waiter_ioloop(self, io_loop: asyncio.AbstractEventLoop, term_number: int):
        """
        Set the asyncio IOLoop that will be used when notifying the calling thread that the election of the
        specified term number has completed.
        """
        pass

    async def wait_for_election_to_end(self, term_number: int):
        """
        Wait until the leader of the specified election finishes executing the code,
        or until we know that all replicas yielded.

        :param term_number: the term number of the election
        """
        pass

    async def append_execution_end_notification(self, notification: ExecutionCompleteNotification):
        """
        Explicitly propose and append (to the synchronized Raft log) a ExecutionCompleteNotification object to
        signify that we've finished executing code in the current election.

        This function exists so that we can mock proposals of ExecutionCompleteNotification objects specifically,
        rather than mocking the more generic _serialize_and_append_value method.

        :param notification: the notification to be appended to the sync log
        """
        pass

    async def notify_execution_complete(self, term_number: int):
        """
        Notify our peer replicas that we have finished executing the code for the specified election.

        :param term_number: the term of the election for which we served as leader and executed
        the user-submitted code.
        """
        self._term += 1

        key: str = self.__get_path_for_metadata()

        val_size: int = get_size(self._variables_written)

        self.log.debug(f'Writing S3Log metadata for term {term_number} with {len(self._variables_written)} '
                       f'entries and total size of {val_size} bytes to Redis at key "{key}".')

        start_time: float = time.time()

        data: Dict[str, Any] | bytes = {
            "variable_names": self._variables_written,
            "s3_log_state": {
                "term": self._term,
                "workload_id": self._workload_id
            }
        }

        data = pickle.dumps(data)

        await self.storage_provider.write_value_async(key, data)
        time_elapsed: float = time.time() - start_time

        self.log.debug(f'Wrote S3Log metadata for term {term_number} with {len(self._variables_written)} '
                       f'entries and total size of {len(data)} bytes to Redis at key "{key}" '
                       f'in {round(time_elapsed * 1.0e3, 3)} ms.')

        if (self._prometheus_enabled and hasattr(self, "_remote_storage_write_latency_ms") and
                self._remote_storage_write_latency_ms is not None):
            self._remote_storage_write_latency_ms.labels(
                session_id=self._kernel_id, workload_id=self._workload_id
            ).observe(time_elapsed * 1e3)

    async def restore_namespace(self) -> Dict[str, Any]:
        """
        Retrieve metadata from remote storage and use the metadata to read all the keys from the namespace.

        This does NOT restore models/datasets from their pointers.

        :return: a dictionary mapping variable names to the variables.
        """
        raise NotImplemented("Implement me.")

    async def append(self, val: SynchronizedValue):
        """
        Append the difference of the value of specified key to the synchronization queue.
        """
        if val is None:
            raise ValueError("Cannot append 'None' value to S3Log.")

        if val.key is None:
            self.log.error(f"Attempting to append value with null key: {val}")
            raise ValueError("Cannot append value to S3Log with null key.")

        if val.key == "":
            self.log.error(f"Attempting to append value with empty string key: {val}")
            raise ValueError("Cannot append value to S3Log whose key is the empty string.")

        if val.key == S3Log._metadata_key:
            self.log.warning(f'Writing variable whose key is equal to "{S3Log._metadata_key}"')
            raise ValueError(f'Cannot append value to S3Log whose key is equal to "{val.key}".'
                             f'That is a reserved phrase.')

        key: str = os.path.join(self._base_path, val.key)

        val_size: int = get_size(val)
        self.log.debug(f'Writing value with size={val_size} bytes to Redis at key "{key}": {val}')

        start_time: float = time.time()

        data = pickle.dumps(val)

        await self.storage_provider.write_value_async(key, data)
        time_elapsed: float = time.time() - start_time

        self._num_changes += 1

        self._keys_written.add(key)
        self._variables_written.add(val.key)

        self.log.debug(f'Wrote value with size={len(data)} bytes to key "{key}" '
                       f'in {round(time_elapsed * 1.0e3, 3)} ms.')

        if (self._prometheus_enabled and hasattr(self, "_remote_storage_write_latency_ms") and
                self._remote_storage_write_latency_ms is not None):
            self._remote_storage_write_latency_ms.labels(
                session_id=self._kernel_id, workload_id=self._workload_id
            ).observe(time_elapsed * 1e3)

    async def close_async(self):
        self.log.debug("Closing S3Log.")
        await self.storage_provider.close_async()

    def close(self):
        """Ensure all async coroutines end and clean up."""
        self.log.debug("Closing S3Log.")
        self.storage_provider.close()

    def __get_path_for_metadata(self) -> str:
        """
        Generate and return the path to which we write our metadata.
        """
        return os.path.join(self._base_path, S3Log._metadata_key)
