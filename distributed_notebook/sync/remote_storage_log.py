import logging
import os
import pickle
import time
from typing import Optional, Any, Dict, Collection, List, Callable

from prometheus_client import Histogram

from distributed_notebook.logs import ColoredLogFormatter
from distributed_notebook.sync.log import SynchronizedValue
from distributed_notebook.sync.remote_storage.error import InvalidKeyError
from distributed_notebook.sync.remote_storage.remote_storage_provider import RemoteStorageProvider
from distributed_notebook.sync.util import get_size


class RemoteStorageLog(object):
    """
    RemoteStorageLog is an implementation of SyncLog -- just like RaftLog -- but RemoteStorageLog uses
    an arbitrary remote remote_storage service to persist checkpointed state.

    Access to the remote remote_storage service is provided by an implementation of RemoteStorageProvider.

    RemoteStorageLog only supports single-replica scheduling policies. Scheduling policies with > 1 replica, such as
    static or dynamic, should use the RaftLog class.
    """

    # This is the key at which we store the metadata for the RemoteStorageLog so that we can recover it in the future.
    _metadata_key: str = "RemoteStorageLog_Metadata"

    def __init__(
            self,
            node_id: int = -1,
            remote_storage_provider: RemoteStorageProvider = None,
            base_path: str = "/store",
            prometheus_enabled: bool = True,
            kernel_id: Optional[str] = None,
            workload_id: str = None,
            remote_storage_write_latency_milliseconds: Optional[Histogram] = None,
    ):
        assert node_id > 0
        assert RemoteStorageProvider is not None

        self.log: logging.Logger = logging.getLogger(
            __class__.__name__ + str(node_id)
        )
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        self._kernel_id: str = kernel_id
        self._node_id: int = node_id
        self._prometheus_enabled: bool = prometheus_enabled
        self._remote_storage_write_latency_ms: Optional[Histogram] = remote_storage_write_latency_milliseconds
        self._workload_id: Optional[str] = workload_id

        self._storage_provider: RemoteStorageProvider = remote_storage_provider

        self._change_handler: Optional[Callable[[SynchronizedValue], None]] = None

        # The full keys that we've written.
        self._keys_written: set[str] = set()

        # The names of the variables that we've written.
        self._variables_written: set[str] = set()

        self._base_path: str = base_path

        # Basically just the number of times we've written something to remote remote_storage.
        self._num_changes: int = 0
        self._term: int = 0
        self._restore_namespace_time_seconds: float = 0.0

        start_time: float = time.time()
        self.variable_names_to_restore: set[str] = self.__load_and_apply_serialized_state()
        self._restoration_time_seconds: float = time.time() - start_time

    @property
    def restore_namespace_time_seconds(self)->float:
        """
        Return the time spent restoring the user namespace.
        """
        return self._restore_namespace_time_seconds

    def clear_restore_namespace_time_seconds(self):
        """
        Clear the 'restore_namespace_time_seconds' metric.
        """
        self._restore_namespace_time_seconds = 0

    @property
    def storage_provider(self) -> RemoteStorageProvider:
        return self._storage_provider

    @property
    def storage_name(self) -> str:
        return self._storage_provider.storage_name

    @property
    def workload_id(self) -> Optional[str]:
        return self._workload_id

    @workload_id.setter
    def workload_id(self, workload_id: Optional[str]):
        self._workload_id = workload_id

    @property
    def needs_to_catch_up(self)->bool:
        return len(self.variable_names_to_restore) > 0

    @property
    def restoration_time_seconds(self)->float:
        """
        Return the time spent on restoring previous state.
        """
        return self._restoration_time_seconds

    def clear_restoration_time(self):
        """
        Clear the 'restoration_time_seconds' metric.
        """
        self._restoration_time_seconds = 0.0

    async def catchup_with_peers(self)->None:
        """
        RemoteStorageLog uses the catchup_with_peers to restore the namespace from remote remote_storage.
        """
        await self.restore_namespace()

    @property
    def leader_id(self)->int:
        """
        Return the ID of the last/current leader node.
        """
        return self._node_id

    @property
    def leader_term(self)->int:
        """
        Return the term number of the last/current leader node.
        """
        return self._term

    async def remove_node(self, node_id):
        """Remove a node from the etcd-raft cluster."""
        self.log.warning(f"RemoteStorageLog just received instruction to remove node {node_id}.")
        # Not supported. Do nothing more.

    async def update_node(self, node_id, address):
        """Add a node to the etcd-raft  cluster."""
        self.log.warning(f"RemoteStorageLog just received instruction to update node {node_id} at address {address}.")
        # Not supported. Do nothing more.

    async def add_node(self, node_id, address):
        """
        Add a node to the etcd-raft cluster.

        NOTE: As of right now (5:39pm EST, Oct 11, 2024), this method is not actually used/called.

        Args:
            node_id: the ID of the node being added.
            address: the IP address of the node being added.
        """
        self.log.warning(f"RemoteStorageLog just received instruction to add node {node_id} at address {address}.")
        # Not supported. Do nothing more.

    @property
    def total_read_time_sec(self) -> float:
        """
        Read the total amount of time spent reading data in seconds.
        """
        return self.storage_provider.read_time

    @property
    def total_write_time_sec(self) -> float:
        """
        Read the total amount of time spent writing data in seconds.
        """
        return self.storage_provider.write_time

    @property
    def num_reads(self) -> int:  # type: ignore
        """The number of individual remote remote_storage reads."""
        return self.storage_provider.num_objects_read

    @property
    def num_changes(self) -> int:  # type: ignore
        """The number of incremental changes since first set or the latest checkpoint."""
        return self._num_changes

    @property
    def term(self) -> int:
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

    def get_election(self, term_number: int, jupyter_msg_id: Optional[str] = None)->Any:
        """
        Returns the election with the specified term number, if one exists.

        If the term number is given as -1, then resolution via the JupyterMessageID is attempted.
        """
        return None

    def get_known_election_terms(self) -> Optional[list[int]]:
        """
        :return: a list of term numbers for which we have an associated Election object
        """
        return []

    def start(self, handler):
        """
        Register change handler, restore internal states, and start monitoring changes.

        handler will be in the form listener(key, val: SyncValue)
        """
        self._change_handler = handler

    def set_should_checkpoint_callback(self, callback):
        """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
          callback will be in the form callback(SyncLog) bool"""
        pass

    def set_checkpoint_callback(self, callback):
        """Set the callback that will be called when the SyncLog decides to checkpoint.
          callback will be in the form callback(Checkpointer)."""
        pass

    async def try_yield_execution(self, jupyter_message_id: str, term_number: int) -> bool:
        """Request yield the update of a term to another replica."""
        return False

    async def try_lead_execution(self, jupyter_message_id: str, term_number: int) -> bool:
        """Request to lead the update of a term. A following append call
           without leading status will fail."""
        return True

    async def wait_for_election_to_end(self, term_number: int):
        """
        Wait until the leader of the specified election finishes executing the code,
        or until we know that all replicas yielded.

        :param term_number: the term number of the election
        """
        pass

    async def notify_execution_complete(self, term_number: int):
        """
        Notify our peer replicas that we have finished executing the code for the specified election.

        :param term_number: the term of the election for which we served as leader and executed
        the user-submitted code.
        """
        key: str = self.__get_path_for_metadata()

        val_size: int = get_size(self._variables_written)

        self.log.debug(f'Writing RemoteStorageLog metadata for term {term_number} with {len(self._variables_written)} '
                       f'entries and total size of {val_size} bytes to {self.storage_provider.storage_name} at key "{key}".')

        start_time: float = time.time()

        data: Dict[str, Any] | bytes = {
            "variable_names": self._variables_written,
            "remote_storage_log_state": {
                "term": self._term,
                "workload_id": self._workload_id
            }
        }

        data = pickle.dumps(data)

        await self.storage_provider.write_value_async(key, data, size_bytes = len(data))
        time_elapsed: float = time.time() - start_time

        self.log.debug(f'Wrote RemoteStorageLog metadata for term {term_number} with {len(self._variables_written)} '
                       f'entries and total size of {len(data)} bytes to {self.storage_name} at key "{key}" '
                       f'in {round(time_elapsed * 1.0e3, 3)} ms.')

        if (self._prometheus_enabled and hasattr(self, "_remote_storage_write_latency_ms") and
                self._remote_storage_write_latency_ms is not None):
            self._remote_storage_write_latency_ms.labels(
                session_id=self._kernel_id, workload_id=self.workload_id
            ).observe(time_elapsed * 1e3)

    def __load_and_apply_serialized_state(self)->set[str]:
        metadata_key: str = self.__get_path_for_metadata()

        try:
            data: Dict[str, Any] | bytes = self.storage_provider.read_value(metadata_key)
        except InvalidKeyError:
            self.log.debug(f'No data stored at metadata key "{metadata_key}". No state to load and apply.')
            return set()

        try:
            data = pickle.loads(data)
        except Exception as ex:
            self.log.error(f"Failed to unpickle RemoteStorageLog metadata: {ex}")
            raise ex  # Re-raise

        remote_storage_log_state: Dict[str, Any] = data["remote_storage_log_state"]
        variable_names: Collection[str] = data["variable_names"]

        self._term = remote_storage_log_state["term"]
        self._workload_id = remote_storage_log_state["workload_id"]

        self.log.debug(f'Restored term number {self._term} and workload ID "{self._workload_id}" from {self.storage_name}.')
        self.log.debug(f"Retrieved list of variable names with size={len(variable_names)} from {self.storage_name}.")

        return set(variable_names)

    async def restore_namespace(self) -> Dict[str, SynchronizedValue]:
        """
        Retrieve metadata from remote remote_storage and use the metadata to read all the keys from the namespace.

        This does NOT restore models/datasets from their pointers.

        :return: a dictionary mapping variable names to the variables.
        """
        if self.variable_names_to_restore is None:
            raise ValueError("Cannot restore namespace. 'Variables to Rename' value is None.")

        restored_namespace: Dict[str, SynchronizedValue] = {}

        if len(self.variable_names_to_restore) == 0:
            self.log.debug("There are no variables to restore.")
            return {}

        start_time: float = time.time()

        for variable_name in self.variable_names_to_restore:
            key: str = self.__get_key_from_variable_name(variable_name)

            self.log.debug(f'Retrieving value for variable "{variable_name}" from {self.storage_name} at key "{key}".')

            variable_value: bytes = self.storage_provider.read_value(key)

            try:
                variable: SynchronizedValue = pickle.loads(variable_value)
            except Exception as ex:
                self.log.error(f'Deserialization of variable "{variable_value}" failed: {ex}')
                raise ex  # Re-raise.

            restored_namespace[variable_name] = variable

            self.log.debug(f'Deserialized variable "{variable_name}" from {self.storage_name} at key "{key}".'
                           f'Passing SynchronizedValue to ChangeHandler now.')

            self._change_handler(variable)

        time_elapsed: float = time.time() - start_time
        self.log.debug(f"Restored {len(self.variable_names_to_restore)} variable(s) from {self.storage_name} "
                       f"in {round(time_elapsed * 1.0e3, 3):,} ms.")

        self._restore_namespace_time_seconds = time_elapsed

        return restored_namespace

    def __get_key(self, val: SynchronizedValue) -> str:
        return self.__get_key_from_variable_name(val.key)

    def __get_key_from_variable_name(self, name: str) -> str:
        return os.path.join(self._base_path, name)

    async def append(self, val: SynchronizedValue):
        """
        Append the difference of the value of specified key to the synchronization queue.
        """
        if val is None:
            raise ValueError("Cannot append 'None' value to RemoteStorageLog.")

        if val.key is None:
            self.log.error(f"Attempting to append value with null key: {val}")
            raise ValueError("Cannot append value to RemoteStorageLog with null key.")

        if val.key == "":
            self.log.error(f"Attempting to append value with empty string key: {val}")
            raise ValueError("Cannot append value to RemoteStorageLog whose key is the empty string.")

        if val.key == RemoteStorageLog._metadata_key:
            self.log.warning(f'Writing variable whose key is equal to "{RemoteStorageLog._metadata_key}"')
            raise ValueError(f'Cannot append value to RemoteStorageLog whose key is equal to "{val.key}".'
                             f'That is a reserved phrase.')

        key: str = self.__get_key(val)

        val_size: int = get_size(val)
        self.log.debug(f'Writing value with size={val_size} bytes to {self.storage_name} at key "{key}": {val}')

        start_time: float = time.time()

        data = pickle.dumps(val)

        await self.storage_provider.write_value_async(key, data, size_bytes = len(data))
        time_elapsed: float = time.time() - start_time

        self._num_changes += 1

        self._keys_written.add(key)
        self._variables_written.add(val.key)

        self.log.debug(f'Wrote value with size={len(data)} bytes to key "{key}" '
                       f'in {round(time_elapsed * 1.0e3, 3)} ms.')

        if (self._prometheus_enabled and hasattr(self, "_remote_storage_write_latency_ms") and
                self._remote_storage_write_latency_ms is not None):
            self._remote_storage_write_latency_ms.labels(
                session_id=self._kernel_id, workload_id=self.workload_id
            ).observe(time_elapsed * 1e3)

    async def close_async(self):
        self.log.debug("Closing RemoteStorageLog.")

        await self.storage_provider.close_async()

    def close_remote_storage_client(self) -> None:
        """
        Close the LogNode's RemoteStorage client.
        """
        self.storage_provider.close()

    def close(self):
        """Ensure all async coroutines end and clean up."""
        self.log.debug("Closing RemoteStorageLog.")
        self.close_remote_storage_client()

    def __get_path_for_metadata(self) -> str:
        """
        Generate and return the path to which we write our metadata.
        """
        return os.path.join(self._base_path, RemoteStorageLog._metadata_key)

    async def write_data_dir_to_remote_storage(
            self,
            last_resource_request: Optional[
                Dict[str, float | int | List[float] | List[int]]
            ] = None,
            remote_storage_definitions: Optional[Dict[str, Any]] = None,
    ):
        """
        Write the contents of the etcd-Raft data directory to RemoteStorage.

        This is not relevant for RemoteStorageLog.
        """
        pass

    async def is_election_voting_complete(self, jupyter_msg_id:str)->bool:
        """
        Check if an election for the given Jupyter msg ID already exists.

        If so, return True if the voting phase of the election is complete.

        The Jupyter msg id would come from an "execute_request" or a
        "yield_request" message.
        """
        pass # not supported when SMR is disabled

    async def is_election_execution_complete(self, jupyter_msg_id:str)->bool:
        """
        Check if an election for the given Jupyter msg ID already exists.

        If so, return True if the execution phase election is complete.

        The Jupyter msg id would come from an "execute_request" or a
        "yield_request" message.
        """
        pass # not supported when SMR is disabled