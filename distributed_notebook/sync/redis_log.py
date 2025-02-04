import asyncio
import time
import pickle
import os
import logging
from typing import Optional, Any

from distributed_notebook.deep_learning import DeepLearningModel
from distributed_notebook.logs import ColoredLogFormatter
from distributed_notebook.sync.checkpointing.pointer import ModelPointer
from distributed_notebook.sync.log import SynchronizedValue

import redis
import redis.asyncio as async_redis

from distributed_notebook.sync.util import get_size


class RedisLog(object):
    _metadata_key:str = "RedisLog_Metadata"

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
            db: int = 0,
            password: Optional[str] = None,
            additional_redis_args: Optional[dict] = None,
            base_path: str = "/store",
            prometheus_enabled: bool = True,
            kernel_id: Optional[str] = None,
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

        self._kernel_id: str = kernel_id
        self._node_id: int = node_id
        self._prometheus_enabled: bool = prometheus_enabled

        self._hostname: str = hostname
        self._port: int = port
        self._db: int = db
        self._password: str = password
        self._additional_redis_args: Optional[dict] = additional_redis_args

        self._async_redis = async_redis.Redis(host = hostname, port = port, db = db, password = password, **additional_redis_args)
        self._redis = redis.Redis(host = hostname, port = port, db = db, password = password, **additional_redis_args)

        self._total_read_time_sec: float = 0
        self._total_write_time_sec: float = 0

        # The full keys that we've written.
        self._keys_written: set[str] = set()

        # The names of the variables that we've written.
        self._variables_written: set[str] = set()

        self._base_path: str = base_path

        # Basically just the number of times we've written something to Redis.
        self._num_changes: int = 0
        self._num_reads: int = 0
        self._term: int = 0

    @property
    def hostname(self) -> str:
        return self._hostname

    @property
    def total_read_time_sec(self) -> float:
        """
        Read the total amount of time spent reading data in seconds.
        """
        return self._total_read_time_sec

    @property
    def total_write_time_sec(self) -> float:
        """
        Read the total amount of time spent writing data in seconds.
        """
        return self._total_write_time_sec

    @property
    def port(self) -> int:
        return self._port

    @property
    def num_reads(self) -> int:  # type: ignore
        """The number of individual remote storage reads."""
        return self._num_reads

    @property
    def num_changes(self) -> int:  # type: ignore
        """The number of incremental changes since first set or the latest checkpoint."""
        return self._num_changes

    @property
    def term(self) -> int:
        """Current term."""
        return self._term

    @property
    def current_election(self)->Any:
        """
        :return: the current election, if one exists
        """
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
        return None

    def get_known_election_terms(self)->Optional[list[int]]:
        """
        :return: a list of term numbers for which we have an associated Election object
        """
        return []

    def start(self, handler):
        """
        Register change handler, restore internal states, and start monitoring changes.

        handler will be in the form listener(key, val: SyncValue)
        """
        pass

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
        redis_key: str = self.__get_path_for_metadata()

        val_size: int = get_size(self._variables_written)

        self.log.debug(f'Writing RedisLog metadata for term {term_number} with {len(self._variables_written)} '
                       f'entries and total size of {val_size} bytes to Redis at key "{redis_key}".')

        start_time: float = time.time()

        data = pickle.dumps(self._variables_written)

        await self._async_redis.set(redis_key, data)
        time_elapsed: float = time.time() - start_time

        self._total_write_time_sec += time_elapsed

        self.log.debug(f'Wrote RedisLog metadata for term {term_number} with {len(self._variables_written)} '
                       f'entries and total size of {len(data)} bytes to Redis at key "{redis_key}" '
                       f'in {round(time_elapsed * 1.0e3, 3)} ms.')

    async def append(self, val: SynchronizedValue):
        """
        Append the difference of the value of specified key to the synchronization queue.
        """
        if val is None:
            raise ValueError("Cannot append 'None' value to RedisLog.")

        if val.key is None:
            self.log.error(f"Attempting to append value with null key: {val}")
            raise ValueError("Cannot append value to RedisLog with null key.")

        if val.key == "":
            self.log.error(f"Attempting to append value with empty string key: {val}")
            raise ValueError("Cannot append value to RedisLog whose key is the empty string.")

        if val.key == RedisLog._metadata_key:
            self.log.warning(f'Writing variable whose key is equal to "{RedisLog._metadata_key}"')
            raise ValueError(f'Cannot append value to RedisLog whose key is equal to "{val.key}".'
                             f'That is a reserved phrase.')
        redis_key: str = os.path.join(self._base_path, val.key)

        val_size: int = get_size(val)
        self.log.debug(f'Writing value with size={val_size} bytes to Redis at key "{redis_key}": {val}')

        start_time: float = time.time()

        data = pickle.dumps(val)

        await self._async_redis.set(redis_key, data)
        time_elapsed: float = time.time() - start_time

        self._num_changes += 1
        self._total_write_time_sec += time_elapsed
        self._keys_written.add(redis_key)
        self._variables_written.add(val.key)

        self.log.debug(f'Wrote value with size={len(data)} bytes to key "{redis_key}" '
                       f'in {round(time_elapsed * 1.0e3, 3)} ms.')

    async def close_async(self):
        self.log.debug("Closing RedisLog.")

        self._redis.close()
        await self._async_redis.close()

    def close(self):
        """Ensure all async coroutines end and clean up."""
        self.log.debug("Closing RedisLog.")

        self._redis.close()

        try:
            asyncio.run(self._async_redis.close())
        except RuntimeError as ex:
            self.log.debug(f"RuntimeError occurred while closing RedisLog: {ex}")

    def __get_path_for_metadata(self)->str:
        """
        Generate and return the path to which we write our metadata.
        """
        return os.path.join(self._base_path, RedisLog._metadata_key)

    async def checkpoint_model_state(self, model: DeepLearningModel):
        """
        Checkpoint the state dictionary of the DeepLearningModel used during training to remote storage.

        This particular method is only used for non-SMR/non-replica-based scheduling policies.

        :return: the e2e latency of the network write, if it occurred, in milliseconds
        """
        # Write the updated model state to remote storage.
        if model is None:
            self.log.debug("Did not find any objects of type DeepLearningModel in the user namespace.")
            return

        if model.requires_checkpointing:
            self.log.debug(
                f"Found model '{model.name}' in user namespace; however, model doesn't require checkpointing.")
            return

        model_pointer: ModelPointer = ModelPointer(
            deep_learning_model=model,
            user_namespace_variable_name="model",
            model_path=os.path.join(self._base_path, model.name),
            proposer_id=self._node_id,
        )

        self.log.debug(f"Checkpointing updated state of model '{model.name}' (on critical path)")

        st: float = time.time()
        await self._remote_checkpointer.write_state_dicts_async(model_pointer)
        et: float = time.time()
        duration_ms: float = (et - st) * 1.0e3

        if self._prometheus_enabled and getattr(self, "remote_storage_write_latency_milliseconds") is not None:
            self.remote_storage_write_latency_milliseconds.labels(
                session_id=self._kernel_id, workload_id=self.workload_id
            ).observe(duration_ms * 1e3)

        self.current_execution_stats.upload_model_and_training_data_microseconds += (
                duration_ms * 1.0e3
        )
        self.current_execution_stats.upload_model_start_unix_millis = st
        self.current_execution_stats.upload_model_end_unix_millis = et

        self.log.debug(f"Checkpointed updated state of model '{model.name}' in {duration_ms:,} ms (on critical path).")