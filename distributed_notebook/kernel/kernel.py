from __future__ import annotations

import asyncio
import faulthandler
import inspect
import json
import logging
import math
import os
import random
import signal
import socket
import sys
import time
import traceback
import typing as t
import uuid
from concurrent import futures
from hmac import compare_digest
from multiprocessing import Process, Queue
from numbers import Number
from threading import Lock
from typing import Union, Optional, Dict, Any, Tuple, Awaitable

import debugpy
import grpc
import numpy as np
import zmq
from IPython.core.interactiveshell import ExecutionResult
from ipykernel import jsonutil
from ipykernel.ipkernel import IPythonKernel
from jupyter_client.jsonutil import extract_dates
from prometheus_client import Counter, Histogram
from prometheus_client import start_http_server
from traitlets import List, Integer, Unicode, Bool, Undefined, Float

from distributed_notebook.deep_learning.data import load_dataset
from distributed_notebook.deep_learning.data.custom_dataset import CustomDataset
from distributed_notebook.deep_learning.models.loader import load_model
from distributed_notebook.deep_learning.models.model import DeepLearningModel
from distributed_notebook.gateway import gateway_pb2
from distributed_notebook.gateway.gateway_pb2_grpc import KernelErrorReporterStub
from distributed_notebook.kernel.iopub_notifier import IOPubNotification
from distributed_notebook.logs import ColoredLogFormatter
from distributed_notebook.sync import Synchronizer, RaftLog, CHECKPOINT_AUTO
from distributed_notebook.sync.checkpointing.checkpointer import Checkpointer
from distributed_notebook.sync.checkpointing.checkpointer_factory import get_checkpointer
from distributed_notebook.sync.checkpointing.pointer import SyncPointer, DatasetPointer, ModelPointer
from distributed_notebook.sync.election import Election, ElectionTimestamps
from distributed_notebook.sync.errors import DiscardMessageError
from distributed_notebook.sync.simulated_checkpointing.simulated_checkpointer import (
    SimulatedCheckpointer,
    get_estimated_io_time_seconds,
    format_size,
)
from .execution_yield_error import ExecutionYieldError
from .stats import ExecutionStats
from .util import extract_header, ElectionAbortedException
from ..deep_learning import DatasetClassesByName, ModelClassesByName, ResNet18, CIFAR10, NaturalLanguageProcessing
from ..sync.log import SyncLog
from ..sync.remote_storage.redis_provider import RedisProvider
from ..sync.remote_storage.s3_provider import S3Provider
from ..sync.remote_storage_log import RemoteStorageLog

import_torch_start: float = time.time()
try:
    import torch

    torch_available: bool = True
    print("[INFO] PyTorch is installed.")

    cuda_available: bool = torch.cuda.is_available()
except ImportError:
    torch_available: bool = False
    cuda_available: bool = False
    print("[WARNING] PyTorch is not installed (and therefore CUDA is unavailable).")

import_torch_end: float = time.time()
import_torch_duration_sec: float = import_torch_end - import_torch_start

if cuda_available:
    print(
        f"[INFO] CUDA is available. Imported PyTorch and initialized CUDA in {import_torch_duration_sec} seconds."
    )
else:
    print("[WARNING] CUDA is unavailable (even though PyTorch is enabled).")


def sigabrt_handler(sig, frame):
    print(f"Received SIGABORT (Python): {sig} {frame}", flush=True)
    sys.stderr.flush()
    sys.stdout.flush()
    sys.exit(0)


def sigint_handler(sig, frame):
    print(f"Received SIGINT (Python): {sig} {frame}", flush=True)
    sys.stderr.flush()
    sys.stdout.flush()
    sys.exit(0)


def sigterm_handler(sig, frame):
    print(f"Received SIGINT (Python): {sig} {frame}", flush=True)
    sys.stderr.flush()
    sys.stdout.flush()
    sys.exit(0)


ErrorNotification: int = 0
WarningNotification: int = 1
InfoNotification: int = 2
SuccessNotification: int = 3

signal.signal(signal.SIGABRT, sigabrt_handler)
signal.signal(signal.SIGINT, sigint_handler)
signal.signal(signal.SIGTERM, sigterm_handler)

DeployModeKubernetes: str = "kubernetes"  # Running via Kubernetes rather than Docker.
DeployModeDockerSwarm: str = "docker-swarm"  # Running via Docker Swarm on one or more machines.
DeployModeDockerCompose: str = "docker-compose"  # Running via Docker Compose on a single system.
DeployModeDocker: str = "docker"  # Used to match against either docker modes.
DeployModeLocal: str = "local"  # Old, used to be used for debugging, no longer used.

# The average time to initialize CUDA on an AWS P3 instance (with a V100 GPU), based on our empirical benchmark.
AverageCudaInitTimeSec: float = 0.17389775
# Standard deviation of CUDA runtime initialization latency on an AWS P3 instance, based on our empirical benchmark.
StandardDeviationCudaInitTimeSec: float = 0.00057978810784631

# Bandwidth (in GB/s) of transferring data from host memory to device (GPU) memory for a V100 GPU (AWS P3 instance).
# This is calculated from our own empirical benchmark.
V100_AvgBandwidth_GbSec_HostToDevice: float = 11.106566666667
V100_StdDevBandwidth_GbSec_HostToDevice: float = 0.0039555867664186

# Bandwidth (in GB/s) of transferring data from device (GPU) memory to host memory for a V100 GPU (AWS P3 instance).
# This is calculated from our own empirical benchmark.
V100_AvgBandwidth_GbSec_DeviceToHost: float = 12.484746666667
V100_StdDevBandwidth_GbSec_DeviceToHost: float = 0.018405739270544

SMR_LEAD_TASK: str = "smr_lead_task"

# This is added to reply_content for replicas that actually executed the user-submitted code.
LEADER_KEY: str = "__LEADER__"

storage_base_default = os.path.dirname(os.path.realpath(__file__))
smr_port_default = 8080
err_wait_persistent_store = RuntimeError("Persistent store not ready, try again later.")
err_failed_to_lead_execution = ExecutionYieldError("Failed to lead the execution.")
err_invalid_request = RuntimeError("Invalid request.")
key_persistent_id = "persistent_id"
enable_storage = True

# Used as the value for an environment variable that was not set.
UNAVAILABLE: str = "N/A"


def tracefunc(frame, event, arg, indent: list[int] = [0]):
    if event == "call":
        indent[0] += 2
        print("-" * indent[0] + "> call function", frame.f_code.co_name, flush=True)
    elif event == "return":
        print("<" + "-" * indent[0], "exit function", frame.f_code.co_name, flush=True)
        indent[0] -= 2
    return tracefunc


def gen_error_response(err):
    return {
        "status": "error",
        "ename": str(type(err).__name__),
        "evalue": str(err),
        "traceback": [],
        "msg_created_at_unix_milliseconds": time.time_ns() // 1_000_000,
    }


def get_download_code(existing_model_code: str, model_name: str) -> str:
    return f"""# Explicitly download the latest model parameters from remote storage.
print("Explicitly downloading the latest model parameters from remote storage for model of type '{model_name}'.", flush = True)
model = __download_func__(__model_pointer__, existing_model = {existing_model_code})
print("Downloaded the latest model parameters from remote storage for model of type '{model_name}'.", flush = True)
"""


def get_create_model_and_dataset_code(deep_learning_model: str, dataset: str, batch_size: Optional[int] = None) -> str:
    return f"""# Create both the model and the dataset.
print(f"Creating model ('{deep_learning_model}') and dataset ('{dataset}') for the first time.", flush = True)
from distributed_notebook.deep_learning import get_model_and_dataset
_model, _dataset = get_model_and_dataset(deep_learning_model_name = "{deep_learning_model}", dataset_name = "{dataset}", batch_size = {batch_size})
model = _model
dataset = _dataset
print(f"Created model ('{deep_learning_model}') and dataset ('{dataset}') for the first time.", flush = True)
"""


def get_create_dataset_only_code(existing_model_name: str, dataset: str, batch_size: Optional[int] = None) -> str:
    return f"""# Create just the dataset.
print(f"Creating dataset ('{dataset}') for the first time.", flush = True)
from distributed_notebook.deep_learning import get_model_and_dataset
import time 
_, _dataset = get_model_and_dataset(deep_learning_model_name = "{existing_model_name}", dataset_name = "{dataset}", batch_size = {batch_size})
dataset = _dataset
print(f"Created dataset ('{dataset}') for the first time.", flush = True)
"""


def get_skipped_creation_code(existing_model_name: str, existing_dataset_name: str,
                              batch_size: Optional[int] = None) -> str:
    return """# existing model is '%s', existing dataset is '%s'
print(f"Model ('{model.name}') and dataset ('{dataset.name}') already exist.", flush = True)
""" % (existing_model_name, existing_dataset_name)


def get_training_code(
        download_code: str,
        creation_code: str,
        gpu_device_ids: list[int],
        target_training_duration_millis: float,
) -> str:
    # raise ValueError("Oops")
    return f"""{creation_code}
{download_code} 
# Distribute the model across the GPUs that we've been allocated.
model.set_gpu_device_ids(device_ids = {gpu_device_ids})
        
# Train for the specified amount of time.
training_time_millis, copy_cpu2gpu_millis, copy_gpu2cpu_millis = model.train(dataset.train_loader, target_training_duration_millis = {target_training_duration_millis})
print("Finished training. Actual training time: %.3f ms." % training_time_millis)
print("Copied model from CPU to GPU in %.3f ms." % copy_cpu2gpu_millis)
print("Copied model back from GPU to CPU in %.3f ms." % copy_gpu2cpu_millis)
"""


class DistributedKernel(IPythonKernel):
    # Configurable properties
    storage_base: Union[str, Unicode] = Unicode(
        storage_base_default,  # type: ignore
        help="""Base directory for remote_storage""",
    ).tag(config=True)

    smr_port = Integer(
        smr_port_default,  # type: ignore
        help="""Port for SMR""",
    ).tag(config=True)

    smr_node_id = Integer(
        1,  # type: ignore
        help="""Node id for SMR""",
    ).tag(config=True)

    smr_nodes = List([], help="""Initial SMR nodes in the SMR cluster""").tag(
        config=True
    )

    smr_join = Bool(
        False,  # type: ignore
        help="""Join the SMR cluster""",
    ).tag(config=True)

    should_register_with_local_daemon = Bool(
        True, help="""Explicitly register with the local daemon?"""
    ).tag(config=True)

    local_daemon_addr: Union[str, Unicode] = Unicode(
        help="""Hostname of the local daemon to register with (when using Docker mode)"""
    ).tag(config=True)

    local_tcp_server_port = Integer(5555, help="Port for local TCP server.").tag(
        config=True
    )

    persistent_id: Union[str, Unicode] = Unicode(
        help="""Persistent id for remote_storage""", allow_none=True
    ).tag(config=True)

    remote_storage_hostname: Union[str, Unicode] = Unicode(
        help="""Hostname of the remotestorage. The SyncLog's RemoteStorage client will connect to this."""
    ).tag(config=True)

    remote_storage: Union[str, Unicode] = Unicode(
        help="The type of remote storage we're using. Valid options, as of right now, are 'hdfs' and 'redis'.",
        default_value="redis",
    ).tag(config=True)

    aws_region: Union[str, Unicode] = Unicode(
        help="The AWS region in which to create/look for the S3 bucket (if we're using AWS S3 for remote storage).",
        default_value="us-east-1",
    ).tag(config=True)

    redis_password: Optional[Union[str, Unicode]] = Unicode(
        help="The password to access Redis (only relevant if using Redis for remote storage).",
        default_value=None,
        allow_none=True
    ).tag(config=True)

    redis_port: Integer = Integer(
        default_value=6379,
        help="Port of the Redis server (only relevant if using Redis for remote storage)."
    ).tag(config=True)

    redis_database: Integer = Integer(
        default_value=0,
        help="Redis database number to use (only relevant if using Redis for remote storage)."
    ).tag(config=True)

    prometheus_port: Integer = Integer(8089, help="Port of the Prometheus Server").tag(
        config=True
    )

    spec_cpus: Float = Float(0, help="Amount of vCPU used by the kernel").tag(
        config=True
    )

    spec_gpus: Integer = Integer(0, help="Number of GPUs used by the kernel").tag(
        config=True
    )

    spec_mem_mb: Float = Float(0, help="Amount of memory in MB used by the kernel").tag(
        config=True
    )

    spec_vram_gb: Float = Float(0, help="Amount of VRAM in GB used by the kernel").tag(
        config=True
    )

    gpu_memory_bandwidth_bytes_sec: Float = Float(
        900_000_000_000, help="Memory bandwidth of the GPU in bytes/sec"
    ).tag(config=True)

    simulate_checkpointing_latency: Bool = Bool(True, help="Simulate network I/O").tag(
        config=True
    )

    deployment_mode: Union[str, Unicode] = Unicode(
        default_value="docker-swarm", help="The deployment mode of the cluster"
    ).tag(config=True)

    workload_id: Union[str, Unicode] = Unicode(
        default_value="N/A",
        help="The ID of the workload in which the kernel is execution.",
    ).tag(config=True)

    election_timeout_seconds: Float = Float(
        10.0,
        help="""How long to wait to receive other proposals before making a decision 
                                            (if we can, like if we have at least received one LEAD proposal). """,
    ).tag(config=True)

    simulate_write_after_execute: Bool = Bool(
        True, help="Simulate network write after executing code?"
    ).tag(config=True)

    simulate_write_after_execute_on_critical_path: Bool = Bool(
        True,
        help="Should the simulated network write after executing code be on the critical path?",
    ).tag(config=True)

    pod_name: Union[str, Unicode] = Unicode(
        help="""Kubernetes name of the Pod encapsulating this distributed kernel replica"""
    ).tag(config=False)

    node_name: Union[str, Unicode] = Unicode(
        help="""Kubernetes name of the Node on which the Pod is running"""
    ).tag(config=False)

    hostname: Union[str, Unicode] = Unicode(
        help="""Hostname of the Pod encapsulating this distributed kernel replica"""
    ).tag(config=False)

    kernel_id: Union[str, Unicode] = Unicode(help="""The ID of the kernel.""").tag(
        config=False
    )

    debug_port: Integer = Integer(8464, help="""Port of debug HTTP server.""").tag(
        config=False
    )

    # If true, then retrieve datasets from our own S3 bucket.
    # The path to each dataset would be: "s3://<bucket_name>/datasets/<dataset_filename>"
    # Each dataset would be stored downloaded as a tar.gz file.
    retrieve_datasets_from_s3: Bool = Bool(default_value=False).tag(
        config=True,
    )

    # If `retrieve_data_from_s3` is True, then this is the S3 bucket from which the kernel should retrieve datasets.
    # The path to each dataset would be: "s3://<bucket_name>/datasets/<dataset_filename>"
    # Each dataset would be stored downloaded as a tar.gz file.
    datasets_s3_bucket: Union[str, Unicode] = Unicode(
        default_value="distributed-notebook-storage"
    ).tag(config=True)

    prewarm_container: Bool = Bool(False,
                                   help="Indicates whether this Kernel was created to serve as a pre-warm container, "
                                        "or if it was created as an actual kernel container.").tag(config=True)

    created_for_migration: Bool = Bool(False, help="If we're created for a migration, "
                                                   "then we do not start our SyncLog right away.").tag(config=True)

    # Indicates whether we should embed Election metadata in "execute_reply" messages.
    include_election_metadata: Bool = Bool(default_value=True).tag(config=True)

    smr_enabled: Bool = Bool(default_value=True).tag(config=True)

    simulate_training_using_sleep: Bool = Bool(default_value=False).tag(config=True)

    implementation = "Distributed Python 3"
    implementation_version = "0.2"
    language = "no-op"
    language_version = "0.2"
    language_info = {
        "name": "Any text",
        "mimetype": "text/plain",
        "file_extension": ".txt",
    }
    # banner = "Distributed kernel - as useful as a parrot"

    # Persistent store will be initialized according to persistent_id associated with the notebook file.
    # The initialization is triggered by kernel.js
    store: Optional[Union[str, asyncio.Future]] = None
    synclog: RaftLog
    synclog_stopped: bool  # closed() called on synclog, during migration prep
    # When migrating a replica, we do NOT want to remove the replica from the raft SMR cluster on shutdown.
    remove_on_shutdown: bool
    synchronizer: Synchronizer
    smr_nodes_map: dict

    def __init__(self, **kwargs):
        start_time: float = time.time()

        faulthandler.enable()
        if super().log is not None:
            super().log.setLevel(logging.DEBUG)
        print(f" Kwargs: {kwargs}")

        session_id: str = os.environ.get("SESSION_ID", default=UNAVAILABLE)
        self.kernel_id = os.environ.get("KERNEL_ID", default=UNAVAILABLE)
        self.docker_container_id: str = "N/A"

        self.control_msg_types = [
            *self.control_msg_types,
            "add_replica_request",
            "update_replica_request",
            "prepare_to_migrate_request",
            "stop_running_training_code_request",
            "ping_kernel_ctrl_request",
            "promote_prewarm_request",
            "reset_kernel_request",
            "start_synclog_request",
        ]

        self.msg_types = [
            *self.msg_types,
            "yield_request",
            "ping_kernel_shell_request",
        ]

        super().__init__(**kwargs)

        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        for handler in self.log.handlers:
            handler.setLevel(logging.DEBUG)

        self.log.info("DistributedKernel initializing.")

        # self.log.info("INFO")
        # self.log.debug("DEBUG")
        # self.log.warning("WARNING")
        # self.log.error("ERROR")

        # Used to create strong references to tasks, such as notifying peer replicas that execution has complete.
        self.background_tasks = set()
        self.next_execute_request_msg_id: Optional[str] = None
        self.old_run_cell = None
        self.prometheus_thread = None
        self.prometheus_server = None
        self.source = None
        self.kernel_notification_service_channel: Optional[grpc.Channel] = None
        self.kernel_notification_service_stub: Optional[KernelErrorReporterStub] = None
        self.message_acknowledgements_enabled: bool = True  # default to True
        self.shell_received_at: Optional[float] = None
        self.init_persistent_store_on_start_future: Optional[futures.Future] = None
        self.store_path: str = ""
        self.synclog: Optional[SyncLog|RemoteStorageLog|RaftLog] = None
        self._remote_checkpointer: Optional[Checkpointer] = None  # created in start method.

        # Normally, we start the SyncLog in DistributedKernel::start.
        #
        # However, if this kernel replica was created during a migration, then we must wait to start
        # the SyncLog until we're explicitly directed to do so. This is because we must wait for the
        # old replica to finish checkpointing its state, and for the old replica's peer address to
        # be updated to our address.
        self.start_synclog_immediately: bool = not self.created_for_migration

        if self.start_synclog_immediately:
            self.log.debug("Will start SyncLog immediately when initializing the persistent store.")
        else:
            self.log.debug("Will wait to start SyncLog until we're explicitly instructed to do so.")

        if "prewarm_container" in kwargs:
            self.prewarm_container = kwargs["prewarm_container"]

        # If we are now, then we were "before" (technically before doesn't exist because this is the c'tor, but still).
        self.was_prewarmed_container = self.prewarm_container

        if self.prewarm_container:
            self.prewarm_container_id: Optional[str] = self.kernel_id
            self.log.debug(f'I am a prewarm container with prewarm ID "{self.prewarm_container_id}"')
        else:
            self.prewarm_container_id: Optional[str] = None
            self.log.debug(f'I am NOT a prewarm container (ID="{self.kernel_id}")')

        # Keep track of how many times we generate the 'download' code when generating custom DL training code.
        self._get_download_code_called: int = 0

        # Keep track of how many times we generate the 'create model for the first time' code when generating
        # custom DL training code.
        self._get_creation_code_called: int = 0

        if "remote_storage" in kwargs:
            kwarg_remote_storage: str = kwargs["remote_storage"]
            self.log.warning(f'Overwriting configured remote checkpointer "{self.remote_storage}" '
                             f'with keyword argument "{kwarg_remote_storage}"')
            self.remote_storage = kwarg_remote_storage
        else:
            self.log.debug(f"Using remote storage '{self.remote_storage}', hostname='{self.remote_storage_hostname}'")

        if "smr_enabled" in kwargs:
            kwargs_smr_enabled: Bool = kwargs["smr_enabled"]
            self.log.warning(f'Overwriting configured "smr_enabled" flag ({self.smr_enabled}) '
                             f'with keyword argument {kwargs_smr_enabled}')
            self.smr_enabled = kwargs_smr_enabled
        else:
            self.log.debug(f"smr_enabled={self.smr_enabled}")

        # Arguments not relevant to the specified remote storage will be ignored.
        self._remote_checkpointer: Optional[Checkpointer] = get_checkpointer(
            remote_storage_name=self.remote_storage,
            host=self.remote_storage_hostname,
            aws_region=self.aws_region,
            redis_port=self.redis_port,
            redis_database=self.redis_database,
            redis_password=self.redis_password,
        )

        # Committed DatasetPointers that we encounter while catching-up after a migration.
        # Once we're caught-up, we download all of these.
        #
        # Keys are user namespace variable names and values are the pointers.
        # This way, if the user overwrote a variable with a different dataset pointer,
        # then we'll only retrieve the most up-to-date version of that variable.
        #
        # TODO: This would be problematic if the user were to overwrite a variable that was initially committed
        #       as a large object, such as a dataset or model, but then later assigned that variable to a smaller
        #       object. That's because we're going to inject the large object pointed to by these pointers into the
        #       user namespace, so we'll end up overwriting the existing variable.
        #
        # TODO: One option is to check if there already exists a variable with the same name once we go and try to
        #       read all of these. But we won't know if that variable was defined before or after the large object.
        #       So, at best, we would just know of a potential conflict, but not how to resolve it...
        self.dataset_pointers_catchup: Dict[str, DatasetPointer] = {}

        if "include_election_metadata" in kwargs:
            self.include_election_metadata = kwargs["include_election_metadata"]

        if self.include_election_metadata:
            self.log.debug("Will include election metadata in 'execute_reply' messages.")
        else:
            self.log.debug("Will NOT include election metadata in 'execute_reply' messages.")

        # Committed ModelPointers that we encounter while catching-up after a migration.
        # Once we're caught-up, we download all of these.
        #
        # Keys are user namespace variable names and values are the pointers.
        # This way, if the user overwrote a variable with a different model pointer,
        # then we'll only retrieve the most up-to-date version of that variable.
        #
        # TODO: This would be problematic if the user were to overwrite a variable that was initially committed
        #       as a large object, such as a dataset or model, but then later assigned that variable to a smaller
        #       object. That's because we're going to inject the large object pointed to by these pointers into the
        #       user namespace, so we'll end up overwriting the existing variable.
        #
        # TODO: One option is to check if there already exists a variable with the same name once we go and try to
        #       read all of these. But we won't know if that variable was defined before or after the large object.
        #       So, at best, we would just know of a potential conflict, but not how to resolve it...
        self.model_pointers_catchup: Dict[str, ModelPointer] = {}

        if "simulate_training_using_sleep" in kwargs:
            self.simulate_training_using_sleep = kwargs["simulate_training_using_sleep"]

        ########################
        # Execution/Data State #
        ########################

        # Tracks all overheads and activity during execution.
        #
        # We create this when the kernel is first created so that any overheads that occur during the initial,
        # first-time start-up of the kernel are recorded.
        #
        # The current_execution_stats field is reset each time an "execute_reply" is sent back to the client.
        self.current_execution_stats: ExecutionStats = ExecutionStats()

        # Indicates if the CUDA runtime initialized.
        # This is only used when simulating the use of real GPUs.
        self.cuda_initialized: bool = False

        # Indicates of the data is already on the GPU.
        # This is only used when simulating the use of real GPUs.
        self.data_on_gpu: bool = False

        # Indicates if the runtime dependencies (e.g., PyTorch/TensorFlow, etc.) are already downloaded.
        # This is only used when simulating the use of real GPUs.
        self.runtime_dependencies_downloaded: bool = False

        # datasaet_downloaded indicates whether the PyTorch dataset assigned to this kernel has been downloaded.
        # This is only used when we're using real GPUs.
        self.datasaet_downloaded: bool = False

        # Indicates if the (latest) model parameters and training data are downloaded.
        # This is only used when simulating the use of real GPUs.
        self.model_and_training_data_downloaded: bool = False

        # This is set to True in self.loaded_serialized_state_callback, which is called by the RaftLog after it
        # loads its serialized state from intermediate remote_storage.
        self.loaded_serialized_state: bool = False

        # _user_ns_lock ensures atomic operations when accessing self.shell.user_ns from coroutines.
        self._user_ns_lock: asyncio.Lock = asyncio.Lock()

        if "aws_region" in kwargs:
            self.aws_region = kwargs["aws_region"]

        if "redis_port" in kwargs:
            self.redis_port = kwargs["redis_port"]

        if "redis_database" in kwargs:
            self.redis_database = kwargs["redis_database"]

        if "redis_password" in kwargs:
            self.redis_password = kwargs["redis_password"]

        self.log.debug(f'Remote storage hostname: "{self.remote_storage_hostname}"')

        # Mapping from Remote Storage / SimulatedCheckpointer name to the SimulatedCheckpointer object.
        self.remote_storages: Dict[str, SimulatedCheckpointer] = {
            "AWS S3 (Default)": SimulatedCheckpointer(
                name="AWS S3 (Default)",
                download_rate=300_000_000,  # 300 MB/sec
                upload_rate=75_000_000,  # 75 MB/sec
                download_variance_percent=0.05,
                upload_variance_percent=0.05,
                read_failure_chance_percentage=0.0,
                write_failure_chance_percentage=0.0,
            )
        }

        self.current_resource_request: Optional[Dict[str, float | int | List[float] | List[int]]] = {
            "cpus": self.spec_cpus,
            "gpus": self.spec_gpus,
            "memory": self.spec_mem_mb,
            "vram": self.spec_vram_gb,
        }
        self.resource_requests: list[Dict[str, Number | List[Number]]] = [self.current_resource_request]

        self.log.debug(f"Initial resource request: {self.current_resource_request}")
        self.log.debug(f"GPU memory bandwidth: {self.gpu_memory_bandwidth_bytes_sec / 1.0e9} bytes/sec")

        if "local_tcp_server_port" in kwargs:
            self.log.warning(f"Overwriting default value of local_tcp_server_port ({self.local_tcp_server_port}) with "
                             f"value from keyword arguments: {kwargs['local_tcp_server_port']}")
            self.local_tcp_server_port = kwargs["local_tcp_server_port"]

        self.prometheus_enabled: bool = True
        prometheus_port_str: str | int = os.environ.get("PROMETHEUS_METRICS_PORT", 8089)

        try:
            self.prometheus_port: int = int(prometheus_port_str)
        except ValueError as ex:
            self.log.error(f'Failed to parse "PROMETHEUS_METRICS_PORT" environment '
                           f'variable value "{prometheus_port_str}": {ex}')
            self.log.error("Will use default prometheus metrics port of 8089.")
            self.prometheus_port: int = 8089

        if self.prometheus_port > 0:
            self.log.debug(f"Starting Prometheus HTTP server on port {self.prometheus_port}.")
            try:
                self.prometheus_server, self.prometheus_thread = start_http_server(self.prometheus_port)
            except OSError as exc:
                self.log.error(f"Failed to start Prometheus Server because: {exc}")
                self.log.error("Disabling Prometheus.")
                self.prometheus_enabled = False
        else:
            self.log.warning(f"Prometheus Port is configured as {self.prometheus_port}. "
                             f"Skipping creation of Prometheus HTTP server.")
            self.prometheus_enabled = False

        if self.prometheus_enabled:
            # Prometheus metrics.
            self.num_yield_proposals: Counter = Counter(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_yield_proposals_total",
                documentation="Total number of 'YIELD' proposals.",
            )
            self.num_lead_proposals: Counter = Counter(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_lead_proposals_total",
                documentation="Total number of 'LEAD' proposals.",
            )
            self.remote_storage_read_latency_milliseconds: Histogram = Histogram(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_remote_storage_read_latency_milliseconds",
                documentation="The amount of time the kernel spent reading data from RemoteStorage.",
                unit="milliseconds",
                labelnames=["session_id", "workload_id"],
                buckets=[
                    1,
                    10,
                    30,
                    75,
                    150,
                    250,
                    500,
                    1000,
                    2000,
                    5000,
                    10e3,
                    20e3,
                    45e3,
                    90e3,
                    300e3,
                ],
            )
            self.remote_storage_write_latency_milliseconds: Histogram = Histogram(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_remote_storage_write_latency_milliseconds",
                documentation="The amount of time the kernel spent writing data to RemoteStorage.",
                unit="milliseconds",
                labelnames=["session_id", "workload_id"],
                buckets=[
                    1,
                    10,
                    30,
                    75,
                    150,
                    250,
                    500,
                    1000,
                    2000,
                    5000,
                    10e3,
                    20e3,
                    45e3,
                    90e3,
                    300e3,
                ],
            )
            self.registration_time_milliseconds: Histogram = Histogram(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_registration_latency_milliseconds",
                documentation="The latency of a new kernel replica registering with its Local Daemon.",
                unit="milliseconds",
                buckets=[
                    1,
                    10,
                    30,
                    75,
                    150,
                    250,
                    500,
                    1000,
                    2000,
                    5000,
                    10e3,
                    20e3,
                    45e3,
                    90e3,
                    300e3,
                ],
            )
            self.execute_request_latency: Histogram = Histogram(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_execute_request_latency_milliseconds",
                documentation="Execution time of the kernels' execute_request method in milliseconds.",
                unit="milliseconds",
                buckets=[
                    10,
                    100,
                    250,
                    500,
                    1e3,
                    5e3,
                    10e3,
                    20e3,
                    30e3,
                    60e3,
                    300e3,
                    600e3,
                    1.0e6,
                    3.6e6,
                    7.2e6,
                    6e7,
                    6e8,
                    6e9,
                ],
            )
            self.delay_milliseconds: Counter = Counter(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="delay_milliseconds",
                documentation="Delay incurred by a particular Session.",
                labelnames=["session_id", "workload_id"],
            )

        from ipykernel.debugger import _is_debugpy_available

        if _is_debugpy_available:
            self.log.info("The Debugger IS available/enabled.")

            assert self.debugger is not None
        else:
            self.log.warning("The Debugger is NOT available/enabled.")

        self.received_message_ids: set = set()

        self.run_training_code_mutex: Lock = Lock()
        self.run_training_code: bool = False

        # If true, then we create a debugpy server.
        self.debugpy_enabled: bool = False
        self.debugpy_wait_for_client: bool = False

        # The time at which we were created (i.e., the time at which the DistributedKernel object was instantiated)
        self.created_at: float = time.time()

        self.execution_ast = None
        self.smr_nodes_map = {}
        self.synclog_stopped = False
        # By default, we do not want to remove the replica from the raft SMR cluster on shutdown.
        # We'll only do this if we're explicitly told to do so.
        self.remove_on_shutdown = False

        # asyncio.Event objects for initialization steps
        self.init_raft_log_event: asyncio.Event = asyncio.Event()
        self.init_synchronizer_event: asyncio.Event = asyncio.Event()
        self.start_synchronizer_event: asyncio.Event = asyncio.Event()
        self.init_persistent_store_event: asyncio.Event = asyncio.Event()

        # How long to wait to receive other proposals before making a decision (if we can, like if we
        # have at least received one LEAD proposal).
        self.election_timeout_seconds: float = 10

        # Single node mode
        if not isinstance(self.smr_nodes, list) or len(self.smr_nodes) == 0:
            self.smr_nodes = [f":{self.smr_port}"]
            self.smr_nodes_map = {0: f":{self.smr_port}"}
        else:
            for i, host in enumerate(self.smr_nodes):
                self.smr_nodes_map[i] = f"{host}:{self.smr_port}"

        self.log.info("Kwargs: %s" % str(kwargs))

        if self.simulate_checkpointing_latency:
            self.log.debug("Checkpointing Latency Simulation is enabled.")
        else:
            self.log.debug("Checkpointing Latency Simulation is disabled.")

        connection_file_path = os.environ.get("CONNECTION_FILE_PATH", "")
        config_file_path = os.environ.get("IPYTHON_CONFIG_PATH", "")

        self.deployment_mode: str = os.environ.get("DEPLOYMENT_MODE", DeployModeDocker)
        if len(self.deployment_mode) == 0:
            raise ValueError("Could not determine deployment mode.")
        else:
            self.log.debug(f"Deployment mode: {self.deployment_mode}")

        if self.deployment_mode == DeployModeKubernetes:
            self.docker_container_name = os.environ.get("POD_NAME", default=UNAVAILABLE)
            self.node_name = os.environ.get("NODE_NAME", default=UNAVAILABLE)
            self.docker_container_id: str = self.docker_container_name
        elif DeployModeDocker in self.deployment_mode:  # compose or swarm
            self.docker_container_id: str = socket.gethostname()
            self.docker_container_name = os.environ.get("CONTAINER_NAME", default=UNAVAILABLE)
            self.node_name = os.environ.get("NODE_NAME", default="DockerNode")
        else:
            self.docker_container_name = os.environ.get("CONTAINER_NAME", default=UNAVAILABLE)
            self.node_name = os.environ.get("NODE_NAME", default=UNAVAILABLE)
            self.docker_container_id: str = self.docker_container_name

        self.log.info('Connection file path: "%s"' % connection_file_path)
        self.log.info('IPython config file path: "%s"' % config_file_path)
        self.log.info('Session ID: "%s"' % session_id)
        self.log.info('Kernel ID: "%s"' % self.kernel_id)
        self.log.info('Pod name: "%s"' % self.docker_container_name)
        self.log.info("SMR port: '%d'" % self.smr_port)
        self.log.info('RemoteStorage hostname: "%s"' % self.remote_storage_hostname)

        if self.remote_storage_hostname == "":
            self.remote_storage_hostname = kwargs.get("remote_storage_hostname", "")

        if self.remote_storage_hostname == "":
            raise ValueError("The RemoteStorage hostname is empty. Was it specified in the configuration file?")

        self.log.info("CPU: %.2f, Memory: %f, GPUs: %d, VRAM: %.6f."
                      % (self.spec_cpus, self.spec_mem_mb, self.spec_gpus, self.spec_vram_gb)
                      )

        # This should only be accessed from the control IO loop (rather than the main/shell IO loop).
        self.persistent_store_cv: asyncio.Condition = asyncio.Condition()

        # preparing_to_migrate_cv is used when handling shutdown requests.
        #
        # Specifically, the control thread needs to wait to shut the kernel down until the important state
        # is safely persisted to remote storage.
        #
        # This condition variable is used to wait until that completes, and to signal that it has completed.
        self.preparing_to_migrate_cv: asyncio.Condition = asyncio.Condition()

        # preparing_to_migrate is a flag indicating that the kernel is preparing to migrate and should not
        # be shutdown, as important state needs to be checkpointed first.
        self.preparing_to_migrate: bool = False

        self.checkpointing_state_cv: asyncio.Condition = asyncio.Condition()
        self.checkpointing_state: bool = False

        # If we're part of a migration operation, then it will be set when we register with the local daemon.
        self.should_read_data_from_remote_storage: bool = False

        self.connection_info: dict[str, Any] = {}

        if len(connection_file_path) > 0:
            try:
                with open(connection_file_path, "r") as connection_file:
                    self.connection_info = json.load(connection_file)
            except Exception as ex:
                self.log.error(f'Failed to obtain connection info from file "{connection_file_path}" because: {ex}')

        self.log.info("Connection info: %s" % str(self.connection_info))

        # Allow setting env variable to prevent registration altogether.
        skip_registration_override: bool = (os.environ.get("SKIP_REGISTRATION", "false").lower() == "true")

        if self.should_register_with_local_daemon and not skip_registration_override:
            registration_start: float = time.time()
            self.register_with_local_daemon(self.connection_info, session_id)
            registration_duration: float = (time.time() - registration_start) * 1.0e3

            self.log.debug(f"Finished registration procedure in {round(registration_duration, 3):,} ms.")

            if self.prometheus_enabled:
                self.registration_time_milliseconds.observe(registration_duration)

            self.__init_tcp_server()
        else:
            self.log.warning("Skipping registration step with local daemon.")
            self.__init_tcp_server()
            self.smr_node_id: int = int(os.environ.get("smr_node_id", "1"))
            self.hostname = os.environ.get("hostname", str(socket.gethostname()))
            self.num_replicas: int = 1
            self.smr_nodes_map = {1: str(self.hostname) + ":" + str(self.smr_port)}
            self.debug_port: int = int(os.environ.get("debug_port", "31000"))
            self.debugpy_port: int = int(os.environ.get("debugpy_port", self.debug_port + 10000))

            if "persistent_id" in kwargs:
                self.persistent_id: str = kwargs["persistent_id"]
            else:
                self.persistent_id = os.environ.get("persistent_id", str(uuid.uuid4()))

            # self.start()

    def __init_tcp_server(self):
        if self.local_tcp_server_port < 0:
            self.log.warning(f"Local TCP server port set to {self.local_tcp_server_port}. Returning immediately.")
            return

        self.local_tcp_server_queue: Queue = Queue()
        self.local_tcp_server_process: Process = Process(target=self.server_process,
                                                         args=(self.local_tcp_server_queue,))
        self.local_tcp_server_process.daemon = True
        self.local_tcp_server_process.start()
        self.log.info(f"Local TCP server process has PID={self.local_tcp_server_process.pid}")

    # TODO(Ben): Is the existence of this process slowing down the termination process?
    # TODO(Ben): Is this actually being used right now?
    def server_process(self, queue: Queue):
        faulthandler.enable()

        if self.local_tcp_server_port < 0:
            self.log.warning(
                f"[Local TCP Server] Port set to {self.local_tcp_server_port}. Returning immediately."
            )
            return

        self.log.info(
            f"[Local TCP Server] Starting. Port: {self.local_tcp_server_port}"
        )
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", self.local_tcp_server_port))
        sock.listen(5)
        self.log.info(f"[Local TCP Server] Started. Port: {self.local_tcp_server_port}")

        while True:
            conn, addr = sock.accept()

            self.log.info(
                f"[Local TCP Server] Received incoming connection (addr={addr}). Training should have started."
            )

            queue.get(block=True, timeout=None)

            self.log.info(
                "[Local TCP Server] Received 'STOP' instruction from Cluster."
            )

            conn.send(b"stop")

            self.log.info(
                "[Local TCP Server] Sent 'STOP' instruction to shell thread via TCP."
            )

            conn.close()

            # config_info:dict,

    def init_metadata(self, parent):
        """Initialize metadata.

        Run at the beginning of each execution request.
        """
        md = super().init_metadata(parent)
        md.update(parent['metadata'])

        return md

    def handle_registration_response(self, response: bytes, start_time: float, local_daemon_service_name: str):
        if len(response) == 0:
            self.log.error("Received empty response from local daemon during registration...")
            raise ValueError("received empty response from local daemon during registration procedure")

        self.log.info(f"Received {len(response)} byte(s) in response from LocalDaemon after "
                      f"{time.time() - start_time} seconds. Response payload: {str(response)}")

        response_dict = json.loads(response)

        # If we're a pre-warm container, then we ignore everything from the initial registration payload
        # except for the "message ACKs" configuration.
        if self.prewarm_container:
            if "message_acknowledgements_enabled" in response_dict:
                self.message_acknowledgements_enabled = bool(response_dict["message_acknowledgements_enabled"])

                if self.message_acknowledgements_enabled:
                    self.log.debug("Message acknowledgements are enabled.")
                else:
                    self.log.debug("Message acknowledgements are disabled.")
            else:
                self.log.warning('No "message_acknowledgements_enabled" entry in response from local daemon.')

            return

        self.smr_node_id: int = response_dict["smr_node_id"]
        self.hostname = response_dict["hostname"]

        # self.smr_nodes = [hostname + ":" + str(self.smr_port) for hostname in response_dict["replicas"]]

        if "replicas" not in response_dict:
            self.log.error("No replicas contained in registration response from local daemon.")
            self.log.error("Registration response:")
            for k, v in response_dict.items():
                self.log.error(f"\t{k}: {v}")
            raise ValueError('registration response from local daemon did not contained a "replicas" entry')

        # Note: we expect the keys to be integers; however, they will have been converted to strings.
        # See https://stackoverflow.com/a/1451857 for details.
        # We convert the string keys, which are node IDs, back to integers.
        #
        # We also append ":<SMR_PORT>" to each address before storing it in the map.
        replicas: Optional[dict[int, str]] = response_dict["replicas"]
        if replicas is None or len(replicas) == 0:
            self.log.error("No replicas contained in registration response from local daemon.")
            self.log.error("Registration response:")
            for k, v in response_dict.items():
                self.log.error(f"\t{k}: {v}")
            raise ValueError('registration response from local daemon did not contained a "replicas" entry')

        self.num_replicas: int = len(replicas)
        if "smr_port" in response_dict:  # Only occurs during testing.
            self.smr_port = int(response_dict["smr_port"])
            self.log.debug(f"Set SMR port to value from Local Daemon registration response: {self.smr_port}")

            self.smr_nodes_map = {int(node_id_str): node_addr for node_id_str, node_addr in replicas.items()}
        else:
            self.smr_nodes_map = {
                int(node_id_str): (node_addr + ":" + str(self.smr_port))
                for node_id_str, node_addr in replicas.items()
            }

        # If we're part of a migration operation, then we should receive both a persistent ID AND an RemoteStorage Data Directory.
        # If we're not part of a migration operation, then we'll JUST receive the persistent ID.
        if "persistent_id" in response_dict:
            self.log.info('Received persistent ID from registration: "%s"' % response_dict["persistent_id"])
            self.persistent_id = response_dict["persistent_id"]

        # We'll also use this as an indicator of whether we should simulate additional checkpointing overhead.
        self.should_read_data_from_remote_storage = response_dict.get("should_read_data_from_remote_storage", False)

        if self.should_read_data_from_remote_storage:
            self.log.debug("We SHOULD read data from RemoteStorage.")
        else:
            self.log.debug("We should NOT read data from RemoteStorage.")

        if "debug_port" in response_dict:
            self.debug_port: int = int(response_dict["debug_port"])
            self.log.info(f"Assigned debug port to {self.debug_port}")
            self.debugpy_port: int = int(os.environ.get("debugpy_port", self.debug_port + 10000))
        else:
            self.log.warning(
                'No "debug_port" entry found in response from local daemon.'
            )
            self.debug_port: int = -1
            self.debugpy_port: int = -1

        if "message_acknowledgements_enabled" in response_dict:
            self.message_acknowledgements_enabled = bool(response_dict["message_acknowledgements_enabled"])

            if self.message_acknowledgements_enabled:
                self.log.debug("Message acknowledgements are enabled.")
            else:
                self.log.debug("Message acknowledgements are disabled.")
        else:
            self.log.warning('No "message_acknowledgements_enabled" entry in response from local daemon.')

        self.log.info("Received SMR Node ID after registering with local daemon: %d" % self.smr_node_id)
        self.log.info("Replica hostnames: %s" % str(self.smr_nodes_map))

        if self.smr_nodes_map[self.smr_node_id] != (self.hostname + ":" + str(self.smr_port)):
            expected_hostname: str = self.hostname + ":" + str(self.smr_port)
            self.log.error("Invalid Configuration")
            self.log.error(f'Expected our entry in SMR nodes map to equal "{expected_hostname}".')
            self.log.error(f'Instead, entry {self.smr_node_id} is equal to "{self.smr_nodes_map[self.smr_node_id]}"')
            raise ValueError(f'Invalid configuration: SMR node map entry {self.smr_node_id} '
                             f'has unexpected value "{expected_hostname}"')

        grpc_port: int = response_dict.get("grpc_port", -1)
        if grpc_port == -1:
            self.log.warning("Received -1 for gRPC port from registration payload.")
        else:
            self.log.debug(f'Creating gRPC connection to Local Daemon at "{local_daemon_service_name}:{grpc_port}"')
            # Establish gRPC connection with Local Daemon so that we can send notifications if/when errors occur.
            self.kernel_notification_service_channel = grpc.insecure_channel(f"{local_daemon_service_name}:{grpc_port}")
            self.kernel_notification_service_stub = KernelErrorReporterStub(self.kernel_notification_service_channel)

    async def register_with_local_daemon_async(self, connection_info: dict, session_id: str):
        self.log.info("Registering with local daemon now.")

        local_daemon_service_name = os.environ.get("LOCAL_DAEMON_SERVICE_NAME", default=self.local_daemon_addr)
        server_port = os.environ.get("LOCAL_DAEMON_SERVICE_PORT", default=8075)
        try:
            server_port = int(server_port)
        except ValueError:
            server_port = 8075

        self.log.info('Local Daemon network address: "%s:%d"' % (local_daemon_service_name, server_port))

        start_time: float = time.time()
        try:
            reader, writer = await asyncio.open_connection(local_daemon_service_name, server_port)
        except Exception as ex:
            self.log.error(f"Failed to connect to LocalDaemon at {local_daemon_service_name}:{server_port}")
            self.log.error("Reason: %s" % str(ex))
            raise ex

        self.log.info(f"Successfully connected to Local Daemon at {local_daemon_service_name}:{server_port} "
                      f"in {round((time.time() - start_time) * 1.0e3, 3):,} ms.")

        try:
            registration_payload = {
                "op": "register",
                "signatureScheme": connection_info.get("signature_scheme", "hmac-sha256"),
                "key": connection_info.get("key", ""),
                "replicaId": self.smr_node_id,
                "numReplicas": len(self.smr_nodes_map),
                "join": self.smr_join,
                "podName": self.docker_container_name,
                "nodeName": self.node_name,
                "connection-info": connection_info,
                "workload_id": self.workload_id,
                "prewarm_container": self.prewarm_container,
                "kernel": {
                    "id": self.kernel_id,
                    "session": session_id,
                    "signature_scheme": connection_info.get("signature_scheme", "hmac-sha256"),
                    "key": connection_info.get("key", ""),
                },
            }
        except Exception as ex:
            self.log.error(f"Failed to create registration payload: {ex}")
            raise ex  # Re-raise

        self.log.info("Sending registration payload to local daemon: %s" % str(registration_payload))

        writer.write(json.dumps(registration_payload).encode())
        await writer.drain()

        self.log.info("Asynchronously sent registration payload to local daemon.")

        num_tries: int = 0
        registered_successfully: bool = False

        while num_tries < 3 and not registered_successfully:
            response: bytes = await reader.read(1024)

            try:
                self.handle_registration_response(response, start_time, local_daemon_service_name)
                registered_successfully = True
                break
            except ValueError as ex:
                self.log.error(ex)
                num_tries += 1
                await asyncio.sleep(num_tries * 2)
                continue

        if not registered_successfully:
            self.report_error("Failed to Register with Local Daemon", "")

        try:
            writer.close()
            await writer.wait_closed()
        except Exception as ex:
            self.log.warning(f"Error while closing writer to Local Daemon: {ex}")
            self.log.warning(traceback.format_exc())

        try:
            reader.close()
            await reader.close()
        except Exception as ex:
            self.log.warning(f"Error while closing writer to Local Daemon: {ex}")
            self.log.warning(traceback.format_exc())

    def register_with_local_daemon(self, connection_info: dict, session_id: str):
        self.log.info("Registering with local daemon now.")

        local_daemon_service_name = os.environ.get("LOCAL_DAEMON_SERVICE_NAME", default=self.local_daemon_addr)
        server_port = os.environ.get("LOCAL_DAEMON_SERVICE_PORT", default=8075)
        try:
            server_port = int(server_port)
        except ValueError:
            server_port = 8075

        self.log.info('Local Daemon network address: "%s:%d"' % (local_daemon_service_name, server_port))

        daemon_registration_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        start_time: float = time.time()
        try:
            daemon_registration_socket.connect((local_daemon_service_name, server_port))
        except Exception as ex:
            self.log.error(f"Failed to connect to LocalDaemon at {local_daemon_service_name}:{server_port}")
            self.log.error("Reason: %s" % str(ex))
            raise ex

        self.log.info(f"Successfully connected to Local Daemon at {local_daemon_service_name}:{server_port} "
                      f"in {round((time.time() - start_time) * 1.0e3, 3):,} ms.")

        try:
            registration_payload = {
                "op": "register",
                "signatureScheme": connection_info.get("signature_scheme", "hmac-sha256"),
                "key": connection_info.get("key", ""),
                "replicaId": self.smr_node_id,
                "numReplicas": len(self.smr_nodes_map),
                "join": self.smr_join,
                "podName": self.docker_container_name,
                "nodeName": self.node_name,
                "connection-info": connection_info,
                "workload_id": self.workload_id,
                "prewarm_container": self.prewarm_container,
                "kernel": {
                    "id": self.kernel_id,
                    "session": session_id,
                    "signature_scheme": connection_info.get("signature_scheme", "hmac-sha256"),
                    "key": connection_info.get("key", ""),
                },
            }
        except Exception as ex:
            self.log.error(f"Failed to create registration payload: {ex}")
            raise ex  # Re-raise

        self.log.info("Sending registration payload to local daemon: %s" % str(registration_payload))

        bytes_sent: int = daemon_registration_socket.send(json.dumps(registration_payload).encode())

        self.log.info("Sent %d byte(s) to local daemon." % bytes_sent)

        num_tries: int = 0
        registered_successfully: bool = False

        while num_tries < 3 and not registered_successfully:
            response: bytes = daemon_registration_socket.recv(1024)

            try:
                self.handle_registration_response(response, start_time, local_daemon_service_name)
                registered_successfully = True
                break
            except ValueError as ex:
                self.log.error(ex)
                num_tries += 1
                time.sleep(num_tries * 2)
                continue

        daemon_registration_socket.close()

        if not registered_successfully:
            self.report_error("Failed to Register with Local Daemon", "")

    def init_debugpy(self):
        if not self.debugpy_enabled or self.debug_port < 0:
            self.log.debug("debugpy server is disabled or server port is set to a negative number.")
            return

        self.log.debug(f"Starting debugpy server on 0.0.0.0:{self.debugpy_port}")
        debugpy.listen(("0.0.0.0", self.debugpy_port))

        if self.debugpy_wait_for_client:
            self.log.debug("Waiting for debugpy client to connect before proceeding.")
            debugpy.wait_for_client()
            self.log.debug("Debugpy client has connected. We may now proceed.")
            debugpy.breakpoint()
            self.log.debug("Should have broken on the previous line!")

    def persistent_store_initialized_callback(self, f: futures.Future):
        """
        Simple callback to print a message when the initialization of the persistent store completes.
        """
        if f.cancelled():
            self.log.error("Initialization of Persistent Store on-start was apparently cancelled...")
            return

        ex = f.exception()
        if ex is not None:
            self.log.error(f"Initialization of Persistent Store apparently raised an exception: {ex}")
            self.report_error(f'Kernel {self.kernel_id} Failed to Initialize Persistent Store', str(ex))
            return

        self.log.debug("Initialization of Persistent Store has completed on the Control Thread's IO loop.")

    def start(self):
        self.log.info('DistributedKernel is starting. Persistent ID = "%s"' % self.persistent_id)

        super().start()

        self.init_debugpy()

        persistent_id_defined: bool = self.persistent_id != Undefined and self.persistent_id != "" and self.persistent_id is not None

        if persistent_id_defined and self.start_synclog_immediately:
            assert isinstance(self.persistent_id, str)
            self.log.debug(f"Scheduling creation of init_persistent_store_on_start_future. "
                           f"Loop is running: {self.control_thread.io_loop.asyncio_loop.is_running()}")
            self.init_persistent_store_on_start_future: futures.Future = (
                asyncio.run_coroutine_threadsafe(
                    self.init_persistent_store_on_start(self.persistent_id),
                    self.control_thread.io_loop.asyncio_loop,
                )
            )
            self.init_persistent_store_on_start_future.add_done_callback(self.persistent_store_initialized_callback)
        else:
            self.log.warning("Will NOT be initializing Persistent Store on start, "
                             "as persistent ID is not yet available.")

    async def init_persistent_store_on_start(self, persistent_id: str):
        self.log.info(f'Initializing Persistent Store on start, as persistent ID is available: "{persistent_id}"')
        # Create future to avoid duplicate initialization
        future = asyncio.Future(loop=asyncio.get_running_loop())
        self.store = future

        rsp = None

        try:
            self.store = await self.init_persistent_store_with_persistent_id(persistent_id)
            self.log.info(f"Persistent store confirmed on start: {self.store}")
            rsp = self.gen_simple_response()
            self.log.info("Persistent store confirmed: " + self.store)
            return True
        except Exception as ex:
            self.log.error(f'Failed to initialize persistent store with ID "{persistent_id}" because: {ex}')
            self.log.error(traceback.format_exc())
            self.report_error("Failed to Init Persistent Store During Start", str(ex))
            rsp = gen_error_response(ex)
            future.set_result(rsp)
            return False
        finally:
            future.set_result(rsp)

    async def kernel_info_request(self, stream, ident, parent):
        """Handle a kernel info request."""
        if not self.session:
            return
        content = {"status": "ok"}
        content.update(self.kernel_info)

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell or process_control.
        buffers, _ = self.extract_and_process_request_trace(parent, -1)

        msg = self.session.send(
            stream, "kernel_info_reply", content, parent, ident, buffers=buffers
        )
        self.log.debug(f'Sent "kernel_info_reply" message: {msg}')

    async def dispatch_shell(self, msg):
        """
        Override of the base class' dispatch_shell method. We completely override it here.

        That is, we do not call the base class' dispatch_shell method at all.
        """
        self.shell_received_at: float = time.time() * 1.0e3

        if self.synclog is not None and hasattr(self.synclog,
                                                "fallback_future_io_loop") and self.synclog.fallback_future_io_loop is None:
            self.synclog.fallback_future_io_loop = asyncio.get_running_loop()

        if not self.session:
            self.log.error("Received SHELL message, but our Session is None...")
            self.log.error(f"Will be discarding SHELL message: {msg}")
            return

        self.log.debug(f'Received new SHELL message. Flushing control queue before processing: {msg}')

        # flush control queue before handling shell requests
        await self._flush_control_queue()

        idents, msg = self.session.feed_identities(msg, copy=False)
        try:
            # Pass False for content so we don't store the digest and get a duplicate_signature error later.
            msg = self.session.deserialize(msg, content=True, copy=False)
        except Exception as ex:
            self.log.error(f"Received invalid SHELL message: {msg}")  # noqa: G201
            self.log.error(f"Shell message is invalid because: {ex}")

            # Just deserialize the beginning so that we can log some helpful error messages.
            min_length = 5
            msg_list = t.cast(t.List[zmq.Message], msg)
            msg_list_beginning = [bytes(msg.bytes) for msg in msg_list[:min_length]]
            msg_list = t.cast(t.List[bytes], msg_list)
            msg_list = msg_list_beginning + msg_list[min_length:]

            message: dict = {}
            header = self.session.unpack(msg_list[1])
            message["header"] = extract_dates(header)
            msg_type = message["msg_type"]
            msg_id = message["msg_id"]

            if self.session.auth is not None:
                signature = msg_list[0]
                if signature:
                    check = self.session.sign(msg_list[1:5])
                    if not compare_digest(signature, check):
                        self.log.error(
                            'Shell "{msg_type}" message with ID="{msg_id}" is invalid due to an incorrect signature.'
                        )
                        self.log.error(
                            "Signature generated from signing frames 1 through 5 of message does not produce "
                            "the signature included within the message."
                        )
                        self.log.error(
                            f'Signature included in message: "{signature}". '
                            f'Signature produced during verification process: "{check}".'
                        )
                        self.log.error("Frames 1 through 5:")
                        for i in range(1, 5, 1):
                            self.log.error(f"Frame #{i}: {msg_list[i]}")
                        self.log.error("All frames of message:")
                        for i, frame in enumerate(msg_list):
                            self.log.error(f"Frame #{i}: {frame}")
            else:
                self.log.error(
                    f'Invalid shell message "{msg_id}" of type "{msg_type}": {msg_list}\n\n'
                )

            message["msg_id"] = header["msg_id"]
            message["msg_type"] = header["msg_type"]
            message["parent_header"] = extract_dates(self.session.unpack(msg_list[2]))
            message["metadata"] = self.session.unpack(msg_list[3])

            # Try to ACK anyway; we'll just have to use incomplete information. But we should be able to get the job done via the identities...
            self.send_ack(
                self.shell_stream,
                msg_type,
                msg_id,
                idents,
                message,
                stream_name="shell",
            )  # Send an ACK.

            self.report_error(
                error_title="Kernel Replica Received an Invalid Shell Message",
                error_message=f"Replica {self.smr_node_id} of Kernel {self.kernel_id} has received an invalid shell message: {ex}.",
            )

            return

        # Set the parent message for side effects.
        self.set_parent(idents, msg, channel="shell")
        self._publish_status("busy", "shell")

        # self.log.info(f"Received SHELL message: {str(msg_deserialized)}")
        msg_id: str = msg["header"]["msg_id"]
        msg_type: str = msg["header"]["msg_type"]
        self.log.debug("=============================================================================================")
        self.log.debug(f'→ Received SHELL message {msg_id} of type "{msg_type}": {msg}')
        self.log.debug("=============================================================================================")
        sys.stderr.flush()
        sys.stdout.flush()
        self.send_ack(self.shell_stream, msg_type, msg_id, idents, msg, stream_name="shell")  # Send an ACK.

        # Only abort execute requests
        if self._aborting and msg_type == "execute_request":
            self.log.warning(
                "We're aborting, and message is an \"execute_request\". Won't be processing it..."
            )
            self._send_abort_reply(self.shell_stream, msg, idents)
            self._publish_status("idle", "shell")
            # flush to ensure reply is sent before
            # handling the next request
            if self.shell_stream:
                self.shell_stream.flush(zmq.POLLOUT)
            return

        if not self.should_handle(self.shell_stream, msg, idents):
            self.log.warning(
                f'We should not handle shell "{msg_type}" message "{msg_id}". Returning.'
            )
            return

        # The first time we call 'extract_and_process_request_trace' for this request.
        # The second time will be in the handler itself.
        buffers, _ = self.extract_and_process_request_trace(msg, self.shell_received_at)

        handler = self.shell_handlers.get(msg_type, None)
        if handler is None:
            self.log.warning('Unknown shell message type: "%r"', msg_type)
        else:
            try:
                self.pre_handler_hook()
            except Exception as ex:
                self.log.debug("Unable to signal in pre_handler_hook:", exc_info=True)
                self.report_error(
                    error_title=f'Kernel "{self.kernel_id}" Encountered {type(ex).__name__} Exception '
                                f'While Signaling in pre_handler_hook for Shell Request "{msg_id}" of '
                                f'type "{msg_type}"',
                    error_message=str(ex),
                )
            try:
                result = handler(self.shell_stream, idents, msg)
                if inspect.isawaitable(result):
                    await result
            except Exception as ex:
                self.log.error(f'Exception in shell message handler for "{msg_type}" message "{msg_id}": {ex}',
                               exc_info=True, )  # noqa: G201
                self.report_error(
                    error_title=f'Kernel "{self.kernel_id}" Encountered {type(ex).__name__} Exception '
                                f'While Executing Handler for Shell Request "{msg_id}" of type "{msg_type}"',
                    error_message=str(ex),
                )
            except KeyboardInterrupt:
                # Ctrl-c shouldn't crash the kernel here.
                self.log.error("KeyboardInterrupt caught in kernel.")
            finally:
                try:
                    self.post_handler_hook()
                except Exception as ex:
                    self.log.debug("Unable to signal in post_handler_hook:", exc_info=True)
                    self.report_error(
                        error_title=f'Kernel "{self.kernel_id}" Encountered {type(ex).__name__} Exception '
                                    f'While Signaling in post_handler_hook for Shell Request "{msg_id}" of '
                                    f'type "{msg_type}"',
                        error_message=str(ex),
                    )

        self.log.debug(f'Finished processing shell message {msg_id} of type "{msg_type}"')
        sys.stderr.flush()
        sys.stdout.flush()

        self._publish_status("idle", "shell")
        # flush to ensure reply is sent before
        # handling the next request
        if self.shell_stream:
            self.shell_stream.flush(zmq.POLLOUT)

    def should_handle(self, stream, msg, idents):
        """Check whether a (shell-channel?) message should be handled"""
        msg_id = msg["header"]["msg_id"]
        msg_type = msg["header"]["msg_type"]
        if msg_id in self.received_message_ids:
            # Check if the message has a "ForceReprocess" field in its metadata frame with a value of "true".
            # If so, then we'll reprocess it anyway -- as these are likely resubmitted 'yield_request' or 'execute_request' messages.
            metadata: dict[str, Any] = msg.get("metadata", {})
            if "force_reprocess" in metadata:
                force_reprocess: bool = metadata["force_reprocess"]
                self.log.warning(
                    f'Received duplicate "{msg_id}" message with ID={msg_type} and "force_reprocess" equal to {force_reprocess}.'
                )
                # Presumably this will be True, but if it isn't True, then we definitely shouldn't reprocess the message.
                return force_reprocess
            else:
                self.log.warning(
                    f'Received duplicate "{msg_id}" message with ID={msg_type} and no '
                    f'"force_reprocess" entry in the request\'s metadata...'
                )

            return False
        else:
            self.received_message_ids.add(msg_id)
        return super().should_handle(stream, msg, idents)

    async def process_control(self, msg):
        """
        Override of `process_control`.

        This method is used to dispatch control requests.

        We overrode it so that we can send ACKs.
        """
        received_at: float = time.time() * 1.0e3

        if not self.session:
            self.log.error("We have no session. Cannot process control message...")
            return

        idents, msg = self.session.feed_identities(msg, copy=False)
        try:
            msg = self.session.deserialize(msg, content=True, copy=False)
        except Exception as ex:
            self.log.error(f"Received invalid CONTROL message: {ex}")  # noqa: G201
            self.kernel_notification_service_stub.Notify(
                gateway_pb2.KernelNotification(
                    title="Kernel Replica Received an Invalid Control Message",
                    message=f"Replica {self.smr_node_id} of Kernel {self.kernel_id} has received an invalid control message: {ex}.",
                    notificationType=ErrorNotification,
                    kernelId=self.kernel_id,
                    replicaId=self.smr_node_id,
                    containerId=self.docker_container_id,
                )
            )

            minlen = 5
            msg_list = t.cast(t.List[zmq.Message], msg)
            msg_list_beginning = [bytes(msg.bytes) for msg in msg_list[:minlen]]
            msg_list = t.cast(t.List[bytes], msg_list)
            msg_list = msg_list_beginning + msg_list[minlen:]

            self.log.error(f"Invalid control message: {msg_list}\n\n")

            message: dict = {}
            header = self.session.unpack(msg_list[1])
            message["header"] = extract_dates(header)
            msg_type = message["msg_type"]
            msg_id = message["msg_id"]

            message["msg_id"] = header["msg_id"]
            message["msg_type"] = header["msg_type"]
            message["parent_header"] = extract_dates(self.session.unpack(msg_list[2]))
            message["metadata"] = self.session.unpack(msg_list[3])

            # Try to ACK anyway; we'll just have to use incomplete information. But we should be able to get the job done via the identities...
            self.send_ack(
                self.control_stream,
                msg_type,
                msg_id,
                idents,
                message,
                stream_name="control",
            )  # Send an ACK.
            if self.control_stream:
                self.control_stream.flush(zmq.POLLOUT)

            return

        header: dict = msg["header"]
        msg_type: str = header["msg_type"]
        msg_id: str = header["msg_id"]

        self.log.debug("=============================================================================================")
        self.log.debug(f'→ Received CONTROL message {msg_id} of type "{msg_type}"')
        self.log.debug("=============================================================================================")
        sys.stderr.flush()
        sys.stdout.flush()

        # The first time we call this method for this request.
        self.extract_and_process_request_trace(msg, received_at)

        # Set the parent message for side effects.
        self.set_parent(idents, msg, channel="control")
        self._publish_status("busy", "control")

        self.send_ack(
            self.control_stream, msg_type, msg_id, idents, msg, stream_name="control"
        )  # Send an ACK.
        if self.control_stream:
            self.control_stream.flush(zmq.POLLOUT)

        if msg_id in self.received_message_ids:
            # Is it safe to assume a msg_id will not be resubmitted?
            return False
        else:
            self.received_message_ids.add(msg_id)

        handler = self.control_handlers.get(msg_type, None)
        if handler is None:
            self.log.error("UNKNOWN CONTROL MESSAGE TYPE: %r", msg_type)
        else:
            try:
                result = handler(self.control_stream, idents, msg)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                self.log.error("Exception in control handler:", exc_info=True)  # noqa: G201

        sys.stdout.flush()
        sys.stderr.flush()
        self._publish_status("idle", "control")

        # flush to ensure reply is sent
        if self.control_stream:
            self.control_stream.flush(zmq.POLLOUT)

        self.log.debug(f'Finished processing control message {msg_id} of type "{msg_type}"')
        sys.stderr.flush()
        sys.stdout.flush()

    async def init_persistent_store(self, code):
        if await self.check_persistent_store():
            return self.gen_simple_response()

        self.log.info('Initializing persistent datastore now using code "%s"' % str(code))

        # By executing code, we can get persistent id later.
        # The execution_count should not be counted and will reset later.
        execution_count = self.shell.execution_count  # type: ignore

        # continue to initialize store
        try:
            # Create future to avoid duplicate initialization
            future = asyncio.Future(loop=asyncio.get_running_loop())
            self.store = future

            # Execute code to get persistent id
            await asyncio.ensure_future(super().do_execute(code, True, store_history=False))
            self.log.info('Successfully executed initialization code: "%s"' % str(code))
            # Reset execution_count
            self.shell.execution_count = execution_count  # type: ignore

            # type: ignore
            async with self._user_ns_lock:
                self.persistent_id = self.shell.user_ns[key_persistent_id]

            self.log.info('Persistent ID set: "%s"' % self.persistent_id)

            # Initialize persistent store
            try:
                self.store = await self.init_persistent_store_with_persistent_id(self.persistent_id)
                self.log.info(f"Persistent store confirmed on start: {self.store}")
                rsp = self.gen_simple_response()
            except Exception as ex:
                self.log.error(f'Failed to initialize persistent store with ID "{self.persistent_id}" because: {ex}')
                self.log.error(traceback.format_exc())
                self.report_error("Failed to Init Persistent Store", str(ex))
                rsp = gen_error_response(ex)

            # Resolve future
            future.set_result(rsp)
            self.log.info("Persistent store confirmed: " + self.store)

            return rsp
        except Exception as e:
            self.log.error("Exception encountered during code execution: %s" % str(e))

            self.shell.execution_count = execution_count  # type: ignore
            err_rsp = gen_error_response(e)

            assert isinstance(self.store, asyncio.Future)
            self.store.set_result(err_rsp)
            self.store = None
            return err_rsp

    async def init_persistent_store_with_persistent_id(self, persistent_id: str) -> str:
        """Initialize persistent store with persistent id. Return store path."""
        assert isinstance(self.storage_base, str)
        self.store_path: str = os.path.join(self.storage_base, "store", persistent_id)

        self.log.info(f'Initializing the Persistent Store with Persistent ID: "{persistent_id}"')
        self.log.debug('Full path of Persistent Store: "%s"' % self.store_path)
        self.log.debug("Disabling `outstream` now.")

        sys.stderr.flush()
        sys.stdout.flush()

        # Disable outstream
        self.toggle_outstream(override=True, enable=False)

        self.log.debug("Disabled `outstream`.")
        self.log.debug("Overriding shell hooks now.")

        # Override shell hooks
        await self.override_shell()

        self.log.debug("Overrode shell hooks.")

        try:
            # Get synclog for synchronization.
            sync_log: SyncLog = await self.get_synclog(self.store_path)
        except Exception as ex:
            self.log.error("Creation of SyncLog failed: %s" % str(ex))
            self.log.error(traceback.format_exc())
            # re-raise
            raise ex

        self.init_raft_log_event.set()

        # Start the synchronizer.
        # Starting can be non-blocking, call synchronizer.ready() later to confirm the actual execution_count.
        try:
            self.synchronizer: Optional[Synchronizer] = Synchronizer(
                sync_log,
                store_path=self.store_path,
                module=self.shell.user_module,
                opts=CHECKPOINT_AUTO,
                num_replicas=self.num_replicas,
                node_id=self.smr_node_id,
                large_object_pointer_committed=self.large_object_pointer_committed,
                remote_checkpointer=self._remote_checkpointer,
                report_error_callback=self.report_error,
            )  # type: ignore
        except Exception as ex:
            self.log.error("Creation of Synchronizer failed: %s" % str(ex))
            self.log.error(traceback.format_exc())
            # re-raise
            raise ex

        if isinstance(sync_log, RaftLog):
            sync_log.set_fast_forward_executions_handler(self.synchronizer.fast_forward_execution_count)
            sync_log.set_set_execution_count_handler(self.synchronizer.set_execution_count)

        self.init_synchronizer_event.set()

        self.synchronizer.start()

        self.start_synchronizer_event.set()

        sys.stderr.flush()
        sys.stdout.flush()

        # If we're not using real GPUs, then we can simulate downloading the model and training data here.
        if self.simulate_training_using_sleep:
            download_model_dur, download_training_data_dur = await self.simulate_download_model_and_training_data()

            if download_model_dur > 0 and self.prometheus_enabled:
                self.remote_storage_read_latency_milliseconds.labels(
                    session_id=self.kernel_id,
                    workload_id=self.workload_id
                ).observe(download_model_dur * 1e3)

                self.delay_milliseconds.labels(
                    session_id=self.kernel_id,
                    workload_id=self.workload_id).inc(download_model_dur * 1e3)

                if self.current_execution_stats is not None:
                    self.current_execution_stats.download_model_microseconds = (download_model_dur * 1.0e6)

            if download_training_data_dur > 0 and self.prometheus_enabled:
                self.remote_storage_read_latency_milliseconds.labels(
                    session_id=self.kernel_id,
                    workload_id=self.workload_id
                ).observe(download_training_data_dur * 1e3)

                self.delay_milliseconds.labels(
                    session_id=self.kernel_id,
                    workload_id=self.workload_id
                ).inc(download_training_data_dur * 1e3)

                if self.current_execution_stats is not None:
                    self.current_execution_stats.download_training_data_microseconds = \
                        download_training_data_dur * 1.0e6

        # We do this here (and not earlier, such as right after creating the RaftLog), as the RaftLog needs to be
        # started before we attempt to catch-up. The catch-up process involves appending a new value and waiting until
        # it gets committed. This cannot be done until the RaftLog has started. And the RaftLog is started by the
        # Synchronizer, within Synchronizer::start.
        if self.synclog.needs_to_catch_up:
            self.log.debug('Need to restore user namespace via the SyncLog.')

            try:
                await self.synclog.catchup_with_peers()
            except Exception as ex:
                self.log.error("Error whilst catching up with peer replicas: %s" % str(ex))
                self.log.error(traceback.format_exc())
                self.report_error("Error While Catching Up with Peer Replicas", str(ex))
                raise ex  # Re-raise

            try:
                await self.__download_pointers_committed_while_catching_up()
            except Exception as ex:
                self.log.error("Error whilst downloading pointers committed while catching up: %s" % str(ex))
                self.log.error(traceback.format_exc())
                self.report_error("Error While Downloading Committed Pointers", str(ex))
                raise ex  # Re-raise

        # Send the 'smr_ready' message AFTER we've caught-up with our peers (if that's something that we needed to do).
        await self.send_smr_ready_notification()

        self.log.info("Started Synchronizer.")

        self.init_persistent_store_event.set()

        # TODO(Ben): Should this go before the "smr_ready" send?
        # It probably shouldn't matter -- or if it does, then more synchronization is required.
        async with self.persistent_store_cv:
            self.log.debug("Calling `notify_all` on the Persistent Store condition variable.")
            self.persistent_store_cv.notify_all()

        self.log.info(f'Successfully initialized persistent store with ID "{persistent_id}".')

        return self.store_path

    def get_most_recently_used_remote_storage(self) -> Optional[SimulatedCheckpointer]:
        """
        Return the SimulatedCheckpointer that was most recently used for any simulated network I/O operation.

        If there are no remote storages registered, then this will return None.
        """
        if self.remote_storages is None or len(self.remote_storages) == 0:
            return None

        last_io_timestamp: float = float("-inf")
        most_recently_used_checkpointer: Optional[SimulatedCheckpointer] = None
        for name, checkpointer in self.remote_storages.items():
            if checkpointer.last_io_timestamp > last_io_timestamp:
                last_io_timestamp = checkpointer.last_io_timestamp
                most_recently_used_checkpointer = checkpointer

        # We either should've identified a SimulatedCheckpointer to return, or we should have no
        # SimulatedCheckpointer objects registered.
        #
        # Even if no SimulatedCheckpointer objects were ever used, their last_io_timestamp field is initialized
        # to -1, whereas our local last_io_timestamp variable is initialized to negative infinity, so we should've
        # selected the first SimulatedCheckpointer we encountered if it is the case that no registered
        # SimulatedCheckpointer objects have ever been used.
        assert (
                most_recently_used_checkpointer is not None
                or len(self.remote_storages) == 0
        )

        return most_recently_used_checkpointer

    async def check_persistent_store(self):
        """Check if persistent store is ready. If initializing, wait. The future return True if ready."""
        store = self.store
        if store is None:
            return False
        elif inspect.isawaitable(store):
            response = await store
            return response["status"] == "ok"
        else:
            return True

    def send_ack(
            self, stream, msg_type: str, msg_id: str, ident, parent, stream_name: str = ""
    ):
        # If message acknowledgements are disabled, then just return immediately.
        if not self.message_acknowledgements_enabled:
            return

        ack_msg = self.session.send(  # type:ignore[assignment]
            stream,
            "ACK",
            {"type": msg_type, "msg_id": msg_id, "source": "PYTHON KERNEL"},
            parent,
            ident=ident,
        )
        self.log.debug(
            f"Sent 'ACK' for {stream_name} {msg_type} message \"{msg_id}\": {ack_msg}. Idents: {ident}"
        )

    def register_remote_storage_definition_from_dict(
            self, remote_storage_definition: Dict[str, Any]
    ) -> Optional[str]:
        if len(remote_storage_definition) == 0:
            return None

        remote_storage_name: str = remote_storage_definition.get("name", "")

        if remote_storage_name == "":
            self.log.warning(
                f"Received non-empty remote storage definition with no name: {remote_storage_definition}"
            )
            return None

        if remote_storage_name in self.remote_storages:
            self.log.debug(
                f'Remote remote_storage "{remote_storage_name}" is already registered.'
            )
            return remote_storage_name

        remote_storage: SimulatedCheckpointer = SimulatedCheckpointer(
            **remote_storage_definition
        )
        self.remote_storages[remote_storage.name] = remote_storage

        self.log.debug(
            f'Successfully registered new remote storage from dictionary definition: "{remote_storage_name}".'
        )

        return remote_storage_name

    def register_remote_storage_definition(
            self, remote_storage_definition: Dict[str, Any] | SimulatedCheckpointer
    ) -> Optional[str]:
        """
        Convert the remote storage definition that was extracted from the metadata of an "execute_request" or
        "yield_request" message to a SimulatedCheckpointer object and store the new SimulatedCheckpointer object
        in our remote_storages mapping.

        Returns:
            the name of the included remote storage, or None if no remote storage was included.
            the name is returned regardless of whether the remote storage had already been registered or not.
        """
        if isinstance(remote_storage_definition, dict):
            return self.register_remote_storage_definition_from_dict(
                remote_storage_definition
            )

        remote_storage_name: str = remote_storage_definition.name
        if remote_storage_name in self.remote_storages:
            self.log.debug(
                f'Remote remote_storage "{remote_storage_name}" is already registered.'
            )
            return remote_storage_name

        self.remote_storages[remote_storage_name] = remote_storage_definition

        self.log.debug(
            f'Successfully registered new remote storage: "{remote_storage_name}".'
        )

        return remote_storage_name

    def __validate_resource_spec_metadata(self, metadata: Dict[str, Any]):
        """
        Validate the 'resource_request' and 'required-X' entries (where X is 'gpus', 'millicpus', 'memory-mb',
        or 'vram-gb'). They should be consistent with each other.
        """
        resource_request: Optional[Dict[str, Any]] = metadata["resource_request"]
        if resource_request is None:
            return

        required_keys: list[str] = ['required-millicpus', 'required-memory-mb', 'required-gpus', 'required-vram-gb']
        resource_request_keys: list[str] = ['cpus', 'memory', 'gpus', 'vram']

        for required_key, resource_request_key in list(zip(required_keys, resource_request_keys)):
            # If either key is not present, then skip it.
            if required_key not in metadata or resource_request_key not in resource_request:
                continue

            if metadata[required_key] != resource_request[resource_request_key]:
                self.log.warning(f"Inconsistent '{required_key}' field ({metadata[required_key]}) "
                                 f"and current resource request '{resource_request_key}' "
                                 f"({resource_request[resource_request_key]}).")

                self.report_warning(f"Inconsistent '{required_key}' Field and "
                                    f"Resource Request '{resource_request_key}' Field",
                                    f"Inconsistent '{required_key}' field ({metadata[required_key]}) "
                                    f"and current resource request '{resource_request_key}' "
                                    f"({resource_request[resource_request_key]}).")

                # raise ValueError(f"Inconsistent '{required_key}' field ({metadata[required_key]}) "
                #                  f"and current resource request '{resource_request_key}' "
                #                  f"({resource_request[resource_request_key]}).")

    async def process_execute_request_metadata(
            self, msg_id: str, msg_type: str, metadata: Dict[str, Any]
    ) -> tuple[Optional[str], list[int]]:
        """
        Process the metadata included in a shell "execute_request" or "yield_request" message.

        :return: a tuple where the first element is the remote storage name and the second is a list of GPU device IDs.
        """
        self.log.debug(f'Processing metadata of "{msg_type}" request "{msg_id}": {metadata}')

        if metadata is None:
            return None, []

        resource_request: Dict[str, Any] = metadata.get("resource_request", {})

        remote_storage_definition: Dict[str, Any] = metadata.get("remote_storage_definition", {})

        workload_id: str = metadata.get("workload_id", "")

        if len(resource_request) > 0:
            self.log.debug(f'Extracted ResourceRequest from "{msg_type}" metadata: {resource_request}')

            self.resource_requests.append(resource_request)
            self.current_resource_request = resource_request
            self.__validate_resource_spec_metadata(metadata)

        remote_storage_name: Optional[str] = None
        if len(remote_storage_definition) > 0:
            self.log.debug(f'Extracted ResourceRequest from "{msg_type}" metadata: {remote_storage_definition}')

            remote_storage_name = self.register_remote_storage_definition(remote_storage_definition)

        if len(workload_id) > 0:
            self.log.debug(f'Extracted workload ID from "{msg_type}" metadata: {workload_id}')

        gpu_device_ids: list[int] = metadata.get("gpu_device_ids", [])

        return remote_storage_name, gpu_device_ids

    async def set_checkpointing_state(self, val: bool) -> None:
        """
        This method updates the value of the 'checkpoint_state' field.

        This method should only be called within the Control thread's IO loop.
        """
        self.log.debug(f"Setting value of 'Checkpointing State' flag from {self.checkpointing_state} to {val}.")

        async with self.checkpointing_state_cv:
            self.checkpointing_state = val
            self.log.debug(f"Set value of 'Checkpointing State' flag to {val}.")

            if not self.checkpointing_state:
                self.log.debug(f"Notifying any waiters of the 'Checkpointing State' CV.")
                self.checkpointing_state_cv.notify_all()

    async def execute_request(self, stream, ident, parent):
        """Override for receiving specific instructions about which replica should execute some code."""
        start_time: float = time.time()

        parent_header: dict[str, Any] = extract_header(parent)

        self.log.debug(f"execute_request called for message with msg_id=\"{parent_header['msg_id']}\". "
                       f"identity frame(s): {str(ident)}")

        self.next_execute_request_msg_id: str = parent_header["msg_id"]

        parent_header = extract_header(parent)
        self._associate_new_top_level_threads_with(parent_header)

        if not self.session:
            self.log.error("We don't have a Session. Cannot process 'execute_request'.")

            # Commented-out because it's unclear if we should reset the execution stats here or not...
            #
            # Reset the current ExecutionStats object.
            # self.current_execution_stats = ExecutionStats()

            return

        try:
            content = parent["content"]
            code = content["code"]
            silent = content.get("silent", False)
            store_history = content.get("store_history", not silent)
            user_expressions = content.get("user_expressions", {})
            allow_stdin = content.get("allow_stdin", False)
            cell_meta = parent.get("metadata", {})
            cell_id = cell_meta.get("cellId")
        except Exception as ex:
            self.log.error("Got bad msg: ")
            self.log.error("%s", parent)
            self.report_error('Got Bad "execute_request" Message',
                              f"Error: {ex}. Message: {parent}")

            # Commented-out because it's unclear if we should reset the execution stats here or not...
            # 
            # Reset the current ExecutionStats object.
            # self.current_execution_stats = ExecutionStats()

            return

        stop_on_error = content.get("stop_on_error", True)

        metadata = self.init_metadata(parent)
        metadata.update(parent["metadata"])

        self.log.debug(f'"execute_request" metadata entries ({len(metadata)}):')
        for k, v in metadata.items():
            self.log.debug(f'"{k}" (valtype={type(v).__name__}): {v}')

        # Process the metadata included in the request.
        # If we get back a remote storage name, then we'll use it to simulate I/O after we finish the execution.
        remote_storage_name, gpu_device_ids, = await self.process_execute_request_metadata(
            parent_header["msg_id"], parent_header["msg_type"], metadata)

        training_duration_millis: float = -1
        if "training_duration_millis" in metadata:
            try:
                training_duration_millis = float(metadata["training_duration_millis"])
            except ValueError:
                pass

        target_model: Optional[str] = metadata.get("model", ResNet18.model_name())
        target_dataset: Optional[str] = metadata.get("dataset", CIFAR10.dataset_name())
        batch_size: Optional[int] = metadata.get("batch_size", 1)
        target_replica_id: int = metadata.get("target_replica_id", -1)

        # Re-broadcast our input for the benefit of listening clients, and
        # start computing output
        if not silent:
            self.execution_count += 1
            self._publish_execute_input(code, parent, self.execution_count)

        # Call do_execute with the appropriate arguments
        reply_content: Dict[str, Any] | Awaitable[Dict[str, Any]] = self.do_execute(
            code=code,
            silent=silent,
            store_history=store_history,
            user_expressions=user_expressions,
            allow_stdin=allow_stdin,
            remote_storage_name=remote_storage_name,
            cell_meta=cell_meta,
            cell_id=cell_id,
            training_duration_millis=training_duration_millis,
            gpu_device_ids=gpu_device_ids,
            deep_learning_model_name=target_model,
            execute_request_metadata=metadata,
            dataset=target_dataset,
            batch_size=batch_size,
            parent_header=parent_header,
            target_replica_id=target_replica_id,
            parent=parent,
        )

        if inspect.isawaitable(reply_content):
            reply_content = await reply_content

        # Flush output before sending the reply.
        sys.stdout.flush()
        sys.stderr.flush()
        # FIXME: on rare occasions, the flush doesn't seem to make it to the clients...
        #        This seems to mitigate the problem, but we definitely need to better understand what's going on.
        if self._execute_sleep:
            time.sleep(self._execute_sleep)

        term_number: int = -1
        was_primary_replica: bool = False
        if LEADER_KEY in reply_content:
            was_primary_replica = True
            del reply_content[LEADER_KEY]

            performed_dl_training: bool = reply_content.pop("performed_dl_training", False)
            term_number: int = reply_content.pop("term_number", -1)
            code: Optional[str] = reply_content.pop("code", None)

            assert term_number >= 0
            assert code is not None

            await self.handle_post_execution_overheads(
                remote_storage_name=remote_storage_name,
                performed_dl_training=performed_dl_training,
                code=code,
            )

        # Send the reply.
        reply_content = jsonutil.json_clean(reply_content)
        metadata = self.finish_metadata(parent, metadata, reply_content)

        if self.smr_enabled:
            # Embed the election metadata if we've been configured to do so.
            current_election: Election = self.synchronizer.current_election
            if current_election is not None and self.include_election_metadata:
                metadata["election_metadata"] = current_election.get_election_metadata()
                metadata["election_metadata"]["leader_term"] = self.synclog.leader_term
                metadata["election_metadata"]["leader_id"] = self.synclog.leader_id

            # If we weren't the lead replica, then we didn't recover the term number up above, so
            # let's get the current term number from the election object.
            if term_number == -1:
                term_number = current_election.term_number

            # If SMR is enabled, then we send the response now.
            self.log.debug("Sending 'execute_reply' message now.")

            # Send the reply now.
            buffers, _ = self.extract_and_process_request_trace(
                parent, -1, execution_stats=self.current_execution_stats
            )
            reply_msg: t.Dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
                stream,
                "execute_reply",
                reply_content,
                parent,
                metadata=metadata,
                ident=ident,
                buffers=buffers,
            )

            self.log.debug(f'Sent "execute_reply" message: {reply_msg}')

        # Synchronize updated state if we executed the user-submitted code.
        if was_primary_replica:
            await self.synchronize_updated_state_and_notify_execution_complete(
                term_number = term_number,
                parent_header = parent_header,
            )
        else:
            self.log.debug(f"We were not the primary replica for term {term_number}. Skipping synchronization step.")

        # Record synchronization and checkpointing overhead.
        # For replica-based approaches, this won't be included in what is sent back to the client.
        # But we will update the Prometheus metrics (if Prometheus is enabled).

        # Add the time from the synchronizer (so, the overhead of calling `append` on the SyncLog by the synchronizer)
        self.current_execution_stats.upload_model_and_training_data_microseconds += \
            (self.synchronizer.synchronization_time_seconds * 1.0e6)

        # Add the `write_time` from the remote checkpointer.
        self.current_execution_stats.upload_model_and_training_data_microseconds += \
            (self._remote_checkpointer.storage_provider.write_time * 1.0e6)

        # Add the time to load and apply serialized state from remote storage.
        self.current_execution_stats.download_model_microseconds += \
            (self.synclog.restoration_time_seconds * 1.0e6)

        # For SMR-based policies, the "restore namespace" time is how long it takes to "catch up".
        # For other policies, the "restore namespace" time is the time taken to read the data from remote storage.
        if self.smr_enabled and self.num_replicas > 1:
            self.current_execution_stats.download_model_microseconds += \
                (self.synclog.restore_namespace_time_seconds * 1.0e6)
        else:
            self.current_execution_stats.download_model_microseconds += \
                (self.synclog.restore_namespace_time_seconds * 1.0e6)

        self.current_execution_stats.download_model_microseconds += \
            (self._remote_checkpointer.storage_provider.read_time * 1.0e6)

        # If prometheus is enabled, then publish the write latencies from the state synchronization to prometheus
        # (though it may or may not be scraped correctly for policies in which containers only exist for a single
        # training event due to the short-lived nature of those containers...)
        if self.prometheus_enabled:
            self.remote_storage_write_latency_milliseconds.labels(
                session_id=self.kernel_id, workload_id=self.workload_id
            ).observe(self._remote_checkpointer.storage_provider.write_time * 1e3)

            for write_time_sec in self.synchronizer.synchronization_times:
                self.remote_storage_write_latency_milliseconds.labels(
                    session_id=self.kernel_id, workload_id=self.workload_id
                ).observe(write_time_sec * 1e3)

        if not silent and reply_msg["content"]["status"] == "error" and stop_on_error:
            self._abort_queues()

        await_election_end_task: Optional[asyncio.Task] = None
        if self.smr_enabled and not was_primary_replica:
            await_election_end_task = asyncio.create_task(self.synchronizer.wait_for_election_to_end(term_number))

            # We need to save a reference to this task to prevent it from being garbage collected mid-execution.
            # See the docs for details: https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
            self.background_tasks.add(await_election_end_task)

            # To prevent keeping references to finished tasks forever, we make each task remove its own reference
            # from the "background tasks" set after completion.
            await_election_end_task.add_done_callback(self.background_tasks.discard)

            # Wait for the task to end. By not returning here, we ensure that we cannot process any additional
            # "execute_request" messages until all replicas have finished.

            # TODO: Might there still be race conditions where one replica starts processing a future "execute_request"
            #       message before the others, and possibly starts a new election and proposes something before the
            #       others do?
            self.log.debug(f"Waiting for election {term_number} "
                           "to be totally finished before returning from execute_request function.")

        # If we're supposed to write-after-execute, but the write operation is not supposed to be on the critical path,
        # then we'll perform the write now, as we already sent the "execute_reply" back to the Local Daemon. That will
        # allow the Local Daemon to release our GPU resources, and we can now perform the write operation off of the
        # critical path. The write operation will still block future executions from running if they arrive during the
        # write operation, but that's correct.
        if (
                self.simulate_training_using_sleep
                and remote_storage_name is not None
                and self.simulate_write_after_execute
                and not self.simulate_write_after_execute_on_critical_path
        ):
            self.log.debug(f"Performing post-execution simulated network write "
                           f"operation to '{remote_storage_name}' off of critical path.")

            duration: float = await self.simulate_remote_checkpointing(remote_storage_name, io_type="upload")

            # TODO: What if we receive next message before this completes?
            if duration > 0 and self.prometheus_enabled and hasattr(self, "remote_storage_write_latency_milliseconds"):
                self.remote_storage_write_latency_milliseconds.labels(
                    session_id=self.kernel_id, workload_id=self.workload_id
                ).observe(duration * 1e3)

        # If SMR is disabled, then we send the "execute_reply" now.
        if not self.smr_enabled:
            self.log.debug("Sending 'execute_reply' message now.")

            # Send the reply now.
            buffers, _ = self.extract_and_process_request_trace(
                parent, -1, execution_stats=self.current_execution_stats
            )

            reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
                stream,
                "execute_reply",
                reply_content,
                parent=parent,
                metadata=metadata,
                ident=ident,
                buffers=buffers,
            )

            self.log.debug(f'Sent "execute_reply" message: {reply_msg}')
        else:
            # smr is enabled
            #
            # Update the value of the 'checkpointing_state' flag on the control thread's IO loop, so that
            # the 'checkpointing_state_cv' is accessed only on the control thread's IO loop.
            future = asyncio.run_coroutine_threadsafe(
                self.set_checkpointing_state(False),
                loop=self.control_thread.io_loop.asyncio_loop
            )

            future.result()

            execute_request_id: str = parent["header"]["msg_id"]

            # Make sure the 'buffers' variable is defined.
            buffers, request_trace = self.extract_and_process_request_trace(
                parent, -1, execution_stats=self.current_execution_stats
            )

            # If SMR is enabled, then we send an IO pub message with the completed execute trace.
            self.session.send(
                self.iopub_socket,
                IOPubNotification.ExecuteStatistics,
                {
                    "execute_reply_content": reply_content,
                    "kernel_id": self.kernel_id,
                    "replica_id": self.smr_node_id,
                },
                parent=parent,
                metadata=metadata,
                buffers=buffers,
                ident=self._topic(IOPubNotification.ExecuteStatistics),
            )

            self.log.debug(f'Sent IOPub message containing completed execution '
                           f'statistics for execution "{execute_request_id}".')

            if not was_primary_replica:
                assert await_election_end_task is not None
                await await_election_end_task

        end_time: float = time.time()
        duration_ms: float = (end_time - start_time) * 1.0e3

        if self.prometheus_enabled:
            self.execute_request_latency.observe(duration_ms)

        # Reset the current ExecutionStats object.
        self.current_execution_stats = ExecutionStats()

        self.synchronizer.clear_sync_time()
        self._remote_checkpointer.storage_provider.clear_statistics()
        self.synclog.clear_restoration_time()
        self.synclog.clear_restore_namespace_time_seconds()

    async def synchronize_updated_state_and_notify_execution_complete(
            self,
            term_number: int,
            parent_header: Dict[str, Any]
    ):
        self.log.debug(f"We were the primary replica for term {term_number}. Synchronizing updated state/AST.")

        # Update the value of the 'checkpointing_state' flag on the control thread's IO loop, so that
        # the 'checkpointing_state_cv' is accessed only on the control thread's IO loop.
        future = asyncio.run_coroutine_threadsafe(
            self.set_checkpointing_state(True),
            loop=self.control_thread.io_loop.asyncio_loop
        )

        # Wait for the above to finish.
        future.result()

        execute_request_id: str = parent_header["msg_id"]

        try:
            sync_start: float = time.time()
            # Synchronize the term's AST. For multi-replica policies, this will append and commit state to the RaftLog.
            # For single-replica policies, this will persist the AST and any variables to remote storage, namely AWS S3
            # or Redis, depending on the system's configuration.
            await asyncio.wait_for(self.synchronize_updated_state(term_number, execute_request_id), timeout = 120)

            self.current_execution_stats.synchronize_updated_state_time_millis = (time.time() - sync_start) * 1.0e3
        except TimeoutError as ex:
            self.log.error(f'Timed-out while synchronizing updated state for term {term_number} '
                           f'and execution "{execute_request_id}": {ex}')
            self.log.error(traceback.format_exc())
            self.report_error(f'Failed to Synchronize Updated State in Term {term_number} '
                              f'for Execution "{execute_request_id}"', str(ex))
        except Exception as ex:
            self.log.error(f'Error while synchronizing updated state for term {term_number} '
                           f'and execution "{execute_request_id}": {ex}')
            self.log.error(traceback.format_exc())
            self.report_error(f'Failed to Synchronize Updated State in Term {term_number} '
                              f'for Execution "{execute_request_id}"', str(ex))

        notify_complete_task: Optional[asyncio.Task] = None
        commit_notify_exec_complete_start: float = time.time()
        try:
            # The effect of this call depends upon whether we're a single-replica or multi-replica deployment.
            #
            # For multi-replica deployments, this will notify the follower/non-primary replicas that we're done executing
            # the user-submitted code, and that they're up-to-date in terms of receiving state updates from the RaftLog.
            #
            # For single-replica deployments, this will prompt the synchronizer to write a list of keys to remote storage
            # (again, either Redis or AWS S3) at a deterministic key based on our persistent ID. This list of keys is used
            # if and when we (this kernel) is recreated in a new container for a future execution. Specifically, we'll
            # read the list of keys, and then we'll read the data for each key in the list. Doing so will restore our
            # runtime state.
            notify_complete_task = await self.schedule_notify_execution_complete(term_number)
        except Exception as ex:
            self.log.error(f'Error while scheduling "Execution Complete" notification for term {term_number} '
                           f'and execution "{execute_request_id}": {ex}')
            self.log.error(traceback.format_exc())
            self.report_error(f'Failed to Schedule "Execute Complete" Notification {term_number} '
                              f'for Execution "{execute_request_id}"', str(ex))

        # Wait for the notification to be committed to the RaftLog.
        if notify_complete_task is not None:
            try:
                await asyncio.wait([notify_complete_task], return_when=asyncio.FIRST_COMPLETED)
                commit_notify_exec_complete_dur_millis = (time.time() - commit_notify_exec_complete_start) * 1.0e3
                self.current_execution_stats.commit_exec_end_millis = commit_notify_exec_complete_dur_millis
            except Exception as ex:
                self.log.error(
                    f'Error while waiting for commitment of "Execution Complete" notification for term {term_number} '
                    f'and execution "{execute_request_id}" to RaftLog: {ex}')
                self.log.error(traceback.format_exc())
                self.report_error(f'Potential Failure to Commit "Execute Complete" Notification {term_number} '
                                  f'for Execution "{execute_request_id}"', str(ex))

    async def __reset_user_namespace_state(self) -> Tuple[dict, bool]:
        """
        Reset the user namespace.
        :return:
        """
        self.log.warning(f"Resetting user namespace. Current size: {len(self.shell.user_ns)} variable(s).")
        previous_var_names: set[str] = set(self.shell.user_ns.keys())

        async with self._user_ns_lock:
            prev_size: int = len(self.shell.user_ns)

            # Remove any datasets downloaded to the container's file system.
            for var_name, var_value in self.shell.user_ns.items():
                if isinstance(var_value, CustomDataset):
                    self.log.debug(f'Found custom dataset variable "{var_name}" '
                                   f'of type "{type(var_value).__name__}" in user namespace.')

                    var_value.remove_local_files()
                elif isinstance(var_value, DatasetPointer):
                    self.log.debug(f'Found custom dataset pointer variable "{var_name}" '
                                   f'for dataset "{var_value.dataset_name}" in user namespace.')

                    if var_value.dataset is not None:
                        var_value.dataset.remove_local_files()

            # Clear the user namespace
            self.shell.user_ns.clear()

            # Restore essential built-ins
            self.shell.init_user_ns()

        self.log.warning(f"The user namespace has been reset. "
                         f"Removed {prev_size - len(self.shell.user_ns)} "
                         f"variable(s): {', '.join([x for x in previous_var_names if x not in self.shell.user_ns])}")

        return {
            "status": "ok",
            "id": self.smr_node_id,
            "kernel_id": self.kernel_id,
        }, True

    def __get_prewarm_container_id(self, prewarm_container_id: Optional[str]) -> str:
        """
        Returns the previous prewarm container ID used by this kernel, or generates a new one if there is no valid
        previous prewarm container ID.

        :param prewarm_container_id: value to use as the prewarm container ID (if it is non-nil and non-empty).
        """
        if prewarm_container_id is not None and prewarm_container_id != "":
            self.log.debug(f'Using supplied prewarm container ID: "{prewarm_container_id}"')
            return prewarm_container_id

        if not self.was_prewarmed_container or not hasattr(self, "prewarm_container_id"):
            prewarm_container_id = str(uuid.uuid4())
            self.log.debug(f'Generated new prewarm container ID: "{prewarm_container_id}"')
            return prewarm_container_id

        if self.prewarm_container_id is None or self.prewarm_container_id == "":
            prewarm_container_id = str(uuid.uuid4())
            self.log.debug(f'Generated new prewarm container ID: "{prewarm_container_id}"')
            return prewarm_container_id

        self.log.debug(f'Reusing previous new prewarm container ID: "{self.prewarm_container_id}"')
        return self.prewarm_container_id

    async def __revert_to_prewarm(self, prewarm_container_id: Optional[str] = None) -> Tuple[dict, bool]:
        """
        Revert to prewarm mode.

        :param prewarm_container_id: prewarm kernel ID to use. If unspecified, then a random ID will be generated.
        """
        self.log.debug(f"Reverting to PREWARM mode.")

        prewarm_container_id = self.__get_prewarm_container_id(prewarm_container_id)

        # When using RaftLog, we must first stop the SyncLog (RaftLog) before copying the data directory.
        #
        # Reference: https://etcd.io/docs/v2.3/admin_guide/#member-migration

        # Step 1: stop the sync log
        intermediate_resp, ok = await self.__close_synclog()
        if not ok:
            self.log.error(f"Failed to close SyncLog while reverting to PREWARM. "
                           f"Intermediate response: {intermediate_resp}")
            return intermediate_resp, False

        # Step 2: copy the data directory to RemoteStorage
        intermediate_resp, ok = await self.__write_synclog_data_dir_to_remote_storage()
        if not ok:
            self.log.error(f"Failed to write SyncLog data directory to remote storage while reverting to PREWARM. "
                           f"Intermediate response: {intermediate_resp}")
            await self.__close_synclog_remote_storage_client()
            return intermediate_resp, False

        # Step 3: close the SyncLog's remote storage client if we haven't done so already.
        if isinstance(self.synclog, RaftLog):
            # We'll ignore any errors at this stage.
            await self.__close_synclog_remote_storage_client()

        self.log.debug("Closing Synchronizer")

        if hasattr(self, "synchronizer") and self.synchronizer is not None:
            self.synchronizer.close()

        self.synchronizer: Optional[Synchronizer] = None
        self.synclog: Optional[SyncLog|RemoteStorageLog|RaftLog] = None

        self.smr_node_id = 0
        self.kernel_id = prewarm_container_id
        self.prewarm_container_id = prewarm_container_id
        self.was_prewarmed_container = True
        self.prewarm_container = True

        await self.__reset_user_namespace_state()

        return {
            "status": "ok",
            "id": 0,
            "kernel_id": prewarm_container_id,
            "prewarm_container": True,
        }, True

    async def __handle_start_synclog_request(self, parent) -> Dict[str, str]:
        if self.persistent_id is None or self.persistent_id == "":
            content = parent.get("content", {})
            self.persistent_id = content.get("persistent_id", "")

        if self.persistent_id is None or self.persistent_id == "":
            self.log.error("start_synclog_request: persistent ID is invalid. Cannot start.")
            reply_content: Dict[str, str] = {
                "status": "error",
                "ename": "SyncLogNull",
                "evalue": "cannot create and start SyncLog because persistent ID is invalid"
            }

            return reply_content

        self.log.debug(f'Starting SyncLog. PersistentID="{self.persistent_id}"')

        # Create future to avoid duplicate initialization
        future: asyncio.Future = asyncio.Future(loop=asyncio.get_running_loop())
        self.store = future

        try:
            self.store = await self.init_persistent_store_with_persistent_id(self.persistent_id)
            rsp = self.gen_simple_response()
        except Exception as ex:
            self.log.error(f"Failed to initialize persistent store during start-synclog request: {ex}")
            self.log.error(traceback.format_exc())
            self.report_error("Failed to Init Persistent Store While Handling 'Start SyncLog' Request", str(ex))
            rsp = gen_error_response(ex)

        future.set_result(rsp)
        self.log.info("Persistent store confirmed: " + self.store)

        reply_content: Dict[str, str] = {"status": "ok"}
        return reply_content

    async def start_synclog_request(self, stream, ident, parent):
        """
        Our Local Scheduler will send us a "start_synclog_request" message when it is time for us to start our
        SyncLog. This occurs during migration operations.

        Normally, we start the SyncLog in DistributedKernel::start.

        However, if this kernel replica was created during a migration, then we must wait to start
        the SyncLog until we're explicitly directed to do so. This is because we must wait for the
        old replica to finish checkpointing its state, and for the old replica's peer address to
        be updated to our address.
        """
        self.log.debug("start_synclog_request: Starting SyncLog now.")

        reply_content: Dict[str, str] = await self.__handle_start_synclog_request(parent)

        buffers, _ = self.extract_and_process_request_trace(parent, -1)
        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "start_synclog_reply",
            reply_content,
            parent,
            metadata={},
            buffers=buffers,
            ident=ident,
        )

        self.log.debug(f'Sent "start_synclog_reply" message: {reply_msg}')

    async def reset_kernel_request(self, stream, ident, parent):
        """
        reset_request is used to reset the user namespace/state of the DistributedKernel.

        It can optionally be used to revert a "standard" DistributedKernel back to a "prewarm" DistributedKernel.
        """
        content = parent.get("content", {})
        self.log.debug(f"Received RESET request\n{json.dumps(content, indent=2)}")

        revert_to_prewarm: bool = content.get("revert_to_prewarm", False)
        kernel_id: Optional[str] = content.get("kernel_id", None)

        # If we're supposed to revert to prewarm, then do that.
        # Otherwise, we'll just reset the user namespace.
        if revert_to_prewarm:
            reply_content, ok = await self.__revert_to_prewarm(prewarm_container_id=kernel_id)

            # Make sure this entry is present.
            if ok:
                reply_content["prewarm_container"] = True
        else:
            reply_content, ok = await self.__reset_user_namespace_state()

            # Make sure this entry is present.
            if ok:
                reply_content["prewarm_container"] = False

        buffers, _ = self.extract_and_process_request_trace(parent, -1)
        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "reset_kernel_reply",
            reply_content,
            parent,
            metadata={},
            buffers=buffers,
            ident=ident,
        )

        self.log.debug(f'Sent "reset_kernel_reply" message: {reply_msg}')

    async def promote_prewarm_request(self, stream, ident, parent):
        """
        Handle requests to promote ourselves to a standard container.

        :param stream:
        :param ident:
        :param parent:
        :return:
        """
        content = parent.get("content", {})
        self.log.debug(f"Received PROMOTION request\n{json.dumps(content, indent=2)}")

        if not self.prewarm_container:
            self.log.error(f"We are NOT a pre-warm container...")
            self.report_error(f"Kernel '{self.kernel_id}' received a PROMOTION request despite "
                              f"not being a pre-warmed container",
                              "See message title.")
            return

        self.log.debug(f'Replacing old prewarm ID "{self.kernel_id}" with new kernel ID "{content["kernel_id"]}"')

        self.kernel_id: str = content["kernel_id"]
        distributed_kernel_config: Dict[str, Any] = content["distributed_kernel_config"]

        for key, value in distributed_kernel_config.items():
            if hasattr(self, key):
                self.log.debug(f"Assigning field '{key}' to value of type "
                               f"'{type(value).__name__}': '{value}'.")
                setattr(self, key, value)
            else:
                self.log.warning(f"Payload has unmatched field '{key}' with "
                                 f"value of type '{type(value).__name__}': '{value}'.")

        self.prewarm_container = False

        registration_start: float = time.time()
        await self.register_with_local_daemon_async(self.connection_info, self.kernel_id)
        registration_duration: float = (time.time() - registration_start) * 1.0e3

        if self.prometheus_enabled:
            self.registration_time_milliseconds.observe(registration_duration)

        # We use whatever is included in the prewarm request body rather than our instance variable (which we only
        # use as a fall-back here), as we are a pre-warm container, and so the value of the 
        # self.start_synclog_immediately instance variable does not reflect what we are supposed to do here.
        #
        # If we're being used as part of a migration, then we'll want to delay initializing the persistent store
        # until we're explicitly told to do so.
        start_synclog_immediately: bool = content.get("start_synclog_immediately", self.start_synclog_immediately)

        if start_synclog_immediately:
            self.log.info(f'Initializing Persistent Store with new Persistent ID: "{self.persistent_id}"')

            # Create future to avoid duplicate initialization
            future: asyncio.Future = asyncio.Future(loop=asyncio.get_running_loop())
            self.store = future

            try:
                self.store = await self.init_persistent_store_with_persistent_id(self.persistent_id)
                rsp = self.gen_simple_response()
                self.log.info("Persistent store confirmed: " + self.store)
            except Exception as ex:
                self.log.error(f"Failed to initialize persistent store during prewarm promotion: {ex}")
                self.log.error(traceback.format_exc())
                self.report_error("Failed to Init Persistent Store During Prewarm Promotion", str(ex))
                rsp = gen_error_response(ex)

            future.set_result(rsp)
        else:
            self.log.info(f'Waiting to start SyncLog until explicitly instructed to do so. '
                          f'Persistent ID: "{self.persistent_id}"')

        buffers, _ = self.extract_and_process_request_trace(parent, -1)
        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "promote_prewarm_reply",
            {"timestamp": time.time(), "persistent_id": self.persistent_id},
            parent,
            metadata={},
            buffers=buffers,
            ident=ident,
        )

        self.log.debug(f'Sent "promote_prewarm_reply" message: {reply_msg}')

    async def ping_kernel_ctrl_request(self, stream, ident, parent):
        """Respond to a 'ping kernel' Control request."""
        self.log.debug("Ping-Kernel (CONTROL) received.")

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in process_control.
        buffers, _ = self.extract_and_process_request_trace(parent, -1)
        self.log.debug(f"Embedding the following buffers in ping_reply: {buffers}")
        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "ping_reply",
            {"timestamp": time.time()},
            parent,
            metadata={},
            buffers=buffers,
            ident=ident,
        )
        faulthandler.dump_traceback(file=sys.stderr)

        cond: bool = asyncio.get_running_loop() == self.io_loop.asyncio_loop  # type: ignore
        cond2: bool = asyncio.get_running_loop() == self.control_thread.io_loop.asyncio_loop

        self.log.debug(f"Running event loop is self.io_loop: {cond}")
        self.log.debug(f"Running event loop is self.control_thread.io_loop: {cond2}")

        def callback():
            self.log.debug("Hello from self.ioloop!")

        def callback2():
            self.log.debug("Hello from self.control_thread.ioloop!")

        if not cond and cond2:
            self.io_loop.add_callback(callback)
            self.control_thread.io_loop.add_callback(callback2)

        self.log.debug(f"Sent ping_reply (control): {str(reply_msg)}")

    async def ping_kernel_shell_request(self, stream, ident, parent):
        """Respond to a 'ping kernel' Shell request."""
        self.log.debug("Ping-Kernel (SHELL) received.")

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell.
        buffers, _ = self.extract_and_process_request_trace(parent, -1)

        self.log.debug(f"Embedding the following buffers in ping_reply: {buffers}")
        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "ping_reply",
            {"timestamp": time.time()},
            parent,
            metadata={},
            buffers=buffers,
            ident=ident,
        )
        self.log.debug(f"Sent ping_reply (shell): {str(reply_msg)}")

    async def check_status_of_previous_election(self):
        """
        Check the status of the previous election, and wait for it to complete if it isn't done yet.
        """
        term_number: int = self.synchronizer.execution_count
        self.log.debug(f"Checking status of previous election (term {term_number}).")

        # If we've not yet created/held the first election, then we have nothing to check.
        if not self.synchronizer.created_first_election():
            return

        # If the term number is 0, but we've created the first election, then the first election must have failed.
        if term_number == 0:
            first_election: Election = self.synchronizer.get_election(1)

            if first_election is None:
                self.log.error("We've supposedly created the first election, "
                               "but cannot find election with term number equal to 1.")
                self.report_error("Cannot Find First Election",
                                  f"Replica {self.smr_node_id} of kernel {self.kernel_id} thinks it "
                                  "created the first election, but it cannot find any record of that election...")
                raise ValueError("We've supposedly created the first election, "
                                 "but cannot find election with term number equal to 1.")

            if not first_election.is_in_failed_state:
                self.log.error(f"Current term number is 0, and we've created the first election, "
                               f"but the election is not in failed state. Instead, it is in state "
                               f"{first_election.election_state.get_name()}.")
                self.report_error("Election State Error",
                                  f"Replica {self.smr_node_id} of kernel {self.kernel_id} has current term number of "
                                  f"0, and it has created the first election, but the election is not in failed "
                                  f"state. Instead, it is in state {first_election.election_state.get_name()}.")
                raise ValueError(f"Current term number is 0, and we've created the first election, "
                                 f"but the election is not in failed state. Instead, it is in state "
                                 f"{first_election.election_state.get_name()}.")

            term_number = 1

        try:
            if not self.synchronizer.is_election_finished(term_number):
                self.log.warning(f"Previous election (term {term_number}) has not finished yet. "
                                 f"Waiting for that election to finish before proceeding.")

                # Wait for the last election to end.
                await self.synchronizer.wait_for_election_to_end(term_number)
        except ValueError as ex:
            self.log.warning(f"Encountered ValueError while checking status of previous election: {ex}")
            self.report_error(
                error_title=f"Replica {self.smr_node_id} of Kernel {self.kernel_id} Encountered a ValueError While Checking Status of Election {term_number}",
                error_message=f"Error: {ex}. Kernel knows only about the following election terms: {self.synchronizer.get_known_election_terms()}",
            )
        except Exception as ex:
            self.log.error(f"Error encountered while checking status of previous election: {ex}")
            title: str = (f"Replica {self.smr_node_id} of Kernel {self.kernel_id} Encountered Unexpected "
                          f"{type(ex).__name__} While Checking Status of Election {term_number}")
            content: str = (f"Error: {ex}. Kernel knows only about the following election terms: "
                            f"{self.synchronizer.get_known_election_terms()}")
            self.report_error(error_title=title, error_message=content)

    def __check_for_existing_election(self, msg_id: str) -> int:
        """
        Check if there already exists an election associated with the Jupyter message ID from the parent header.

        If so, then return its term number if it is still in progress or if it is in a failed state.

        If it is voting-complete (or further along then that), then raise an ElectionAbortedException.

        If no associated election exists, then return -1.
        """
        if not self.smr_enabled:
            return -1

        term: int = -1
        existing_election: Optional[Election] = self.synclog.get_election(term, msg_id)

        # If we did not recover an existing election using the term, then we'll check if we've received any
        # synchronized values via the RaftLog for the Jupyter msg ID of the code submission message.
        if existing_election is None:
            term = self.synclog.check_for_term_with_jupyter_id(msg_id)

            # If the term is negative, then we can just return -1. We've come up dry.
            if term < 0:
                return -1

            self.log.debug(f'Found mapping from Jupyter message ID "{msg_id}" to '
                           f'term number {term} in Synchronizer.')

            # Since we found the term, we'll just double-check -- try to recover the election again,
            # this time using the term number.
            existing_election = self.synclog.get_election(term, msg_id)

        # If the existing election is non-null at this point...
        if existing_election is not None:

            # Then let's check if it's (a) not failed and (b) past the voting phase.
            # If so, then we'll just abort -- the winner has already been chosen.
            vote_done: bool = existing_election.voting_phase_completed_successfully
            election_failed: bool = existing_election.is_in_failed_state

            if not election_failed and vote_done and existing_election.winner_id == self.smr_node_id:
                self.log.debug(f'Found voting-complete existing election in state {existing_election.state.get_name()} '
                               f'with term number {existing_election.term_number} associated with Jupyter '
                               f'message "{msg_id}" -- and I was the winner!')
                return existing_election.term_number

            if not election_failed and vote_done:
                self.log.warning(f'At a minimum, the voting phase for existing election associated with Jupyter '
                                 f'message "{msg_id}" has already completed. No need for us to participate.')
                raise ElectionAbortedException(f'election associated with Jupyter message "{msg_id}" '
                                               f'is already voting-complete')

            self.log.debug(f'Found existing election in state {existing_election.state.get_name()} with term number '
                           f'{existing_election.term_number} associated with Jupyter message "{msg_id}"')

            return existing_election.term_number

        # The term number may be valid at this point, or it may be -1.
        return term

    async def yield_request(self, stream, ident, parent):
        """
        Similar to the do_execute method, but this method ALWAYS proposes "YIELD" instead of "LEAD".
        Kernel replicas are directed to call this instead of do_execute by their local daemon if there are resource constraints
        preventing them from being able to execute the user's code (e.g., insufficient GPUs available).

        Raises:
            err_wait_persistent_store: If the persistent data store is not available yet.

        Returns:
            dict: A dict containing the fields described in the "Execution results" Jupyter documentation available here:
            https://jupyter-client.readthedocs.io/en/latest/messaging.html#execution-results
        """
        assert self.shell is not None

        reply_content: Dict[str, Any] = {}
        error_occurred: bool = False  # Separate flag, since we raise an exception and generate an error response when we yield successfully.

        parent_header: dict[str, Any] = extract_header(parent)
        self._associate_new_top_level_threads_with(parent_header)
        self.next_execute_request_msg_id: str = parent_header["msg_id"]
        self.log.debug(
            f"yield_request with msg_id=\"{parent_header['msg_id']}\" called within the Distributed Python Kernel."
        )
        self.log.debug("Parent: %s" % str(parent))

        if not self.session:
            return
        try:
            content = parent["content"]
            self.log.debug("Content: %s" % content)
            code = content["code"]
            silent = content.get("silent", False)
        except Exception as ex:
            self.log.error(f"Got bad msg: {parent}. Exception: {ex}")
            self.kernel_notification_service_stub.Notify(
                gateway_pb2.KernelNotification(
                    title='Kernel Replica Received an Invalid "yield_request" Message',
                    message=f'Replica {self.smr_node_id} of Kernel {self.kernel_id} has received an invalid "yield_request" message: {ex}.',
                    notificationType=ErrorNotification,
                    kernelId=self.kernel_id,
                    replicaId=self.smr_node_id,
                    containerId=self.docker_container_id,
                )
            )
            return

        stop_on_error = content.get("stop_on_error", True)

        metadata = self.init_metadata(parent)

        target_replica_id: int = parent["metadata"].get("target_replica_id", -1)

        await self.process_execute_request_metadata(parent_header["msg_id"], parent_header["msg_type"],
                                                    parent["metadata"])

        # Re-broadcast our input for the benefit of listening clients, and
        # start computing output
        if not silent:
            assert self.execution_count is not None
            self.execution_count += 1
            self._publish_execute_input(code, parent, self.execution_count)

        current_term_number: int = -1
        try:
            self.toggle_outstream(override=True, enable=False)

            # Ensure persistent store is ready
            if not await self.check_persistent_store():
                raise err_wait_persistent_store

            current_term_number = self.__check_for_existing_election(parent_header["msg_id"])

            if current_term_number < 0:
                current_term_number = self.synchronizer.execution_count + 1

                execution_index: int = parent["metadata"].get("execution_index", -1)
                if execution_index >= 1 and execution_index != current_term_number:
                    self.log.warning(f'Computed term number {current_term_number} != "execution_index" entry '
                                     f'in metadata ({execution_index}). Will use {execution_index} instead.')

                    current_term_number = execution_index

            self.log.info(f"Calling synchronizer.ready({current_term_number}) now with YIELD proposal.")

            # Pass 'True' for the 'lead' parameter to propose LEAD.
            # Pass 'False' for the 'lead' parameter to propose YIELD.
            #
            # Pass 0 to lead the next execution based on history, which should be passed only if a duplicated execution is acceptable.
            # Pass value > 0 to lead a specific execution.
            # In either case, the execution will wait until states are synchronized.
            # type: ignore
            st: float = time.time()
            self.shell.execution_count = await self.synchronizer.ready(
                parent_header["msg_id"], current_term_number, lead=False, target_replica_id=target_replica_id)

            self.log.info(f"Completed call to synchronizer.ready({current_term_number}) with YIELD proposal. "
                          f"shell.execution_count: {self.shell.execution_count}")

            self.session.send(
                self.iopub_socket,
                IOPubNotification.SmrYieldTask,
                {
                    "time_elapsed_sec": time.time() - st,
                    "replica_id": self.smr_node_id,
                },
                ident=self._topic(IOPubNotification.SmrYieldTask),
            )

            if self.prometheus_enabled:
                self.num_yield_proposals.inc()

            if self.shell.execution_count == 0:  # type: ignore
                self.log.debug("I will NOT leading this execution.")
                reply_content: dict[str, Any] = gen_error_response(err_failed_to_lead_execution)
                # reply_content['yield-reason'] = TODO(Ben): Add this once I figure out how to extract it from the message payloads.
            else:
                self.log.error(f"I've been selected to lead this execution ({self.shell.execution_count}), "
                               f"but I'm supposed to yield!")

                # Notify the client that we will lead the execution (which is bad, in this case, as we were supposed to yield.)
                self.session.send(
                    self.iopub_socket,
                    "smr_lead_after_yield",
                    {"term": self.synchronizer.execution_count + 1},
                    ident=self._topic(SMR_LEAD_TASK),
                )
        except ElectionAbortedException as e:
            self.log.warning(e)
            # We intentionally raise ElectionAbortedException to abort early if we determine we can/should.
            reply_content: dict[str, Any] = gen_error_response(err_failed_to_lead_execution)
        except Exception as e:
            self.log.error(f"Error while yielding execution for term {current_term_number}: {e}")
            reply_content = gen_error_response(e)
            error_occurred = True

            # Flush output before sending the reply.
        sys.stdout.flush()
        sys.stderr.flush()
        # FIXME: on rare occasions, the flush doesn't seem to make it to the
        # clients... This seems to mitigate the problem, but we definitely need
        # to better understand what's going on.
        if self._execute_sleep:
            time.sleep(self._execute_sleep)

        # Payloads should be retrieved regardless of outcome, so we can both
        # recover partial output (that could have been generated early in a
        # block, before an error) and always clear the payload system.
        reply_content["payload"] = self.shell.payload_manager.read_payload()
        reply_content["yielded"] = True

        # If there was a reason for why we were explicitly told to YIELD included in the metadata of the
        # "yield_request" message, then we'll include it in our response as well as in our response's metadata.
        request_metadata: dict[str, Any] = parent.get("metadata", {})

        if "yield-reason" in request_metadata:
            yield_reason: str = request_metadata["yield-reason"]
            reply_content["yield-reason"] = yield_reason
            metadata["yield-reason"] = yield_reason

        # Be aggressive about clearing the payload because we don't want
        # it to sit in memory until the next execute_request comes in.
        self.shell.payload_manager.clear_payload()

        # Send the reply.
        reply_content: Dict[str, Any] = jsonutil.json_clean(reply_content)
        metadata = self.finish_metadata(parent, metadata, reply_content)

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell.
        buffers, _ = self.extract_and_process_request_trace(parent, -1)
        reply_msg: Dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "execute_reply",
            reply_content,
            parent,
            metadata=metadata,
            buffers=buffers,
            ident=ident,
        )

        self.log.debug("Sent the following reply after yield_request: %s" % reply_msg)

        if not silent and error_occurred and stop_on_error:  # reply_msg["content"]["status"] == "error"
            self._abort_queues()

        # Commented-out:
        #
        # Do this at the beginning instead.
        #
        #
        #
        # Block until the leader finishes executing the code, or until we know that all replicas yielded,
        # in which case we can just return, as the code will presumably be resubmitted shortly.
        current_election: Election = self.synchronizer.current_election
        if current_election is None:
            self.log.error(f"Expected to have non-null election at this point in yield_request...")
            self.report_error(
                "Election is Null in yield_request",
                "Election should not be null at this point.")
            await asyncio.sleep(5)
            return

        term_number: int = current_election.term_number
        self.log.debug(f"Waiting for election {term_number} "
                       "to be totally finished before returning from yield_request function.")

        await self.synchronizer.wait_for_election_to_end(term_number)
        self.log.debug(f"Done with yield_request for term {term_number}.")

    def copy_data_from_gpu_to_cpu(self, size_gb: float = 0, force: bool = False) -> None:
        if size_gb == 0:
            return

        # If the data is already on the CPU, then just return (unless force is True).
        if not self.data_on_gpu and not force:
            return

        if size_gb < 0:
            self.log.error(
                f"Cannot copy data of negative size from GPU to CPU: {size_gb} GB"
            )
            return

        bandwidth_gb_sec: float = np.random.normal(
            V100_AvgBandwidth_GbSec_DeviceToHost,
            V100_StdDevBandwidth_GbSec_DeviceToHost,
        )
        self.log.debug(
            f"Copying {size_gb} GB of data from the GPU to main memory with bandwidth of {bandwidth_gb_sec} GB/s."
        )

        latency_sec: float = size_gb / bandwidth_gb_sec
        time.sleep(latency_sec)

        self.data_on_gpu = False

    def copy_data_from_cpu_to_gpu(self, size_gb: float = 0, force: bool = False) -> None:
        if size_gb == 0:
            return

        # If the data is already on the GPU, then just return (unless force is True).
        if self.data_on_gpu and not force:
            return

        if size_gb < 0:
            self.log.error(
                f"Cannot copy data of negative size from CPU to GPU: {size_gb} GB"
            )
            return

        bandwidth_gb_sec: float = np.random.normal(
            V100_AvgBandwidth_GbSec_HostToDevice,
            V100_StdDevBandwidth_GbSec_HostToDevice,
        )
        self.log.debug(
            f"Copying {size_gb} GB of data from main memory to the GPU with bandwidth of {bandwidth_gb_sec} GB/s."
        )

        latency_sec: float = size_gb / bandwidth_gb_sec
        time.sleep(latency_sec)

        self.data_on_gpu = True

    def initialize_cuda_runtime(self, force: bool = False) -> None:
        # If the CUDA runtime is already initialized, then just return (unless force is True).
        if self.cuda_initialized and not force:
            return

        self.log.debug("Initializing CUDA runtime.")

        cuda_init_latency_sec = np.random.normal(
            AverageCudaInitTimeSec, StandardDeviationCudaInitTimeSec
        )
        time.sleep(cuda_init_latency_sec)

        self.cuda_initialized = True

    async def simulate_download_model_and_training_data(self, force: bool = False,
                                                        remote_storage_name: Optional[str] = None) -> tuple[
        float, float]:
        self.log.debug("Simulating the downloading of model and training data...")

        # If the model and training data are already downloaded, then just return (unless force is True).
        if self.model_and_training_data_downloaded and not force:
            self.log.debug(
                "Model and training data are already downloaded (simulated). Returning immediately."
            )
            return 0.0, 0.0

        if self.simulate_training_using_sleep:
            self.log.debug(
                "We're using real GPUs. No need to simulate the downloading of model and training data."
            )
            return 0.0, 0.0

        if self.current_resource_request is None:
            self.log.warning(
                "Current resource request is None; cannot simulate downloading model/training data..."
            )
            self.report_error(
                "No Current Resource Request",
                f"Kernel {self.kernel_id} does not have a resource request, so it cannot "
                f"simulate network I/O...",
            )
            return 0.0, 0.0

        if self.current_resource_request.get("vram", 0) == 0:
            self.log.debug(
                "Latest resource request specifies 0GB for VRAM. Nothing to download."
            )
            return 0.0, 0.0

        vram_gb: float | int = self.current_resource_request.get("vram", -1)

        if vram_gb == -1:
            self.report_error(
                "Current Resource Request Does Not Specify VRAM",
                f"The current resource request for {self.kernel_id} does not have a VRAM entry, "
                f"so the kernel cannot simulate downloading model/training data...",
            )
            return 0.0, 0.0

        vram_mb: float = vram_gb * 1.0e3
        vram_bytes: int = int(vram_mb * 1e6)

        self.log.debug(
            f"Downloading model and training data. Combined size: {vram_mb:,} MB."
        )
        download_model_duration: float = await self.simulate_remote_checkpointing(
            remote_storage_name, io_type="download", vram_bytes=vram_bytes // 2
        )
        download_training_data_duration: float = (
            await self.simulate_remote_checkpointing(
                remote_storage_name, io_type="download", vram_bytes=vram_bytes // 2
            )
        )

        return download_model_duration, download_training_data_duration

    def download_runtime_dependencies(
            self,
            n: int = 5,
            avg_latency: float = 5.0,
            std_dev_latency: float = 1.0,
            force: bool = False,
    ) -> None:
        """
        Simulate the downloading of runtime dependencies.

        :param n: the number of dependencies we'll pretend to download. this does not impact latency.
        :param avg_latency: the average time spent downloading + installing runtime dependencies (in seconds).
        :param std_dev_latency: standard deviation of time to download + install runtime dependencies (in seconds).
        :param force: if True, then simulate the download even if runtime_dependencies_downloaded is set to True.
        """
        # If the runtime dependencies are already downloaded & installed, then just return (unless force is True).
        if self.runtime_dependencies_downloaded and not force:
            return

        self.log.debug(
            "Downloading runtime dependencies (e.g., PyTorch, TensorFlow, etc.)."
        )

        # We're just going to randomly select ~5 dependencies to pretend to download.
        dependencies = [
            "TensorFlow",
            "PyTorch",
            "scikit-learn",
            "XGBoost",
            "LightGBM",
            "Hugging Face Transformers",
            "NumPy",
            "SciPy",
            "Pandas",
            "Matplotlib",
            "Seaborn",
            "Plotly",
            "TensorBoard",
            "Dask",
            "Ray",
            "Horovod",
            "OpenCV",
            "Pillow (PIL)",
            "NLTK/SpaCy",
            "Fastai",
        ]
        dependencies_subset = random.sample(dependencies, n)

        latency: float = np.random.normal(avg_latency, std_dev_latency)
        latency_frac: float = latency / n

        for dependency in dependencies_subset:
            self.log.debug(f"Download dependency: '{dependency}'")
            time.sleep(latency_frac)

        self.runtime_dependencies_downloaded = True

    def perform_initializations(self, vram_size_gb: float = 0):
        """
        Perform any necessary initialization steps, [possibly but not necessarily] including:
        - Initialize the CUDA runtime
        - Download (and install) any runtime dependencies, such as PyTorch or TensorFlow
        - Download the latest model parameters + training data
        - Copy the model + training data from host memory to device memory
        """
        if not self.cuda_initialized:
            init_cuda_start: float = time.time()
            self.initialize_cuda_runtime()
            init_cuda_ms: float = (time.time() - init_cuda_start) * 1.0e3
            self.log.debug(f"Initialized CUDA runtime in {init_cuda_ms} ms.")
            self.current_execution_stats.cuda_init_microseconds = (
                    init_cuda_ms * 1.0e3
            )  # it's already in milliseconds

        if not self.data_on_gpu:
            copy_data_to_gpu_start: float = time.time()
            self.copy_data_from_cpu_to_gpu(size_gb=vram_size_gb)
            copy_data_to_gpu_ms: float = (time.time() - copy_data_to_gpu_start) * 1.0e3
            self.log.debug(f"Copied {vram_size_gb} GB of data from main memory to the GPU {copy_data_to_gpu_ms} ms.")

            # it's already in milliseconds
            self.current_execution_stats.copy_data_from_cpu_to_gpu_microseconds = copy_data_to_gpu_ms * 1.0e3

    @property
    def get_creation_code_called(self) -> int:
        """
        :return: how many times we generate the 'create model for the first time' code when generating
                 custom DL training code.
        """
        return self._get_creation_code_called

    @property
    def get_download_code_called(self) -> int:
        """
        :return: how many times we generate the 'download' code when generating custom DL training code.
        """
        return self._get_download_code_called

    async def get_custom_training_code(
            self,
            target_training_duration_millis: float,
            gpu_device_ids: list[int] = None,
            deep_learning_model: Optional[str] = None,
            dataset_name: Optional[str] = None,
            batch_size: Optional[int] = None,
    ) -> str:
        """
        Generate the Python code that will be executed by this Jupyter kernel.

        This is used specifically to generate some sort of PyTorch GPU-enabled training code.

        :param target_training_duration_millis: how long the training should last in milliseconds.
        :param gpu_device_ids: the GPU device IDs assigned to this kernel.
        :param deep_learning_model: the name of the deep learning model to be used during the training.
        :param dataset_name: the name of the dataset to be used during the training.
        :param batch_size: the batch size for the training job. only used when the variables are first created.

        :return: the generated Python code to be executed by this Jupyter kernel.
        """
        if self.simulate_training_using_sleep:
            self.log.debug("We're not using real GPUs. Will spoof DL training using time.sleep().")
            return f"import time\ntime.sleep({target_training_duration_millis / 1.0e3})"

        if gpu_device_ids is None or len(gpu_device_ids) == 0:
            self.log.warning("No GPU device IDs specified. Defaulting to [0].")
            gpu_device_ids = [0]

        download_code: str = ""
        existing_model_code: str = "None"
        creation_code: str = ""

        # Acquire lock. Critical section.
        async with self._user_ns_lock:
            existing_model: Optional[DeepLearningModel | Any] = self.shell.user_ns.get("model", None)

            # Case 1: there is no "model" variable in the user namespace.
            # In this case, we'll create both the model and the dataset (even if the dataset already exists).
            if existing_model is None:
                self.log.debug(f'No "model" variable in user namespace. '
                               f'Will create new "{deep_learning_model}" model variable and '
                               f'new "{dataset_name}" dataset variable.')
                creation_code = get_create_model_and_dataset_code(deep_learning_model, dataset_name, batch_size)
                self._get_creation_code_called += 1

            # Case 2: there IS an existing model variable, but it's not of the correct type.
            # In this case, we recreate the model variable so that it is of the correct type.
            # We also (re)create the dataset variable (even if it already exists).
            if existing_model is not None and not isinstance(existing_model, ModelClassesByName[deep_learning_model]):
                self.log.warning(
                    f"Found existing variable 'model' in shell user namespace of type "
                    f"'{type(existing_model).__name__}'. Variable will be overwritten with "
                    f"variable for '{deep_learning_model}' model of type '{ModelClassesByName[deep_learning_model].__name__}'."
                )
                existing_model = None
                creation_code = get_create_model_and_dataset_code(deep_learning_model, dataset_name, batch_size)
                self._get_creation_code_called += 1

            # Case 3: there is already an existing "model" variable of the correct type in the user namespace.
            #
            # If existing_model is non-null at this point, then it is an instance of either
            # DeepLearningModel or a subclass of DeepLearningModel.
            if existing_model is not None and isinstance(existing_model, ModelClassesByName[deep_learning_model]):
                self.log.debug(f'"model" variable found in user namespace. '
                               f'Will reuse existing "{deep_learning_model}" model variable.')

                self.shell.user_ns["__existing_model__"] = existing_model
                # self.shell.user_ns["__download_dataset_iopub_notify__"] = self.load_dataset_iopub_notify
                existing_model_code = "__existing_model__"

                # Case 3a: there was already a "model" variable of the correct type, but no dataset variable.
                # In this case, we'll JUST create the dataset variable, and we'll re-use the existing model variable.
                dataset: Optional[CustomDataset] = self.shell.user_ns.get("dataset")
                if dataset is None or not isinstance(dataset, DatasetClassesByName[dataset_name]):
                    self.log.debug("Dataset variable either does not exist or is for the wrong type of dataset.")
                    self.log.debug(f'Will create new "dataset" variable for "{dataset_name}" dataset.')
                    creation_code = get_create_dataset_only_code(deep_learning_model, dataset_name, batch_size)
                else:
                    # Case 3b: the dataset variable also already existed, so we'll not create any new variables.
                    creation_code = get_skipped_creation_code(deep_learning_model, dataset_name)

            # Case 3 continued: if SMR is enabled and there are multiple replicas, then we'll
            # download the latest model state from remote storage to ensure it is up to date.
            #
            # TODO: Will this result in the previous leader unnecessarily downloading the latest state?
            if self.smr_enabled and self.num_replicas > 1 and existing_model is not None:
                self.log.debug(f'Will retrieve latest state for "{deep_learning_model}" model from remote storage.')

                # Inject the __download_func__ and __model_pointer__ variables into the user namespace.
                self.shell.user_ns["__download_func__"] = self.__load_model_from_remote_storage
                self.shell.user_ns["__model_pointer__"] = ModelPointer(
                    deep_learning_model=existing_model,
                    user_namespace_variable_name="model",
                    # __model_pointer__ is temporary; the actual variable we want to use is the 'model' variable.
                    model_path=os.path.join(self.store_path, existing_model.name),
                )
                self._get_download_code_called += 1
                download_code: str = get_download_code(existing_model_code, deep_learning_model)

        return get_training_code(download_code, creation_code, gpu_device_ids, target_training_duration_millis)

    def load_dataset_iopub_notify(self, downloading: bool = True, dataset_name: str = "N/A",
                                  time_elapsed_sec: float = 0.0):
        if downloading:
            topic = IOPubNotification.DownloadingDataset
        else:
            topic = IOPubNotification.DownloadedDataset

        content: Dict[str, Any] = {
            "replica_id": self.smr_node_id,
            "dataset_name": dataset_name,
        }

        if time_elapsed_sec > 0:
            content["time_elapsed_sec"] = time_elapsed_sec

        self.session.send(
            self.iopub_socket,
            topic,
            content,
            ident=self._topic(topic),
        )

    async def primary_replica_protocol(
            self,
            term_number: int = -1,
            election_start: float = time.time(),
            jupyter_message_id: str = "",
            target_replica_id: int = -1,
    ):
        assert term_number >= 0

        if not self.smr_enabled:
            return True

        # Pass 'True' for the 'lead' parameter to propose LEAD.
        # Pass 'False' for the 'lead' parameter to propose YIELD.
        #
        # Pass 0 to lead the next execution based on history, which should be passed only if a duplicated execution is acceptable.
        # Pass value > 0 to lead a specific execution.
        # In either case, the execution will wait until states are synchronized.
        # type: ignore
        self.shell.execution_count = await self.synchronizer.ready(
            jupyter_message_id, term_number, lead=True, target_replica_id=target_replica_id
        )

        self.current_execution_stats.leader_election_microseconds = int((time.time() - election_start) * 1.0e6)

        self.log.info(f"Completed call to synchronizer.ready({term_number}) with LEAD proposal. "
                      f"shell.execution_count: {self.shell.execution_count}")

        if self.prometheus_enabled:
            self.num_lead_proposals.inc()

        if self.shell.execution_count == 0:  # type: ignore
            self.log.debug("I will NOT leading this execution.")
            return False

        return True

    async def do_execute(
            self,
            code: str,
            silent: bool,
            store_history: bool = True,
            user_expressions: dict = None,
            allow_stdin: bool = False,
            remote_storage_name: Optional[str] = None,
            training_duration_millis: float = -1,
            gpu_device_ids: list[int] = None,
            deep_learning_model_name: Optional[str] = None,
            dataset: Optional[str] = None,
            batch_size: Optional[int] = 1,
            execute_request_metadata: Optional[Dict[str, Any]] = None,
            parent_header: Dict[str, Any] = None,
            parent=None,
            target_replica_id: int = -1,
            *,
            cell_meta=None,
            cell_id=None,
    ) -> Dict[str, Any]:
        """
        Execute user code. This is part of the official Jupyter kernel API.
        Reference: https://jupyter-client.readthedocs.io/en/latest/wrapperkernels.html#MyKernel.do_execute

        Args:
            :param parent: the parent message in its entirety
            :param target_replica_id: smr node ID of target, pre-selected primary replica, or -1 if none pre-selected
            :param parent_header: header of execute_request message.
            :param execute_request_metadata: the metadata of the execute request.
            :param dataset: the dataset to be used for deep learning training
            :param deep_learning_model_name: the model to be used for deep learning training
            :param batch_size: batch size to pass to dataset constructor
            :param gpu_device_ids: the gpu device IDs that we've been assigned/allocated
            :param remote_storage_name: the name of the remote storage that we should use for (simulated) checkpointing
            :param cell_meta:
            :param cell_id:
            :param code: (str): The code to be executed.
            :param silent: (bool): Whether to display output.
            :param store_history: (bool, optional): Whether to record this code in history and increase the execution count. If silent is True, this is implicitly False. Defaults to True.
            :param user_expressions: (dict, optional): Mapping of names to expressions to evaluate after the code has run. You can ignore this if you need to. Defaults to None.
            :param allow_stdin: (bool, optional): Whether the frontend can provide input on request (e.g. for Python's raw_input()). Defaults to False.
            :param training_duration_millis: (float): The length of time that the kernel should train for. If specified, we run special training code and ignore what we were told to execute.

        Raises:
            err_wait_persistent_store: If the persistent data store is not available yet.
            err_failed_to_lead_execution: If this kernel replica is not "selected" to execute the code.

        Returns:
            dict: A dict containing the fields described in the "Execution results" Jupyter documentation available here:
            https://jupyter-client.readthedocs.io/en/latest/messaging.html#execution-results
        """
        if len(code) > 0:
            self.log.info("DistributedKernel is preparing to execute some code: %s\n\n", code)
        else:
            self.log.warning("DistributedKernel is preparing to execute empty codeblock...\n\n")

        # Make sure we have some GPU device IDs if we're using real GPUs and our current resource
        # request indicates that we have 1 or more GPUs assigned to us.
        if not self.simulate_training_using_sleep and cuda_available and (
                gpu_device_ids is None or len(gpu_device_ids) == 0) and self.current_resource_request.get("gpus",
                                                                                                          0) > 0:
            self.log.error("We're using real GPUs, but we've not been assigned any GPU device IDs...")

            self.report_error(
                f"Replica {self.smr_node_id} of Kernel '{self.kernel_id}' Was Not Assigned Any GPU Device IDs",
                f"Replica {self.smr_node_id} of kernel '{self.kernel_id}' was not assigned any GPU device "
                f"IDs for 'execute_request' message '{self.next_execute_request_msg_id}'",
            )
            gpu_device_ids = [0]  # Default to this for now.

        # Special code to initialize persistent store
        if code[: len(key_persistent_id)] == key_persistent_id:
            self.log.debug('Using special code to initialize persistent store: "%s"' % code)
            return await asyncio.ensure_future(self.init_persistent_store(code))

        # Ensure persistent store is ready
        if not await self.check_persistent_store():
            async with self._user_ns_lock:
                persistent_id = self.shell.user_ns.get("persistent_id", None)

            if persistent_id is not None:
                self.persistent_id = persistent_id
                code = 'persistent_id = "%s"' % self.persistent_id
                await asyncio.ensure_future(self.init_persistent_store(code))

        # Check the status of the last election before proceeding.
        try:
            await self.check_status_of_previous_election()
        except Exception as ex:
            self.log.error(f"Error while checking previous election: {ex}")
            self.log.error(traceback.format_exc())

            title: str = f"Replica {self.smr_node_id} of Kernel {self.kernel_id} Erred While Checking Prev. Election"
            content: str = f'"execute_request" ID="{self.next_execute_request_msg_id}", error: {ex}'
            self.report_error(error_title=title, error_message=content)

        current_term_number: int = -1
        try:
            self.toggle_outstream(override=True, enable=False)

            # Ensure persistent store is ready
            if not await self.check_persistent_store():
                raise err_wait_persistent_store

            current_term_number = self.__check_for_existing_election(parent_header["msg_id"])
            if current_term_number < 0:
                current_term_number = self.synchronizer.execution_count + 1

                execution_index: int = execute_request_metadata.get("execution_index", -1)
                if 1 <= execution_index != current_term_number:
                    self.log.warning(f'Computed term number {current_term_number} != "execution_index" entry '
                                     f'in metadata ({execution_index}). Will use {execution_index} instead.')

                    current_term_number = execution_index

            self.log.info(f"Calling synchronizer.ready({current_term_number}) now with LEAD proposal.")

            election_start: float = time.time()
            is_primary_replica: bool = await self.primary_replica_protocol(
                term_number=current_term_number,
                election_start=election_start,
                jupyter_message_id=self.next_execute_request_msg_id,
                target_replica_id=target_replica_id,
            )

            if not is_primary_replica:
                raise err_failed_to_lead_execution

            self.log.debug(f"I WILL lead this execution ({self.shell.execution_count}).")
            self.current_execution_stats.won_election = True

            if self.simulate_training_using_sleep:
                vram_size_gb = self.current_resource_request.get("vram", 0)
                self.perform_initializations(vram_size_gb=vram_size_gb)

            # Notify the client that we will lead the execution.
            # TODO: Eventually, we could pass "gpu" as True or False depending on whether we really
            #       do need GPUs for this training task, assuming we'd know about that here.
            content: dict[str, str | float | bool] = {
                "gpu": True,
                "msg_created_at_unix_milliseconds": time.time_ns() // 1_000_000,
                "execute_request_msg_id": self.next_execute_request_msg_id,
            }

            self.session.send(
                self.iopub_socket,
                SMR_LEAD_TASK,
                content,
                parent=parent,
                ident=self._topic(SMR_LEAD_TASK),
            )  # type: ignore

            self.log.debug(f'Sent "smr_lead_task" message for "execute_request" '
                           f'message "{self.next_execute_request_msg_id}".')

            reply_content, performed_dl_training, code = await self.execute_user_code(
                target_training_duration_millis=training_duration_millis,
                code=code,
                gpu_device_ids=gpu_device_ids,
                deep_learning_model_name=deep_learning_model_name,
                dataset=dataset,
                batch_size=batch_size,
                execute_request_metadata=execute_request_metadata,
                execute_request_msg_id=self.next_execute_request_msg_id,
            )

            # Re-enable stdout and stderr forwarding.
            self.toggle_outstream(override=True, enable=True)

            reply_content[LEADER_KEY] = True
            reply_content["term_number"] = current_term_number
            reply_content["performed_dl_training"] = performed_dl_training
            reply_content["code"] = code
            reply_content["yielded"] = False
        except ElectionAbortedException as ex:
            self.log.warning("ElectionAbortedException: {ex}")

            # Return generic yield reply content.
            reply_content = gen_error_response(err_failed_to_lead_execution)
        except ExecutionYieldError as eye:
            self.log.info("Execution yielded: {}".format(eye))

            reply_content = gen_error_response(eye)
            reply_content["yielded"] = True
        except DiscardMessageError as dme:
            self.log.warning(f"Received direction to discard Jupyter Message {self.next_execute_request_msg_id}, "
                             f"as election for term {current_term_number} was skipped: {dme}")

            title: str = (f"Election {current_term_number} Skipped by Replica {self.smr_node_id} "
                          f"of Kernel {self.kernel_id}")
            body: str = (f'"execute_request" message {self.next_execute_request_msg_id} was dropped by replica '
                         f'{self.smr_node_id} of kernel {self.kernel_id}, as associated election '
                         f'(term={current_term_number}) was skipped.')
            typ: int = WarningNotification

            self.send_notification(notification_title=title, notification_body=body, notification_type=typ)

            reply_content = gen_error_response(dme)
        except Exception as e:
            self.log.error(f"Execution error: {e}")
            self.log.error(traceback.format_exc())
            self.report_error("Execution Error", f"{type(e).__name__}: {e}")

            reply_content = gen_error_response(e)
            reply_content["yielded"] = False

        return reply_content

    def __validate_gpu_id_args(
            self,
            gpu_device_ids: list[int] = None,
            execute_request_metadata: Optional[Dict[str, Any]] = None,
    ):
        # If we're simulating training using time.sleep, then we have
        # no expectations regarding the GPU device IDs. We can return.
        if self.simulate_training_using_sleep:
            return

        # If we received any valid GPU device IDs, then we're OK to return.
        if gpu_device_ids is not None and len(gpu_device_ids) > 0:
            return

        # At this point, the GPU device IDs are either none or empty.
        if execute_request_metadata is not None and "required-gpus" in execute_request_metadata:
            # If in the request metadata, we were told that we don't actually need any GPUs
            # for this training, then we're okay to return.
            if execute_request_metadata["required-gpus"] == 0:
                return

        if execute_request_metadata is not None and "resource_request" in execute_request_metadata:
            # If the latest/current resource request is embedded in the metadata, then we can
            # check that to verify that we do not in fact require any GPU device IDs.
            resource_request: Dict[str, int | float] = execute_request_metadata["resource_request"]
            if "gpus" in resource_request and resource_request["gpus"] == 0:
                return

        # If our current execution request is not None and there is a "gpus" entry,
        # then check that it is 0. If so, then it is OK that we did not receive any
        # valid GPU device IDs.
        #
        # Note that our current resource request will/should have been set to the value of the "resource_request"
        # entry in the "execute_request" metadata frame, so the last if-statement check would've found this...
        if self.current_resource_request is not None and "gpus" in self.current_resource_request:
            if self.current_resource_request["gpus"] == 0:
                return

        # Something is wrong. We should have received GPU device IDs.
        error_title: str = (f'Kernel "{self.kernel_id}" Received 0 GPU device IDs for '
                            f'"execute_request" "{self.next_execute_request_msg_id}"')

        self.report_error(error_title, 'No GPU device IDs received.')

        raise ValueError("Received 0 valid GPU device IDs, though it seems that we should have...")

    async def execute_user_code(
            self,
            target_training_duration_millis: float = 0,
            code: str = "",
            silent: bool = False,
            store_history: bool = True,
            user_expressions: dict = None,
            allow_stdin: bool = False,
            gpu_device_ids: list[int] = None,
            deep_learning_model_name: Optional[str] = None,
            dataset: Optional[str] = None,
            batch_size: Optional[int] = 1,
            execute_request_metadata: Optional[Dict[str, Any]] = None,
            execute_request_msg_id: str = "",
    ) -> tuple[Dict[str, Any], bool, str]:
        """
        Execute the user-submitted code.

        :return: a tuple containing:
                 - (1) the reply content
                 - (2) a bool indicating whether the training we performed used an actual (or simulated/fake) GPU
                 - (3) the code that was executed (which may have been updated/changed)
        """
        if not self.simulate_training_using_sleep:
            self.__validate_gpu_id_args(
                gpu_device_ids=gpu_device_ids,
                execute_request_metadata=execute_request_metadata
            )

        performed_dl_training: bool = False

        # Check for the special training code that indicates that we are to generate some code to simulate
        # the execution of deep learning training.
        #
        # Note that the user could also just submit the code that we generate directly, but this simplified
        # the implementation for evaluation purposes.
        if "training_duration_millis = " in code:
            # First, make sure that we have a valid, non-zero value for the 'target_training_duration_millis' variable.
            if target_training_duration_millis <= 0:
                target_training_duration_millis = int(float(code[27:]))

            self.log.debug(
                f"Explicitly instructed to train for {target_training_duration_millis / 1.0e3:,} seconds. "
                f"Discarding specified code. Will use custom training code instead."
            )

            # Generate the custom code.
            code = await self.get_custom_training_code(
                target_training_duration_millis=target_training_duration_millis,
                gpu_device_ids=gpu_device_ids,
                deep_learning_model=deep_learning_model_name,
                dataset_name=dataset,
                batch_size=batch_size,
            )
            performed_dl_training = True

        self.log.debug(f"Executing the following code now:\n"
                       f"{ColoredLogFormatter.LIGHT_CYAN}{ColoredLogFormatter.BOLD}{code}{ColoredLogFormatter.reset}\n")

        self.current_execution_stats.execution_start_unix_millis = time.time() * 1.0e3

        # Execute code
        reply_routing = super().do_execute(
            code,
            silent,
            store_history=store_history,
            user_expressions=user_expressions,
            allow_stdin=allow_stdin,
        )

        # Wait for the settlement of variables.
        reply_content = await reply_routing
        self.current_execution_stats.execution_end_unix_millis = time.time() * 1.0e3
        exec_duration_millis: float = (
                self.current_execution_stats.execution_end_unix_millis
                - self.current_execution_stats.execution_start_unix_millis
        )
        self.current_execution_stats.execution_time_microseconds = exec_duration_millis * 1.0e3
        reply_content["execution_start_unix_millis"] = self.current_execution_stats.execution_start_unix_millis
        reply_content["execution_finished_unix_millis"] = self.current_execution_stats.execution_end_unix_millis

        if 'status' in reply_content and reply_content['status'] == 'error':
            self.log.error(f"Error while executing user-submitted code: {reply_content}")
        else:
            self.log.info(f"Finished executing user-submitted code in {exec_duration_millis:,} ms. "
                          f"Returning the following content: {reply_content}")

        return reply_content, performed_dl_training, code

    async def synchronize_updated_state(self, term_number: int, jupyter_message_id: str):
        """
        Synchronize any state updated during the last execution with peer replicas.

        If SMR is disabled, then synchronize_updated_state returns immediately.

        Warning: as of right now, any synchronization errors are simply caught and logged.

        :param jupyter_message_id: the associated jupyter message id
        :param term_number: the term for which we're performing the state synchronization.
        """
        if self.execution_ast is None:
            self.log.warning(f"Synchronizing AST for term {term_number}. "
                             f"Execution AST is None. "
                             f"Synchronization will likely fail...")
        else:
            self.log.debug(f"Synchronizing AST for term {term_number}. "
                           f"Execution AST is NOT None.")

        sync_start_time: float = time.time() * 1.0e3

        try:
            # Do the synchronization
            synchronized_successfully: bool = await self.synchronizer.sync(
                execution_ast=self.execution_ast,
                source=self.source,
                jupyter_message_id=jupyter_message_id
            )
        except Exception as exc:
            self.log.error(f"Synchronization failed: {exc}")
            self.kernel_notification_service_stub.Notify(
                gateway_pb2.KernelNotification(
                    title="Synchronization Error",
                    message=f"Replica {self.smr_node_id} of Kernel {self.kernel_id} "
                            f"has experienced a synchronization error: {exc}.",
                    notificationType=ErrorNotification,
                    kernelId=self.kernel_id,
                    replicaId=self.smr_node_id,
                    containerId=self.docker_container_id,
                )
            )

            synchronized_successfully: bool = False

        sync_end_time: float = time.time() * 1.0e3
        sync_duration_millis: float = sync_end_time - sync_start_time

        if synchronized_successfully:
            self.log.debug(f"Successfully synchronized updated state from term "
                           f"{term_number} with peers in {sync_duration_millis} ms.")
            self.current_execution_stats.sync_start_unix_millis = sync_start_time
            self.current_execution_stats.sync_end_unix_millis = sync_end_time
            self.current_execution_stats.sync_duration_millis = sync_duration_millis

        self.execution_ast = None
        self.source = None

        if self.execution_count is None:
            raise ValueError(f'Execution Count is None.')

    async def extract_mem_copy_times(self, code: str = ""):
        """
        Extract the latencies of copying memory from the CPU to the GPU and from the GPU to the CPU from the model
        variable that was used in the last user-executed code.
        :param code: the code that the user executed
        """
        async with self._user_ns_lock:
            model: Optional[DeepLearningModel] = self.shell.user_ns.get("model", None)

        if model is None:
            self.log.debug("Could not find 'model' variable in user namespace.")
            return

        if "model" not in code:
            self.log.debug("Found 'model' variable in user namespace; "
                           "however, 'model' was not referenced in last executed user-submitted code.")
            return

        if len(model.gpu_to_cpu_times) > 0:
            gpu2cpu_micros: float = model.cpu_to_gpu_times[-1]
            self.current_execution_stats.copy_data_from_cpu_to_gpu_microseconds = gpu2cpu_micros
            self.log.debug(f"Retrieved most recent GPU to CPU time from model "
                           f"in shell user namespace: {gpu2cpu_micros} µs")

        if len(model.cpu_to_gpu_times) > 0:
            cpu2gpu_micros: float = model.gpu_to_cpu_times[-1]
            self.current_execution_stats.copy_data_from_gpu_to_cpu_microseconds = cpu2gpu_micros
            self.log.debug(f"Retrieved most recent CPU to GPU time from model "
                           f"in shell user namespace: {cpu2gpu_micros} µs")

    async def extract_dataset_tokenization_latency(self, code: str = ""):
        """
        Inspect the 'dataset' variable from the user namespace and determine if we need to record its download
        latency for the execution request that we're currently processing.
        """
        async with self._user_ns_lock:
            dataset: Optional[CustomDataset] = self.shell.user_ns.get("dataset", None)

        if dataset is None:
            self.log.debug("Could not find 'dataset' variable in user namespace.")
            return

        # Tokenization overhead is only relevant for NLP datasets.
        # We can stop here if we find that the dataset is not an NLP dataset.
        if dataset.category() != NaturalLanguageProcessing:
            return

        if "dataset" not in code:
            self.log.debug("Found 'dataset' variable in user namespace; "
                           "however, 'dataset' was not referenced in the last executed user-submitted code.")
            return

        # TODO: Does this not ultimately get double-counted?
        if dataset.recorded_tokenization_overhead:
            self.log.debug(f'Tokenization overhead of "{dataset.dataset_name()}" dataset has already been recorded.')
            return

        # If the dataset was not already downloaded, then record the time
        # it took to download the dataset in the current execution stats.
        self.log.debug(f'Tokenization overhead of "{dataset.dataset_name()}" dataset has NOT yet been recorded.')
        self.current_execution_stats.tokenize_dataset_microseconds = dataset.tokenization_duration_sec * 1.0e6

        # Flip this to True so that we don't re-record the tokenization overhead if we train
        # again using the same dataset variable.
        dataset.set_recorded_tokenization_overhead(True)

        self.current_execution_stats.tokenize_training_data_start_unix_millis = dataset.tokenization_start
        self.current_execution_stats.tokenize_training_data_end_unix_millis = dataset.tokenization_end

    async def extract_dataset_download_latency(self, code: str = ""):
        """
        Inspect the 'dataset' variable from the user namespace and determine if we need to record its download
        latency for the execution request that we're currently processing.
        """
        async with self._user_ns_lock:
            dataset: Optional[CustomDataset] = self.shell.user_ns.get("dataset", None)

        if dataset is None:
            self.log.debug("Could not find 'dataset' variable in user namespace.")
            return

        if "dataset" not in code:
            self.log.debug("Found 'dataset' variable in user namespace; "
                           "however, 'dataset' was not referenced in the last executed user-submitted code.")
            return

        # TODO: Does this not ultimately get double-counted?
        dataset_already_downloaded: bool = dataset.dataset_already_downloaded
        if dataset_already_downloaded:
            self.log.debug("Dataset was already downloaded.")
            return

        # If the dataset was not already downloaded, then record the time
        # it took to download the dataset in the current execution stats.
        self.log.debug("Dataset was NOT already downloaded. Extracting download times now.")
        self.current_execution_stats.download_training_data_microseconds = dataset.download_duration_sec * 1.0e6

        # Flip this to True so that we don't re-read the download overhead if we train
        # again using the same dataset variable.
        dataset.dataset_already_downloaded = True

        self.current_execution_stats.download_training_data_start_unix_millis = dataset.download_start
        self.current_execution_stats.download_training_data_end_unix_millis = dataset.download_end

    async def handle_post_execution_overheads(
            self,
            remote_storage_name: Optional[str] = None,
            performed_dl_training: bool = False,
            code: str = "",
    ):
        if self.simulate_training_using_sleep and self.data_on_gpu:
            vram_size_gb = self.current_resource_request.get("vram", 0)
            copy_data_to_cpu_start: float = time.time()
            self.copy_data_from_gpu_to_cpu(size_gb=vram_size_gb)
            copy_data_to_cpu_ms: float = (time.time() - copy_data_to_cpu_start) * 1.0e3
            self.log.debug(f"Copied {vram_size_gb} GB of data from GPU to main memory in {copy_data_to_cpu_ms} ms.")
            # it's already in milliseconds
            self.current_execution_stats.copy_data_from_gpu_to_cpu_microseconds = copy_data_to_cpu_ms * 1.0e3

            if (
                    remote_storage_name is not None
                    and self.simulate_write_after_execute
                    and self.simulate_write_after_execute_on_critical_path
            ):
                self.log.debug(f"Performing post-execution simulated network "
                               f"write operation to '{remote_storage_name}' on critical path.")

                duration_sec: float = await self.simulate_remote_checkpointing(remote_storage_name, io_type="upload")

                if duration_sec > 0 and self.prometheus_enabled:
                    self.remote_storage_write_latency_milliseconds.labels(
                        session_id=self.kernel_id, workload_id=self.workload_id
                    ).observe(duration_sec * 1e3)

                    self.delay_milliseconds.labels(
                        session_id=self.kernel_id, workload_id=self.workload_id
                    ).inc(duration_sec * 1e3)

                self.current_execution_stats.upload_runtime_dependencies_microseconds = duration_sec * 1.0e6
        elif not self.simulate_training_using_sleep and performed_dl_training:
            await self.extract_mem_copy_times(code=code)
            await self.extract_dataset_download_latency(code=code)
            await self.extract_dataset_tokenization_latency(code=code)

    async def simulate_remote_checkpointing(
            self,
            remote_storage_name: Optional[str],
            io_type: Optional[str] = None,
            vram_bytes: float = -1,
    ) -> float:
        """
        Simulate checkpointing using the current resource request and the specified remote storage name.

        If the specified remote storage name is None or the empty string, then the most-recently-used remote storage
        will be used again.

        io_type must be "read", "write", "upload", or "download" (case-insensitive).
        """
        if not self.simulate_checkpointing_latency:
            self.log.debug(
                f"Checkpointing is disabled. Skipping simulation of network {io_type} "
                f"targeting remote storage {remote_storage_name}."
            )
            return 0

        if io_type is None:
            raise ValueError("must specify an IO type")

        io_type = io_type.lower()
        if (
                io_type != "read"
                and io_type != "write"
                and io_type != "upload"
                and io_type != "download"
        ):
            raise ValueError(f'unknown/unsupported IO type specified: "{io_type}"')

        if self.current_resource_request is None:
            self.log.error(
                "Current resource request is None; cannot simulate checkpointing..."
            )
            self.report_error(
                "No Current Resource Request",
                f"Kernel {self.kernel_id} does not have a resource request, so it cannot "
                f'simulate checkpointing with specified remote storage "{remote_storage_name}"...',
            )
            return 0

        if self.current_resource_request.get("vram", 0) == 0:
            self.log.debug(
                "Latest resource request specifies 0GB for VRAM. Nothing to checkpoint."
            )
            return 0

        if self.remote_storages is None or len(self.remote_storages) == 0:
            self.log.warning(
                "No remote storages registered; cannot simulate checkpointing..."
            )
            self.report_error(
                f'Unknown Remote Storage "{remote_storage_name}"',
                f'Could not find requested remote storage "{remote_storage_name}" to simulate '
                f'checkpointing after processing "execute_request" "{self.next_execute_request_msg_id}"',
            )
            return 0

        if remote_storage_name is None or remote_storage_name == "":
            self.log.info(
                "No remote storage specified. Will select most-recently-used remote storage."
            )
            # If we have more than one remote storage registered, then we'll just use whatever remote storage was
            # used most recently.
            simulated_checkpointer: Optional[SimulatedCheckpointer] = (
                self.get_most_recently_used_remote_storage()
            )
            if simulated_checkpointer is None:
                assert self.remote_storages is None or len(self.remote_storages) == 0
                self.log.warning("No simulated check-pointers identified.")

            remote_storage_name = simulated_checkpointer.name
            self.log.debug(
                f'Identified remote storage "{simulated_checkpointer.name}" as most-recently-used checkpointer.'
            )
        else:
            simulated_checkpointer: Optional[SimulatedCheckpointer] = (
                self.remote_storages.get(remote_storage_name, None)
            )

        # Verify that the requested SimulatedCheckpointer has been registered, as we cannot perform the
        # simulated checkpointing if it has not been registered.
        if simulated_checkpointer is None:
            self.report_error(
                f'Unknown Remote Storage "{remote_storage_name}"',
                f'Could not find requested remote storage "{remote_storage_name}" to simulate '
                f'checkpointing after processing "execute_request" "{self.next_execute_request_msg_id}"',
            )
            return 0

        if vram_bytes < 0:
            vram_gb: float | int = self.current_resource_request.get("vram", -1)

            if vram_gb == -1:
                self.report_error(
                    "Current Resource Request Does Not Specify VRAM",
                    f"The current resource request for {self.kernel_id} does not have a VRAM entry, "
                    f'so the kernel cannot simulate checkpointing with specified remote storage "{remote_storage_name}"...',
                )
                return 0

            vram_bytes: int = int(vram_gb * 1e9)

        if io_type == "read" or io_type == "download":
            rate_formatted = simulated_checkpointer.download_rate_formatted
            rate = simulated_checkpointer.download_rate
            simulation_func = simulated_checkpointer.download_data
        else:
            rate_formatted = simulated_checkpointer.upload_rate_formatted
            rate = simulated_checkpointer.upload_rate
            simulation_func = simulated_checkpointer.upload_data

        start_time: float = time.time()
        try:
            self.log.debug(
                f"Simulating remote {io_type} of size {format_size(vram_bytes)} "
                f"bytes targeting remote storage {simulated_checkpointer.name}. "
                f"I/O rate: {rate_formatted}. "
                f"Expected time to complete I/O operation: {get_estimated_io_time_seconds(size_bytes=vram_bytes, rate=rate)} seconds."
            )
            await simulation_func(size_bytes=int(vram_bytes))
        except Exception as exc:
            self.log.error(
                f"{type(exc).__name__} while simulating checkpointing with remote storage "
                f"{remote_storage_name} and data of size {format_size(vram_bytes)} bytes: {exc}"
            )
            self.log.error(traceback.format_exc())
            self.report_error(
                f'Kernel "{self.kernel_id}" Failed to Simulate Checkpointing',
                f"{type(exc).__name__} while simulating checkpointing with remote storage "
                f"{remote_storage_name} and data of size {format_size(vram_bytes)} bytes: {exc}",
            )
            raise exc  # re-raise

        duration: float = time.time() - start_time
        self.log.debug(
            f"Finished simulated checkpointing of {format_size(vram_bytes)} bytes to remote storage "
            f"{remote_storage_name} in {duration} seconds."
        )

        return duration

    async def schedule_notify_execution_complete(self, term_number: int) -> Optional[asyncio.Task]:
        """
        Schedule the proposal of an "execution complete" notification for this election.

        The effect of this call depends upon whether we're a single-replica or multi-replica deployment.

        For multi-replica deployments, this will notify the follower/non-primary replicas that we're done
        executing the user-submitted code, and that they're up-to-date in terms of receiving state updates
        from the RaftLog.

        For single-replica deployments, this will prompt the synchronizer to write a list of keys to remote
        remote_storage (again, either Redis or AWS S3) at a deterministic key based on our persistent ID. This list
        of keys is used if and when we (this kernel) is recreated in a new container for a future execution.
        Specifically, we'll read the list of keys, and then we'll read the data for each key in the list. Doing
        so will restore our runtime state.
        """
        if not hasattr(self, "synchronizer") or self.synchronizer is None:
            self.log.warning("No Sychronizer. Cannot notify that execution is complete.")
            return None

        # Add task to the set. This creates a strong reference.
        # We don't await this here so that we can go ahead and send the shell response back.
        # We'll notify our peer replicas in time.
        task: asyncio.Task = asyncio.create_task(self.synchronizer.notify_execution_complete(term_number))

        # We need to save a reference to this task to prevent it from being garbage collected mid-execution.
        # See the docs for details: https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
        self.background_tasks.add(task)

        # To prevent keeping references to finished tasks forever, we make each task remove its own reference
        # from the "background tasks" set after completion.
        task.add_done_callback(self.background_tasks.discard)

        return task

    async def do_shutdown(self, restart):
        self.log.info("Replica %d of kernel %s is shutting down.", self.smr_node_id, self.kernel_id)

        # Make sure we're not (still) preparing to migrate.
        # If we are, then we need to wait to ensure that all important state is checkpointed to remote storage.
        async with self.preparing_to_migrate_cv:
            while self.preparing_to_migrate:
                self.log.warning("We are currently preparing to migrate. "
                                 "Will wait until migration completes before shutting down.")

                await self.preparing_to_migrate_cv.wait()

            self.log.debug("We are not (or are no longer) preparing to migrate and can safely shut down.")

        async with self.checkpointing_state_cv:
            while self.checkpointing_state:
                self.log.debug("We're currently checkpointing state. Waiting to return from 'Prepare to Migrate'.")

                await self.checkpointing_state_cv.wait()

        if hasattr(self, "synchronizer"):
            self.log.info("Closing the Synchronizer.")
            self.synchronizer.close()
            self.log.info("Successfully closed the Synchronizer.")

        # The value of self.synclog_stopped will be True if `prepare_to_migrate` was already called.
        if self.synclog:
            if self.remove_on_shutdown:
                self.log.info(
                    "Removing node %d (that's me) from the SMR cluster.", self.smr_node_id
                )
                try:
                    await self.synclog.remove_node(self.smr_node_id)
                    self.log.info(f"Successfully removed node {self.smr_node_id} "
                                  f"(that's me) from the SMR cluster.")
                except TimeoutError:
                    self.log.error(f"Removing self (node {self.smr_node_id}) from "
                                   f"the SMR cluster timed-out. Continuing onwards.")
                except Exception as ex:
                    self.log.error(f"Failed to remove replica {self.smr_node_id} "
                                   f"of kernel {self.kernel_id} (self).")
                    return gen_error_response(ex), False
            else:
                self.log.info("Not stopping/removing node from etcd/raft cluster.")

            if not self.synclog_stopped:
                self.log.info("Closing the SyncLog (and therefore the etcd-Raft process) now.")
                try:
                    self.synclog.close()
                    self.log.info("SyncLog closed successfully. "
                                  "Writing etcd-Raft data directory to RemoteStorage now.")
                except Exception:
                    self.log.error(f"SyncLog::Close failed for replica {self.smr_node_id} "
                                   f"of kernel {self.kernel_id}.")
            else:
                self.log.info("SyncLog has already been stopped.")

        # Disabling debug mode here, as there is apparently a bug/issue when we're shutting down where we
        # call self.kernel.loop.call_later, but we're presumably running in the control thread's IO loop,
        # so this isn't safe...?
        # TODO: Look into this.
        if self.io_loop is not None:
            self.io_loop.asyncio_loop.set_debug(False)  # type: ignore
        if self.control_thread is not None and self.control_thread.io_loop is not None:
            self.control_thread.io_loop.asyncio_loop.set_debug(False)

        # Give time for the "smr_node_removed" message to be sent.
        # time.sleep(2)
        return super().do_shutdown(restart)

    async def __close_synclog_remote_storage_client(self) -> Tuple[dict, bool]:
        """
        Close the remote storage client of the SyncLog.
        """
        try:
            self.synclog.close_remote_storage_client()
        except Exception as e:
            self.log.error(
                "Failed to close the RemoteStorage client within the LogNode."
            )
            tb: list[str] = traceback.format_exception(e)
            for frame in tb:
                self.log.error(frame)

            return gen_error_response(e), False  # "data_directory": waldir_path,

        return {
            "status": "ok",
            "id": self.smr_node_id,
            "kernel_id": self.kernel_id,
        }, True  # "data_directory": waldir_path,

    async def __close_synclog(self) -> Tuple[dict, bool]:
        """
        Close our SyncLog. This will not close the SyncLog's remote storage client if the SyncLog is of
        type RaftLog. In that case, the remote storage client must be closed explicitly via the
        DistributedKernel's __close_synclog_remote_storage_client method.
        """
        if not hasattr(self, "synclog") or self.synclog is None:
            self.log.warning("We do not have a SyncLog. Cannot close SyncLog.")
            return {
                "status": "ok",
                "id": self.smr_node_id,
                "kernel_id": self.kernel_id,
            }, True

        self.log.info("Closing the SyncLog now.")
        try:
            self.synclog.close()
            self.synclog_stopped = True
            self.log.info("SyncLog closed successfully. Writing SyncLog data directory to RemoteStorage now.")
        except Exception as e:
            self.log.error(
                "Failed to close the SyncLog for replica %d of kernel %s.",
                self.smr_node_id,
                self.kernel_id,
            )
            tb: list[str] = traceback.format_exception(e)
            for frame in tb:
                self.log.error(frame)

            # Report the error to the cluster dashboard (through the Local Daemon and Cluster Gateway).
            self.report_error(
                f"Failed to Close SyncLog for Replica {self.smr_node_id} of Kernel {self.kernel_id}",
                error_message=str(e),
            )

            # Attempt to close the RemoteStorage client.
            self.synclog.close_remote_storage_client()

            return gen_error_response(e), False

        # If the SyncLog is not an instance of RaftLog, then we don't have to worry about writing and copying
        # the SyncLog's data directory to remote storage. We can just return now.
        if not isinstance(self.synclog, RaftLog):
            self.log.debug(f"SyncLog is of type {type(self.synclog).__name__}. "
                           f"Closing remote storage client now.")

            await self.__close_synclog_remote_storage_client()

        return {
            "status": "ok",
            "id": self.smr_node_id,
            "kernel_id": self.kernel_id,
        }, True

    async def __write_synclog_data_dir_to_remote_storage(self) -> Tuple[dict, bool]:
        """
        Write the data directory of the SyncLog to remote storage.
        """

        # Verify that the SyncLog is not None before we continue.
        if self.synclog is None:
            self.log.warning("SyncLog is None. Cannot write data directory to remote storage.")
            return {"status": "ok", "id": self.smr_node_id, "kernel_id": self.kernel_id}, True

        try:
            write_start: float = time.time()

            self.log.debug(
                "Preparing to write state to remote storage. "
                f"Current resource request: {self.current_resource_request}. "
                f"Remote storages ({len(self.remote_storages)}): {self.remote_storages}."
            )

            # Per the Raft documentation, now that we've stopped the SyncLog/RaftLog,
            # we can write its data to intermediate remote_storage.
            waldir_path: str = await self.synclog.write_data_dir_to_remote_storage(
                last_resource_request=self.current_resource_request,
                remote_storage_definitions=self.remote_storages,
            )

            write_duration_ms: float = (time.time() - write_start) * 1.0e3
            if self.prometheus_enabled:
                self.remote_storage_write_latency_milliseconds.labels(
                    session_id=self.kernel_id, workload_id=self.workload_id
                ).observe(write_duration_ms)
            self.log.info(
                'Wrote SyncLog data directory to RemoteStorage. Path: "%s"'
                % waldir_path
            )

            return {
                "status": "ok",
                "id": self.smr_node_id,
                "kernel_id": self.kernel_id,
            }, True  # Didn't fail, we just have nothing to migrate.
        except Exception as e:
            self.log.error(
                "Failed to write the SyncLog data directory of replica %d of kernel %s to RemoteStorage: %s",
                self.smr_node_id,
                self.kernel_id,
                str(e),
            )
            tb: list[str] = traceback.format_exception(e)
            for frame in tb:
                self.log.error(frame)

            # Report the error to the cluster dashboard (through the Local Daemon and Cluster Gateway).
            self.report_error(
                "Failed to Write SyncLog Data Directory", error_message=str(e)
            )

            return gen_error_response(e), False

    async def prepare_to_migrate(self) -> Tuple[dict, bool]:
        self.log.info(
            "Preparing for migration of replica %d of kernel %s.",
            self.smr_node_id,
            self.kernel_id,
        )

        # We don't want to remove this node from the SMR raft cluster when we shut down the kernel,
        # as we're migrating the replica and want to reuse the ID when the raft process resumes
        # on another node.
        self.remove_on_shutdown = False

        if not hasattr(self, "synclog") or self.synclog is None:
            self.log.warning("We do not have a SyncLog. Nothing to do in order to prepare to migrate...")
            return {
                "status": "ok",
                "id": self.smr_node_id,
                "kernel_id": self.kernel_id,
            }, True  # Didn't fail, we just have nothing to migrate.

        async with self.checkpointing_state_cv:
            while self.checkpointing_state:
                self.log.debug("We're currently checkpointing state. Waiting to return from 'Prepare to Migrate'.")

                await self.checkpointing_state_cv.wait()

        # When using RaftLog, we must first stop the SyncLog (RaftLog) before copying the data directory.
        #
        # Reference: https://etcd.io/docs/v2.3/admin_guide/#member-migration

        # Step 1: stop the sync log
        intermediate_resp, ok = await self.__close_synclog()
        if not ok:
            return intermediate_resp, False

        # Step 2: copy the data directory to RemoteStorage
        intermediate_resp, ok = await self.__write_synclog_data_dir_to_remote_storage()
        if not ok:
            await self.__close_synclog_remote_storage_client()
            return intermediate_resp, False

        # Step 3: close the SyncLog's remote storage client if we haven't done so already.
        if isinstance(self.synclog, RaftLog):
            # We'll ignore any errors at this stage.
            await self.__close_synclog_remote_storage_client()

        return {
            "status": "ok",
            "id": self.smr_node_id,
            "kernel_id": self.kernel_id,
        }, True  # "data_directory": waldir_path,

    async def stop_running_training_code_request(self, stream, ident, parent):
        """
        Set the global `run_training_code` flag to False.
        """
        self.log.info("Received 'stop training' instruction.")
        self.local_tcp_server_queue.put(b"stop", block=True, timeout=None)
        self.log.info("Sent 'stop training' instruction to local TCP server.")

        content: dict = {
            "status": "ok",
            # The base class increments the execution count
            "execution_count": self.execution_count,
            "payload": [],
            "user_expressions": {},
        }

        self.session.send(
            stream, "stop_running_training_code_reply", content, parent, ident=ident
        )

    async def prepare_to_migrate_request(self, stream, ident, parent):
        """
        Handle a "prepare-to-migrate" request sent by our local scheduler.

        Args:
            stream (_type_): Jupyter-related.
            ident (_type_): Jupyter-related.
            parent (_type_): Jupyter-related.
        """
        self.log.info("Received 'prepare-to-migrate request'.")

        async with self.persistent_store_cv:
            # TODO(Ben): Do I need to use 'while', or can I just use 'if'?
            if not await self.check_persistent_store():
                self.log.debug("Persistent store is not ready yet. Waiting to handle 'add-replica' request.")
                await self.persistent_store_cv.wait()

        async with self.preparing_to_migrate_cv:
            self.preparing_to_migrate = True

        try:
            content, success = await self.prepare_to_migrate()
        finally:
            # Regardless of what happens up above, we need to flip the value of the self.preparing_to_migrate flag
            # back to false and notify anybody waiting on the condition variable.
            async with self.preparing_to_migrate_cv:
                self.preparing_to_migrate = False
                self.preparing_to_migrate_cv.notify_all()

        if not success:
            self.log.error("Failed to prepare to migrate...")

        self.log.debug("Sending 'prepare_to_migrate_reply' response now.")

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell.
        buffers, _ = self.extract_and_process_request_trace(parent, -1)
        sent_message = self.session.send(
            stream,
            "prepare_to_migrate_reply",
            content,
            parent,
            ident=ident,
            buffers=buffers,
        )

        self.log.debug(f"Sent 'prepare_to_migrate_reply' message: {sent_message}")

    async def do_add_replica(self, replicaId, addr) -> tuple[dict, bool]:
        """Add a replica to the SMR cluster"""
        if not await self.check_persistent_store():
            return gen_error_response(err_wait_persistent_store), False

        self.log.info("Adding replica %d at addr %s now.", replicaId, addr)

        # We didn't check if synclog is ready
        try:
            await self.synclog.add_node(replicaId, "http://{}".format(addr))
            self.log.info(
                "Replica {} at {} has joined the SMR cluster.".format(replicaId, addr)
            )
            return {"status": "ok"}, True
        except Exception as e:
            self.log.error("A replica fails to join: {}...".format(e))
            return gen_error_response(e), False

    def decode_request_trace_from_buffers(self, buffers, msg_id: str = "N/A", msg_type: str = "N/A") -> dict[str, Any]:
        """
        Attempt to decode a RequestTrace from the first buffers frame.

        If the decoding is successful, then the RequestTrace is converted back to a list[bytes].

        Args:
            buffers: the buffers frame that we should try to decode.
            msg_id: the ID of the message whose buffers frame we're decoding; optional, just used for logging.
            msg_type: the type of the message whose buffers frame we're decoding; optional, just used for logging.

        Returns:
            The first buffers frame, extracted and decoded, or an empty dictionary.
        """
        if len(buffers) > 0:
            first_buffers_frame = buffers[0]
        else:
            return {}

        if isinstance(first_buffers_frame, memoryview):
            first_buffers_frame: bytes = first_buffers_frame.tobytes()
            first_buffers_frame: str = first_buffers_frame.decode("utf-8")
        elif isinstance(first_buffers_frame, bytes):
            first_buffers_frame: str = first_buffers_frame.decode("utf-8")
        else:
            first_buffers_frame: str = str(first_buffers_frame)

        try:
            buffers_decoded = json.loads(first_buffers_frame)
            return buffers_decoded
        except json.decoder.JSONDecodeError as ex:
            self.log.warning(f'Failed to decode buffers of "{msg_type}" message "{msg_id}" using JSON because: {ex}')
            self.log.debug(f'Returning empty dictionary for buffers from "{msg_type}" '
                           f'message "{msg_id}" (type={type(first_buffers_frame).__name__}): {first_buffers_frame}')
            return {}

    def update_request_trace(
            self,
            request_trace_frame: Dict[str, Any],
            execution_stats: ExecutionStats,
            msg_id: str,
            msg_type: str,
            received_at: float = 0,
    ):
        """
        Important: this may be called multiple times, such as when sending an "execute_reply" during static, and then
        again later when sending an "execute_statistics" IOPub message. So, we should SET everything in here, rather
        than add/append.
        """
        request_trace: dict[str, Any] = request_trace_frame["request_trace"]

        if received_at > 0:
            received_at = int(math.floor(received_at))
            request_trace["requestReceivedByKernelReplica"] = received_at
        else:
            reply_sent_by_kernel_replica: int = int(math.floor((time.time() * 1.0e3)))
            request_trace["replySentByKernelReplica"] = reply_sent_by_kernel_replica

        request_trace["replicaId"] = self.smr_node_id

        if execution_stats is None:
            return request_trace

        # If the execution_stats parameter is non-null, then embed the included statistics/metrics.
        request_trace["cudaInitMicroseconds"] = int(execution_stats.cuda_init_microseconds)

        request_trace["downloadModelMicroseconds"] = int(execution_stats.download_model_microseconds)

        request_trace["downloadDatasetMicroseconds"] = int(execution_stats.download_training_data_microseconds)
        request_trace["downloadDatasetStartUnixMillis"] = int(execution_stats.download_training_data_start_unix_millis)

        request_trace["downloadDatasetEndUnixMillis"] = int(execution_stats.download_training_data_end_unix_millis)
        request_trace["uploadModelAndTrainingDataMicroseconds"] = int(
            execution_stats.upload_model_and_training_data_microseconds
        )

        request_trace["executionTimeMicroseconds"] = int(execution_stats.execution_time_microseconds)
        request_trace["executionStartUnixMillis"] = int(execution_stats.execution_start_unix_millis)
        request_trace["executionEndUnixMillis"] = int(execution_stats.execution_end_unix_millis)

        request_trace["replayTimeMicroseconds"] = int(execution_stats.replay_time_microseconds)
        request_trace["replayTimeMicroseconds"] = int(execution_stats.replay_time_microseconds)

        request_trace["copyFromCpuToGpuMicroseconds"] = int(execution_stats.copy_data_from_cpu_to_gpu_microseconds)
        request_trace["copyFromGpuToCpuMicroseconds"] = int(execution_stats.copy_data_from_gpu_to_cpu_microseconds)
        request_trace["leaderElectionTimeMicroseconds"] = int(execution_stats.leader_election_microseconds)

        request_trace["syncStartUnixMillis"] = execution_stats.sync_start_unix_millis
        request_trace["syncEndUnixMillis"] = execution_stats.sync_end_unix_millis
        request_trace["syncDurationMillis"] = execution_stats.sync_duration_millis

        request_trace["tokenizeDatasetStart"] = execution_stats.tokenize_training_data_start_unix_millis
        request_trace["tokenizeDatasetEnd"] = execution_stats.tokenize_training_data_end_unix_millis
        request_trace["tokenizeDatasetMicroseconds"] = execution_stats.tokenize_dataset_microseconds

        request_trace["synchronizeUpdatedStateMilliseconds"] = execution_stats.synchronize_updated_state_time_millis
        request_trace["commitExecutionCompleteNotificationMilliseconds"] = execution_stats.commit_exec_end_millis

        request_trace["largeObjectWriteLatenciesMillis"] = self._remote_checkpointer.storage_provider.write_times
        request_trace["largeObjectReadLatenciesMillis"] = self._remote_checkpointer.storage_provider.read_times

        request_trace["synchronizationTimes"] = self.synchronizer.synchronization_times

        # We only want to embed election statistics if this request trace is being embedded in an
        # "execute_request" or "yield_request" message (i.e., a code submission).
        if not self.smr_enabled or (msg_type != "execute_request" and msg_type != "yield_request"):
            return request_trace

        current_election: Election = self.synchronizer.current_election

        # It shouldn't be None, but let's double-check, just to be safe.
        if current_election is None:
            self.log.warning(f"Current Election is None while embedding "
                             f"request trace in '{msg_type}' request '{msg_id}'")
            return request_trace

        # 'current_election_timestamps' is a property. Need to capture its value and use that in a separate
        # if-statement to ensure that it is a valid, non-null reference.
        current_election_timestamps: Optional[ElectionTimestamps] = current_election.current_election_timestamps
        if current_election_timestamps is None:
            self.log.warning(f"Current Election's timestamp data is "
                             f"None while embedding request trace in '{msg_type}' request '{msg_id}'")
            return request_trace

        request_trace["electionCreationTime"] = int(current_election_timestamps.creation_time)
        request_trace["electionProposalPhaseStartTime"] = int(current_election_timestamps.proposal_phase_start_time)
        request_trace["electionExecutionPhaseStartTime"] = int(current_election_timestamps.execution_phase_start_time)
        request_trace["electionEndTime"] = int(current_election_timestamps.end_time)

        self.log.debug(f"Embedding the following RequestTrace in response "
                       f"to '{msg_type}' message:\n{json.dumps(request_trace, indent=2)}")

        return request_trace

    def extract_and_process_request_trace(
            self,
            msg: Dict[str, Any],
            received_at: float,
            execution_stats: Optional[ExecutionStats] = None,
    ) -> Optional[List[bytes], Dict[str, Any]]:
        """
        Attempt to extract the RequestTrace dictionary from the (first) buffers frame of the request.

        If successful, populate the RequestTrace with a "request_received_by_kernel_replica" entry or an
        "reply_sent_by_kernel_replica" entry depending on whether a positive value was passed for the
        received_at argument. Then, re-encode the RequestTrace and return a value in the Buffers format that
        can be directly passed to the Session class' send method.

        received_at should be unix milliseconds.

        execution_stats should only be passed when calling extract_and_process_request_trace from execute_request.

        Returns:
            If the extraction of the request trace was successful, then this returns the message's buffers
            with the first frame modified to contain an updated request trace.

            Otherwise, this returns None.
        """
        msg_header: dict[str, Any] = msg.get("header", {})
        msg_type: str = msg_header.get("msg_type", "N/A")
        msg_id: str = msg_header.get("msg_id", "N/A")

        if "buffers" in msg:
            buffers = msg["buffers"]
        else:
            buffers = []

        request_trace_frame: dict[str, Any] = self.decode_request_trace_from_buffers(
            buffers, msg_id=msg_id, msg_type=msg_type
        )

        if isinstance(request_trace_frame, dict) and "request_trace" in request_trace_frame:
            request_trace = self.update_request_trace(
                request_trace_frame=request_trace_frame,
                execution_stats=execution_stats,
                received_at=received_at,
                msg_type=msg_type,
                msg_id=msg_id,
            )

            buffers[0] = json.dumps(request_trace_frame).encode("utf-8")
            msg["buffers"] = buffers
            return buffers, request_trace

        return None, None

    # customized control message handlers
    async def add_replica_request(self, stream, ident, parent):
        """Add a replica to the SMR cluster"""
        params = parent["content"]
        self.log.info("Received 'add-replica' request for replica with id %d, addr %s" % (params["id"], params["addr"]))

        async with self.persistent_store_cv:
            # TODO(Ben): Do I need to use 'while', or can I just use 'if'?
            if not await self.check_persistent_store():
                self.log.debug("Persistent store is not ready yet. Waiting to handle 'add-replica' request.")
                await self.persistent_store_cv.wait()

        if "id" not in params or "addr" not in params:
            err_content: dict = gen_error_response(err_invalid_request)

            # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
            # The first was in dispatch_shell.
            buffers, _ = self.extract_and_process_request_trace(parent, -1)

            self.session.send(
                stream,
                "add_replica_reply",
                err_content,
                parent,
                ident=ident,
                buffers=buffers,
            )

            return

        content, success = await self.do_add_replica(params["id"], params["addr"])

        if success:
            self.log.debug("Notifying session that SMR node was added.")
            self.session.send(
                self.iopub_socket,
                "smr_node_added",
                {
                    "success": True,
                    "persistent_id": self.persistent_id,
                    "id": params["id"],
                    "addr": params["addr"],
                    "kernel_id": self.kernel_id,
                },
                ident=self._topic("smr_node_added"),
            )  # type: ignore
        else:
            self.log.debug("Notifying session that SMR node addition failed.")
            self.session.send(
                self.iopub_socket,
                "smr_node_added",
                {
                    "success": False,
                    "persistent_id": self.persistent_id,
                    "id": params["id"],
                    "addr": params["addr"],
                    "kernel_id": self.kernel_id,
                },
                ident=self._topic("smr_node_added"),
            )  # type: ignore

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell.
        buffers, _ = self.extract_and_process_request_trace(parent, -1)
        self.session.send(stream, "add_replica_reply", content, parent, ident=ident, buffers=buffers)  # type: ignore

    async def do_update_replica(self, replicaId, addr) -> tuple:
        """
        Update a replica to have a new address

        We also reset certain SMR-related state for this replica, as it will have restarted.
        For example, its attempt number(s) for the current term will be starting over.
        """
        if not await self.check_persistent_store():
            return gen_error_response(err_wait_persistent_store), False

        self.log.info("Updating replica %d with new addr %s now.", replicaId, addr)

        # We didn't check if synclog is ready
        try:
            await self.synclog.update_node(replicaId, "http://{}".format(addr))
            self.log.info("Replica {} at {} has been updated.".format(replicaId, addr))
            return {"status": "ok"}, True
        except Exception as e:
            self.log.error("Failed to update replica: {}...".format(e))
            return gen_error_response(e), False

    async def update_replica_request(self, stream, ident, parent):
        """Update a replica to have a new address"""
        params = parent["content"]
        self.log.info(
            "Received 'update-replica' request for replica with id %d, addr %s"
            % (params["id"], params["addr"])
        )

        async with self.persistent_store_cv:
            # TODO(Ben): Do I need to use 'while', or can I just use 'if'?
            if not await self.check_persistent_store():
                self.log.debug("Persistent store is not ready yet. Waiting to handle 'update-replica' request.")
                await self.persistent_store_cv.wait()

        if "id" not in params or "addr" not in params:
            return gen_error_response(err_invalid_request)

        val = self.do_update_replica(params["id"], params["addr"])
        if inspect.isawaitable(val):
            content, success = await val
        else:
            content, success = val

        if success:
            self.log.debug("Notifying session that SMR node was updated.")

            self.session.send(
                self.iopub_socket,
                "smr_node_updated",
                {
                    "success": True,
                    "persistent_id": self.persistent_id,
                    "id": params["id"],
                    "addr": params["addr"],
                    "kernel_id": self.kernel_id,
                },
                ident=self._topic("smr_node_updated"),
            )  # type: ignore
        else:
            self.log.debug("Notifying session that SMR node update failed.")

            self.session.send(
                self.iopub_socket,
                "smr_node_updated",
                {
                    "success": False,
                    "persistent_id": self.persistent_id,
                    "id": params["id"],
                    "addr": params["addr"],
                    "kernel_id": self.kernel_id,
                },
                ident=self._topic("smr_node_updated"),
            )  # type: ignore

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell.
        buffers, _ = self.extract_and_process_request_trace(parent, -1)
        self.session.send(
            stream,
            "update_replica_reply",
            content,
            parent,
            ident=ident,
            buffers=buffers,
        )  # type: ignore

    def report_error(self, error_title: str = "", error_message: str = ""):
        """
        Send an error report/message to our local daemon via our IOPub socket.
        """
        if self.kernel_notification_service_stub is None:
            self.log.warning(f"Cannot send 'error_report' for error \"{error_title}\" "
                             f"as our gRPC connection was never setup.")
            return

        self.log.debug(f"Sending 'error_report' message for error \"{error_title}\" now. "
                       f"Error message: {error_message}")
        self.kernel_notification_service_stub.Notify(
            gateway_pb2.KernelNotification(
                title=error_title,
                message=error_message,
                notificationType=ErrorNotification,
                kernelId=self.kernel_id,
                replicaId=self.smr_node_id,
                containerId=self.docker_container_id,
                containerName=self.docker_container_name,
            )
        )

    def report_warning(self, warning_title: str = "", warning_message: str = ""):
        """
        Send a warning report/message to our local daemon via our IOPub socket.
        """
        if self.kernel_notification_service_stub is None:
            self.log.warning(
                f"Cannot send 'warning_report' for warning \"{warning_message}\" as our gRPC connection was never setup."
            )
            return

        self.log.debug(
            f"Sending 'warning_report' message for warning \"{warning_message}\" now. Warning message: {warning_message}"
        )
        self.kernel_notification_service_stub.Notify(
            gateway_pb2.KernelNotification(
                title=warning_title,
                message=warning_message,
                notificationType=WarningNotification,
                kernelId=self.kernel_id,
                replicaId=self.smr_node_id,
                containerId=self.docker_container_id,
            )
        )

    def send_notification(
            self,
            notification_title: str = "",
            notification_body: str = "",
            notification_type: int = 2,
    ):
        if notification_type < 0 or notification_type > 3:
            raise ValueError(
                'Invalid notification type specified: "%d"', notification_type
            )

        if self.kernel_notification_service_stub is None:
            self.log.warning(
                f"Cannot send '{notification_type}' notification '{notification_title}' as our gRPC connection was never setup."
            )
            return

        self.log.debug(
            f'Sending "{notification_title}" notification of type {notification_type} now...'
        )
        self.kernel_notification_service_stub.Notify(
            gateway_pb2.KernelNotification(
                title=notification_title,
                message=notification_body,
                notificationType=notification_type,
                kernelId=self.kernel_id,
                replicaId=self.smr_node_id,
                containerId=self.docker_container_id,
            )
        )

    def gen_simple_response(self, execution_count=0):
        return {
            "status": "ok",
            # The base class increments the execution count
            "execution_count": self.execution_count,
            "payload": [],
            "user_expressions": {},
        }

    async def __download_model_pointers_committed_while_catching_up(self):
        """
        Download any Model pointers that were committed while we were catching up.
        """
        if len(self.model_pointers_catchup) == 0:
            self.log.debug("There were no ModelPointer objects committed while we were catching up.")
            return

        self.log.debug(f"Retrieving {len(self.model_pointers_catchup)} "
                       f"DeepLearningModels committed during catch-up phase.")
        _st: float = time.time()
        for var_name, model_pointer in self.model_pointers_catchup.items():
            self.log.debug(f"Loading DeepLearningModel '{model_pointer.model_name}' for variable '{var_name}'")

            async with self._user_ns_lock:
                existing_var_or_model: Optional[Any] = self.shell.user_ns.get(var_name, None)

                if existing_var_or_model is not None and not isinstance(existing_var_or_model, DeepLearningModel):
                    self.log.warning(
                        f"Found existing variable '{var_name}' in shell user namespace of type "
                        f"'{type(existing_var_or_model).__name__}'. Variable will be overwritten with "
                        f"DeepLearningModel '{model_pointer.model_name}'."
                    )
                    existing_var_or_model = None  # So that we pass None for the existing_model argument below.

            # Define the 'existing_model' variable and assign it a (non-nil) value if the value we retrieved from the
            # user namespace is (a) non-nil, and (b) is an instance of DeepLearningModel or ModelPointer.
            #
            # Of course, if the variable retrieved from the user namespace is a ModelPointer, its model field will
            # only be non-nil if we were the one who committed the variable, which won't be the case here, as
            # we're catching up.
            existing_model: Optional[DeepLearningModel] = None
            if existing_var_or_model is not None:
                if isinstance(existing_var_or_model, DeepLearningModel):
                    existing_model = existing_var_or_model
                elif isinstance(existing_var_or_model, ModelPointer):
                    existing_model = existing_var_or_model.model

            st: float = time.time()
            try:
                model: DeepLearningModel = self.__load_model_from_remote_storage(
                    model_pointer, existing_model=existing_model
                )
            except Exception as exc:
                self.log.error(f"Failed to load DeepLearningModel '{model_pointer.model_name}' "
                               f"for variable '{var_name}' because: {exc}")
                self.log.error(traceback.format_exc())
                self.report_error(
                    f"Replica {self.smr_node_id} of kernel {self.kernel_id} failed to load "
                    f"DeepLearningModel '{model_pointer.model_name}' for variable '{var_name}' "
                    f"while catching-up",
                    str(exc),
                )
                continue

            et: float = time.time()
            self.log.debug(
                f"Successfully retrieved DeepLearningModel '{model_pointer.model_name}' for variable "
                f"'{var_name}' from remote storage '{self.remote_storage}' in {et - st} seconds."
            )

            async with self._user_ns_lock:
                self.shell.user_ns[var_name] = model

        _et: float = time.time()
        self.log.debug(
            f"Retrieved {len(self.model_pointers_catchup)} models(s) in {_et - _st} seconds."
        )
        self.model_pointers_catchup.clear()

    async def __download_dataset_pointers_committed_while_catching_up(self):
        """
        Download any Dataset pointers that were committed while we were catching up.
        """
        if len(self.dataset_pointers_catchup) == 0:
            self.log.debug(
                "There were no DatasetPointer objects committed while we were catching up."
            )
            return

        self.log.debug(
            f"Retrieving {len(self.model_pointers_catchup)} Datasets committed during catch-up phase."
        )
        _st: float = time.time()
        for var_name, dataset_pointer in self.dataset_pointers_catchup.items():
            self.log.debug(
                f"Loading Dataset '{dataset_pointer.dataset_name}' for variable '{var_name}'"
            )

            st: float = time.time()
            try:
                dataset: CustomDataset = self.__load_dataset_from_remote_storage(
                    dataset_pointer
                )
            except Exception as exc:
                self.log.error(f"Failed to load Dataset '{dataset_pointer.dataset_name}' for variable '{var_name}'")
                self.log.error(traceback.format_exc())
                self.report_error(
                    f"Replica {self.smr_node_id} of kernel {self.kernel_id} failed to load "
                    f"Dataset '{dataset_pointer.dataset_name}' for variable '{var_name}' "
                    f"while catching-up",
                    str(exc),
                )
                continue

            et: float = time.time()
            self.log.debug(
                f"Successfully retrieved Dataset '{dataset_pointer.dataset_name}' for variable "
                f"'{var_name}' from remote storage in {et - st} seconds."
            )

            async with self._user_ns_lock:
                self.shell.user_ns[var_name] = dataset

        _et: float = time.time()
        self.log.debug(
            f"Retrieved {len(self.dataset_pointers_catchup)} dataset(s) in {_et - _st} seconds."
        )
        self.dataset_pointers_catchup.clear()

    async def __download_pointers_committed_while_catching_up(self):
        """
        Download any Dataset and Model pointers that were committed while we were catching up.
        """
        if len(self.model_pointers_catchup) == 0 and len(self.dataset_pointers_catchup) == 0:
            self.log.debug("There were no models or data committed during catch-up phase.")
            return

        self.log.debug("Downloading any models and data that were committed during catch-up phase...")
        await asyncio.gather(
            self.__download_model_pointers_committed_while_catching_up(),
            self.__download_dataset_pointers_committed_while_catching_up(),
        )
        self.log.debug("Finished downloading the models and data that were committed during catch-up phase.")

    def __load_model_from_remote_storage(
            self,
            pointer: ModelPointer,
            existing_model: Optional[DeepLearningModel] = None,
            send_iopub_notification: bool = False,
    ) -> Optional[DeepLearningModel]:
        """
        Callback to be executed when a pointer to a DeepLearningModel object is committed to the RaftLog.
        :param pointer: the pointer to the DeepLearningModel object.
        """
        if send_iopub_notification:
            self.session.send(
                self.iopub_socket,
                IOPubNotification.DownloadingModel,
                {
                    "replica_id": self.smr_node_id,
                    "model_name": pointer.model_name,
                },
                ident=self._topic(IOPubNotification.DownloadingModel),
            )

        st: float = time.time()

        try:
            read_st: float = time.time()
            model_state_dict, optimizer_state_dict, criterion_state_dict, constructor_args_state = (
                self._remote_checkpointer.read_state_dicts(pointer)
            )
            read_et: float = time.time()

            self.log.debug(
                f"Read updated model state from remote storage '{self._remote_checkpointer.storage_name}' "
                f"in {read_et - read_st} seconds."
            )

            if self.prometheus_enabled:
                self.remote_storage_read_latency_milliseconds.labels(
                    session_id=self.kernel_id, workload_id=self.workload_id
                ).observe((read_et - read_st) * 1.0e3)

            if not self.smr_enabled or self.num_replicas <= 1:
                assert self.current_execution_stats is not None
                self.current_execution_stats.download_model_microseconds += \
                    (read_et - read_st) * 1.0e6
                self.current_execution_stats.download_model_start_unix_millis += \
                    read_st * 1.0e3
                self.current_execution_stats.download_model_end_unix_millis += \
                    read_et * 1.0e3

        except Exception as exc:
            self.log.error(
                f'Failed to read state dictionaries for model "{pointer.large_object_name}" '
                f'from remote storage "{self._remote_checkpointer.storage_name}" because: {exc}'
            )
            self.log.error(traceback.format_exc())
            raise exc  # re-raise

        try:
            model: DeepLearningModel = load_model(
                model_name=pointer.large_object_name,
                existing_model=existing_model,
                # commented out:
                # out_features should/will be passed via the constructor_args_state dictionary.
                # out_features=pointer.out_features,
                model_state_dict=model_state_dict,
                optimizer_state_dict=optimizer_state_dict,
                criterion_state_dict=criterion_state_dict,
                **constructor_args_state,  # out_features should/will be in this dictionary.
            )
        except Exception as exc:
            self.log.error(f'Failed to load committed model "{pointer.model_name}" because: {exc}')
            self.log.error(traceback.format_exc())
            self.report_error(f'Failed to Load Committed Model "{pointer.model_name}"', str(exc))
            return None

        if send_iopub_notification:
            self.session.send(
                self.iopub_socket,
                IOPubNotification.DownloadedModel,
                {
                    "time_elapsed_sec": time.time() - st,
                    "replica_id": self.smr_node_id,
                    "model_name": pointer.model_name,
                },
                ident=self._topic(IOPubNotification.DownloadedModel),
            )

        return model

    def __load_dataset_from_remote_storage(
            self, pointer: DatasetPointer
    ) -> Optional[CustomDataset]:
        var_name: str = pointer.user_namespace_variable_name
        existing_variable: Any = self.shell.user_ns.get(var_name, None)

        if existing_variable is not None:
            if isinstance(existing_variable, CustomDataset):
                self.log.debug(f'Found existing dataset "{var_name}" in user namespace.')

                # If they match, then we're done here.
                # It is necessarily already downloaded by virtue of being one of our Dataset objects.
                if existing_variable.name == pointer.large_object_name:
                    return None

                self.log.warning(f'Existing dataset "{var_name}" does not match freshly-committed '
                                 f'dataset "{pointer.large_object_name}".')
                self.log.warning(f"Will overwrite existing {existing_variable.name} dataset "
                                 f"\"{var_name}\" with '{pointer.large_object_name}' dataset.")
            else:
                self.log.warning(f'Found existing variable "{var_name}" of type {type(existing_variable).__name__}...')
                self.log.warning(f"Will overwrite existing {type(existing_variable).__name__} variable "
                                 f"\"{var_name}\" with '{pointer.large_object_name}' dataset.")

        try:
            st: float = time.time()
            dataset: CustomDataset = load_dataset(pointer.dataset_description)
            et: float = time.time()
        except Exception as exc:
            self.log.error(f'Failed to load committed dataset "{pointer.large_object_name}" because: {exc}')
            self.log.error(traceback.format_exc())
            self.report_error(f'Failed to Load Committed Dataset "{pointer.large_object_name}"', str(exc))
            return None

        if self.prometheus_enabled:
            self.remote_storage_read_latency_milliseconds.labels(
                session_id=self.kernel_id, workload_id=self.workload_id
            ).observe(dataset.download_duration_sec * 1.0e3)

        self.log.debug(f"Successfully loaded committed dataset \"{pointer.large_object_name}\" (varname='{var_name}') "
                       f"from remote storage in {et - st} seconds.")
        return dataset

    def __dataset_committed(self, pointer: DatasetPointer) -> Optional[CustomDataset]:
        """
        Callback to be executed when a pointer to a Dataset object is committed to the RaftLog.
        :param pointer: the pointer to the Dataset object.
        """
        self.log.debug(f'The "{pointer.large_object_name}" dataset (stored in variable '
                       f"'{pointer.user_namespace_variable_name}') was committed.")

        # If we're catching up, then we'll save a reference to this to be processed later, once
        # we're done catching up so that we have the most up-to-date version of the variable.
        if pointer.proposer_id == self.smr_node_id:
            if self.synclog.needs_to_catch_up:
                self.log.debug(
                    f"Received committed '{pointer.large_object_name}' Dataset pointer proposed by ourselves "
                    f"while catching up. Saving for later."
                )
                self.dataset_pointers_catchup[pointer.user_namespace_variable_name] = pointer
                return None
            else:
                self.log.debug(
                    f"Received committed '{pointer.large_object_name}' Dataset pointer proposed by ourselves. "
                    f"Ignoring."
                )
                return None

        return self.__load_dataset_from_remote_storage(pointer)
        # if dataset is not None:
        #     self.shell.user_ns[pointer.user_namespace_variable_name] = dataset

    def __model_committed(self, pointer: ModelPointer) -> Optional[DeepLearningModel]:
        self.log.debug(
            f'An updated "{pointer.large_object_name}" model (stored in variable '
            f"'{pointer.user_namespace_variable_name}') was committed."
        )

        # If we're catching up, then we'll save a reference to this to be processed later, once
        # we're done catching up so that we have the most up-to-date version of the variable.
        if pointer.proposer_id == self.smr_node_id:
            if self.synclog.needs_to_catch_up:
                self.log.debug(
                    f"Received committed '{pointer.model_name}' Model pointer proposed by ourselves "
                    f"while catching up. Saving for later."
                )
                self.model_pointers_catchup[pointer.user_namespace_variable_name] = (
                    pointer
                )
                return None
            else:
                self.log.debug(
                    f"Received committed '{pointer.model_name}' model pointer proposed by ourselves. "
                    f"Ignoring."
                )
                return None

        # Warning: this does not acquire the _user_ns_lock; however, I don't think this code can run concurrently
        # with any of the other code...? Maybe?
        existing_model: Optional[DeepLearningModel | Any] = self.shell.user_ns.get("model", None)
        if existing_model is not None and not isinstance(existing_model, DeepLearningModel):
            self.log.warning(f"Found existing variable 'model' in shell user namespace of type "
                             f"'{type(existing_model).__name__}'. Variable will be overwritten with "
                             f"DeepLearningModel 'ResNet-18'.")
            existing_model = None  # So that we pass None for the existing_model argument below.

        st: float = time.time()
        model: Optional[DeepLearningModel] = self.__load_model_from_remote_storage(pointer,
                                                                                   existing_model=existing_model)
        et: float = time.time()

        self.log.debug(f'Successfully loaded committed model "{pointer.large_object_name}" in {et - st} seconds.')
        return model

    def large_object_pointer_committed(self, pointer: SyncPointer) -> Optional[CustomDataset | DeepLearningModel]:
        """
        Callback to be executed when a pointer to a large object is committed to the RaftLog.
        :param pointer: the pointer to the large object.
        """
        if isinstance(pointer, DatasetPointer):
            return self.__dataset_committed(pointer)
        elif isinstance(pointer, ModelPointer):
            return self.__model_committed(pointer)
        else:
            self.log.error(f"SyncPointer of unknown or unsupported type committed: {pointer}")
            raise ValueError(f"SyncPointer of unknown or unsupported type committed: {pointer}")

    async def override_shell(self):
        """Override IPython Core"""

        # We only want to do this once.
        # If we re-use this container as a pre-warm container, then we do not want to execute this again,
        # as doing so will set self.old_run_cell to self.run_cell, and self.shell.run_cell will be set to
        # self.run_cell, which will lead to infinte recurion/stack overflow.
        if self.old_run_cell is None:
            self.old_run_cell = self.shell.run_cell  # type: ignore
            self.shell.run_cell = self.run_cell  # type: ignore

        self.shell.transform_ast = self.transform_ast  # type: ignore

    async def send_smr_ready_notification(self) -> None:
        """
        Inform our local daemon that we've joined our SMR cluster.

        If we've been started following a migration, then this should only be called once we're fully caught-up.
        """
        # Notify the client that the SMR is ready.
        self.log.info(f'Sending "smr_ready" notification to Local Daemon. Time elapsed since I was created: '
                      f"{time.time() - self.created_at} seconds.")

        self.session.send(
            self.iopub_socket,
            "smr_ready",
            {"persistent_id": self.persistent_id},
            ident=self._topic("smr_ready"),
        )  # type: ignore

        self.log.info(f"Notified local daemon that SMR is ready. Time elapsed since I was created: "
                      f"{time.time() - self.created_at} seconds.")

    def send_iopub_notification(self, topic: IOPubNotification, content: Optional[Dict[str, Any]] = None):
        self.session.send(
            self.iopub_socket,
            str(topic),
            content,
            ident=self._topic(str(topic)),
        )  # type: ignore

    async def get_synclog(self, store_path) -> SyncLog:
        assert isinstance(self.smr_nodes, list)
        assert isinstance(self.smr_nodes_map, dict)
        assert isinstance(self.smr_node_id, int)
        assert isinstance(self.smr_join, bool)

        # self.log.info("Confirmed node {}".format(self.smr_nodes[self.smr_node_id-1]))
        self.log.info("Confirmed node {}".format(self.smr_nodes_map[self.smr_node_id]))

        peer_addresses = []
        ids = []
        for node_id, addr in self.smr_nodes_map.items():
            peer_addresses.append("http://" + addr)
            ids.append(node_id)

        # Implement dynamic later
        # peer_addresses = map(lambda x: "http://{}".format(x), self.smr_nodes)
        self.log.debug(
            "Passing the following addresses to RaftLog: %s" % str(peer_addresses)
        )
        store = ""
        if enable_storage:
            store = store_path

        sys.stderr.flush()
        sys.stdout.flush()

        self.log.debug("Creating SyncLog now.")

        if self.smr_enabled and self.num_replicas > 1:
            try:
                self.log.debug(f"SMR is enabled and we have {self.num_replicas} replicas. Using RaftLog.")
                self.synclog: Optional[SyncLog|RemoteStorageLog|RaftLog] = RaftLog(
                    self.smr_node_id,
                    base_path=store,
                    kernel_id=self.kernel_id,
                    num_replicas=self.num_replicas,
                    remote_storage_hostname=self.remote_storage_hostname,
                    remote_storage=self.remote_storage,
                    load_data_from_remote_storage=self.should_read_data_from_remote_storage,
                    peer_addresses=peer_addresses,
                    peer_ids=ids,
                    join=self.smr_join,
                    debug_port=self.debug_port,
                    report_error_callback=self.report_error,
                    send_notification_func=self.send_notification,
                    remote_storage_read_latency_callback=self.remote_storage_read_latency_callback,
                    deployment_mode=self.deployment_mode,
                    election_timeout_seconds=self.election_timeout_seconds,
                    loaded_serialized_state_callback=self.loaded_serialized_state_callback,
                    shell_io_loop=self.io_loop.asyncio_loop,
                    send_iopub_notification=self.send_iopub_notification,
                )
            except Exception as exc:
                self.log.error("Error while creating RaftLog: %s" % str(exc))

                # Print the stack.
                stack: list[str] = traceback.format_exception(exc)
                for stack_entry in stack:
                    self.log.error(stack_entry)

                self.report_error(error_title="Failed to Create RaftLog", error_message=str(exc))

                # Sleep for 10 seconds to provide plenty of time for the error-report message to be sent before exiting.
                await asyncio.sleep(10)

                # Terminate.
                await self.do_shutdown(False)

                exit(1)
        else:
            self.log.debug(f"SMR is disabled and/or we only have 1 replica. (We have {self.num_replicas}.) "
                           f"Using RemoteStorageLog.")

            remote_storage_read_latency_milliseconds: Optional[Histogram] = None
            if hasattr(self,
                       "remote_storage_read_latency_milliseconds") and self.remote_storage_read_latency_milliseconds is not None:
                remote_storage_read_latency_milliseconds = self.remote_storage_read_latency_milliseconds

            self.log.debug(f'Creating remote storage provider: "{self.remote_storage.lower()}"')

            if self.remote_storage.lower() == "redis":
                remote_storage_provider: RedisProvider = RedisProvider(
                    host=self.remote_storage_hostname,
                    port=self.redis_port,
                    db=self.redis_database,
                    password=self.redis_password,
                )
            elif self.remote_storage.lower() == "s3" or self.remote_storage.lower() == "aws s3":
                remote_storage_provider: S3Provider = S3Provider(
                    bucket_name=self.remote_storage_hostname,
                    aws_region=self.aws_region,
                )
            else:
                self.log.error(f'Unknown or unsupported remote storage specified: {self.remote_storage.lower()}')
                raise ValueError(f'Unknown or unsupported remote storage specified: {self.remote_storage.lower()}')

            if remote_storage_provider is None:
                self.log.error("Remote storage provider should not be None at this point.")
                raise ValueError("Remote storage provider is None, but it should have been created by now.")

            self.synclog: Optional[SyncLog|RemoteStorageLog|RaftLog] = RemoteStorageLog(
                node_id=self.smr_node_id,
                remote_storage_provider=remote_storage_provider,
                base_path=store,
                prometheus_enabled=self.prometheus_enabled,
                kernel_id=self.kernel_id,
                workload_id=self.workload_id,
                remote_storage_write_latency_milliseconds=remote_storage_read_latency_milliseconds,
            )

        if self.synclog is None:
            self.log.error("SyncLog should not be None at this point.")
            raise ValueError("SyncLog is still None, even though it should have been created by now.")

        self.log.debug("Successfully created SyncLog.")

        return self.synclog

    def loaded_serialized_state_callback(self, state: dict[str, dict[str, Any]] = None):
        """
        This is a callback passed to the RaftLog.

        If this kernel was just migrated, then the RaftLog will read some state from intermediate remote_storage.

        Most of this state belongs to the RaftLog object, but we pass some data to the RaftLog before migrating
        that we'd like to recover, if possible.

        The RaftLog will pass us that data via this callback.
        """
        assert state is not None

        self.log.debug(
            "'loaded serialized state' callback is executing within the DistributedKernel."
        )

        last_resource_request: Optional[
            Dict[str, float | int | List[float] | List[int]]
        ] = state.get("last_resource_request", None)
        if last_resource_request is not None:
            self.log.debug(
                f"Recovered last resource request (before migration) from serialized state: {last_resource_request}"
            )

            self.resource_requests.append(last_resource_request)
            self.current_resource_request = last_resource_request

        remote_storage_definitions: Optional[Dict[str, Any]] = state.get(
            "remote_storage_definitions", None
        )
        if remote_storage_definitions is not None:
            self.log.debug(
                f"Recovered remote storage definitions from serialized state: {remote_storage_definitions}"
            )

            for (
                    remote_storage_name,
                    remote_storage_definition,
            ) in remote_storage_definitions.items():
                self.log.debug(
                    f'Registering remote storage loaded from serialized state: "{remote_storage_name}"'
                )
                self.register_remote_storage_definition(remote_storage_definition)

        self.loaded_serialized_state = True

    def remote_storage_read_latency_callback(self, latency_ms: int):
        """
        This is a callback passed to the RaftLog so that it can publish the RemoteStorage read latency to Prometheus.
        Args:
            latency_ms: the latency incurred by the LogNode's RemoteStorage read operation(s).
        """
        if latency_ms < 0:
            return

        if self.prometheus_enabled:
            self.remote_storage_read_latency_milliseconds.labels(
                session_id=self.kernel_id, workload_id=self.workload_id
            ).observe(latency_ms)

    def exception_while_running_cell(self, raw_cell: str, result: ExecutionResult):
        """
        This is called when there's an exception encountered during the execution of a cell.

        This just prints some information about the execution.
        """
        if result.error_before_exec is not None and result.error_in_exec is None:
            when: str = "before"
        elif result.error_before_exec is None and result.error_in_exec is not None:
            when: str = "while"
        else:
            when: str = "both before and while"

        self.log.debug(f"Exception encountered {when} executing of the following cell:\n\n"
                       f"{ColoredLogFormatter.LIGHT_RED}{ColoredLogFormatter.BOLD}{raw_cell}{ColoredLogFormatter.reset}\n")

        if result.error_before_exec is not None:
            self.log.debug(f"The error that occurred before executing the cell was:\n"
                           f"{ColoredLogFormatter.RED}{ColoredLogFormatter.BOLD}{result}{ColoredLogFormatter.reset}\n")
            tb: t.List[str] = traceback.format_exception(result.error_in_exec)
            tb_str: str = ""
            for _tb in tb:
                tb_str += _tb
            self.log.debug(f"\n{ColoredLogFormatter.LIGHT_RED}{tb_str}{ColoredLogFormatter.reset}\n")

            self.report_error("Error Before Executing Cell",
                              f'Replica {self.smr_node_id} of kernel "{self.kernel_id}" encountered the following exception before executing a cell: {result.error_before_exec}')

        if result.error_in_exec is not None:
            self.log.debug(f"The error that occurred while executing the cell was:\n"
                           f"{ColoredLogFormatter.RED}{ColoredLogFormatter.BOLD}{result}{ColoredLogFormatter.reset}\n")
            tb: t.List[str] = traceback.format_exception(result.error_in_exec)
            tb_str: str = ""
            for _tb in tb:
                tb_str += _tb
            self.log.debug(f"\n{ColoredLogFormatter.LIGHT_RED}{tb_str}{ColoredLogFormatter.reset}\n")

            self.report_error("Error While Executing Cell",
                              f'Replica {self.smr_node_id} of kernel "{self.kernel_id}" encountered the following exception while executing a cell: {result.error_in_exec}')

    def run_cell(
            self,
            raw_cell: str,
            store_history: bool = False,
            silent: bool = False,
            shell_futures: bool = True,
            cell_id: str = None,
    ):
        self.log.debug(f"Running cell:\n"
                       f"{ColoredLogFormatter.LIGHT_BLUE}{ColoredLogFormatter.BOLD}{raw_cell}{ColoredLogFormatter.reset}\n")
        self.source = raw_cell
        self.toggle_outstream(override=True, enable=True)
        result: ExecutionResult = self.old_run_cell(
            raw_cell,
            store_history=store_history,
            silent=silent,
            shell_futures=shell_futures,
            cell_id=cell_id,
        )
        self.toggle_outstream(override=True, enable=False)

        if result.success:
            self.log.debug(f"Successfully ran cell without any errors:\n"
                           f"{ColoredLogFormatter.LIGHT_GREEN}{ColoredLogFormatter.BOLD}{raw_cell}{ColoredLogFormatter.reset}\n")
            self.log.debug(f"Result of successful cell execution:\n"
                           f"{ColoredLogFormatter.GREEN}{ColoredLogFormatter.BOLD}{result}{ColoredLogFormatter.reset}")
        else:
            self.exception_while_running_cell(raw_cell, result)

        return result

    def transform_ast(self, node):
        # self.log.debug("Assigning execution_tree to %s" % str(node))
        self.execution_ast = node
        return node

    def toggle_outstream(self, override=False, enable=True):
        # Is sys.stdout has attribute 'disable'?
        if not hasattr(sys.stdout, "disable"):
            # self.log.warning("sys.stdout didn't initialize with kernel.OutStream.")
            return

        if override:
            sys.stdout.disable = not enable  # type: ignore
            sys.stderr.disable = not enable  # type: ignore

            if sys.stdout.disable:
                self.log.debug("stdout and stderr DISABLED.")
            else:
                self.log.debug("stdout and stderr ENABLED.")
        else:
            sys.stdout.disable = not sys.stdout.disable  # type: ignore
            sys.stderr.disable = not sys.stderr.disable  # type: ignore

            if sys.stdout.disable:
                self.log.debug("stdout and stderr DISABLED.")
            else:
                self.log.debug("stdout and stderr ENABLED.")

    @property
    def user_ns_lock(self):
        return self._user_ns_lock
