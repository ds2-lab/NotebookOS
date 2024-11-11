from __future__ import annotations

import asyncio
import faulthandler
import inspect
import json
import logging
import math
import os
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
from threading import Lock
from typing import Union, Optional, Dict, Any

import debugpy
import grpc
import zmq
from ipykernel import jsonutil
from ipykernel.ipkernel import IPythonKernel
from jupyter_client.jsonutil import extract_dates
from prometheus_client import Counter, Histogram
from prometheus_client import start_http_server
from traitlets import List, Integer, Unicode, Bool, Undefined, Float

from .execution_yield_error import ExecutionYieldError
from .util import extract_header
from ..gateway import gateway_pb2
from ..gateway.gateway_pb2_grpc import KernelErrorReporterStub
from ..logging import ColoredLogFormatter
from ..sync import Synchronizer, RaftLog, CHECKPOINT_AUTO
from ..sync.election import Election


# from traitlets.traitlets import Set


def sigabrt_handler(sig, frame):
    print(f'Received SIGABORT (Python): {sig} {frame}', flush=True)
    sys.stderr.flush()
    sys.stdout.flush()
    sys.exit(0)


def sigint_handler(sig, frame):
    print(f'Received SIGINT (Python): {sig} {frame}', flush=True)
    sys.stderr.flush()
    sys.stdout.flush()
    sys.exit(0)


def sigterm_handler(sig, frame):
    print(f'Received SIGINT (Python): {sig} {frame}', flush=True)
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

DeploymentMode_Kubernetes: str = "KUBERNETES"
DeploymentMode_DockerSwarm: str = "DOCKER-SWARM"
DeploymentMode_DockerCompose: str = "DOCKER-COMPOSE"
DeploymentMode_Local: str = "LOCAL"

SMR_LEAD_TASK: str = "smr_lead_task"

storage_base_default = os.path.dirname(os.path.realpath(__file__))
smr_port_default = 10000
err_wait_persistent_store = RuntimeError(
    "Persistent store not ready, try again later.")
err_failed_to_lead_execution = ExecutionYieldError(
    "Failed to lead the execution.")
err_invalid_request = RuntimeError("Invalid request.")
key_persistent_id = "persistent_id"
enable_storage = True

# Used as the value for an environment variable that was not set.
UNAVAILABLE: str = "N/A"


# logging.basicConfig(level=logging.DEBUG,
#                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s [%(threadName)s (%(thread)d)] ")

def tracefunc(frame, event, arg, indent: list[int] = [0]):
    if event == "call":
        indent[0] += 2
        print("-" * indent[0] + "> call function", frame.f_code.co_name, flush=True)
    elif event == "return":
        print("<" + "-" * indent[0], "exit function", frame.f_code.co_name, flush=True)
        indent[0] -= 2
    return tracefunc


def gen_error_response(err):
    return {'status': 'error',
            'ename': str(type(err).__name__),
            'evalue': str(err),
            'traceback': [],
            }


class DistributedKernel(IPythonKernel):
    # Configurable properties
    storage_base: Union[str, Unicode] = Unicode(storage_base_default,  # type: ignore
                                                help="""Base directory for storage"""
                                                ).tag(config=True)

    smr_port = Integer(smr_port_default,  # type: ignore
                       help="""Port for SMR"""
                       ).tag(config=True)

    smr_node_id = Integer(1,  # type: ignore
                          help="""Node id for SMR"""
                          ).tag(config=True)

    smr_nodes = List([],
                     help="""Initial SMR nodes in the SMR cluster"""
                     ).tag(config=True)

    smr_join = Bool(False,  # type: ignore
                    help="""Join the SMR cluster"""
                    ).tag(config=True)

    should_register_with_local_daemon = Bool(True,
                                             help="""Explicitly register with the local daemon?"""
                                             ).tag(config=True)

    local_daemon_addr: Union[str, Unicode] = Unicode(
        help="""Hostname of the local daemon to register with (when using Docker mode)""").tag(config=True)

    local_tcp_server_port = Integer(5555, help="Port for local TCP server.").tag(config=True)

    persistent_id: Union[str, Unicode] = Unicode(help="""Persistent id for storage""",allow_none=True).tag(config=True)

    pod_name: Union[str, Unicode] = Unicode(
        help="""Kubernetes name of the Pod encapsulating this distributed kernel replica""").tag(config=False)

    node_name: Union[str, Unicode] = Unicode(help="""Kubernetes name of the Node on which the Pod is running""").tag(
        config=False)

    hostname: Union[str, Unicode] = Unicode(
        help="""Hostname of the Pod encapsulating this distributed kernel replica""").tag(config=False)

    hdfs_namenode_hostname: Union[str, Unicode] = Unicode(
        help="""Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this.""").tag(config=True)

    kernel_id: Union[str, Unicode] = Unicode(help="""The ID of the kernel.""").tag(config=False)

    # data_directory: Union[str, Unicode] = Unicode(help="""The etcd-raft WAL/data directory. This will always be equal to the empty string unless we're created during a migration operation.""").tag(config=False)

    debug_port: Integer = Integer(8464, help="""Port of debug HTTP server.""").tag(config=False)

    election_timeout_seconds: Float = Float(10.0,
                                            help="""How long to wait to receive other proposals before making a decision (if we can, like if we have at least received one LEAD proposal). """).tag(
        config=True)

    implementation = 'Distributed Python 3'
    implementation_version = '0.2'
    language = 'no-op'
    language_version = '0.2'
    language_info = {
        'name': 'Any text',
        'mimetype': 'text/plain',
        'file_extension': '.txt',
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
        faulthandler.enable()
        if super().log is not None:
            super().log.setLevel(logging.DEBUG)
        print(f' Kwargs: {kwargs}')

        self.control_msg_types = [
            *self.control_msg_types,
            "add_replica_request",
            "update_replica_request",
            "prepare_to_migrate_request",
            "stop_running_training_code_request",
            "ping_kernel_ctrl_request",
        ]

        self.msg_types = [
            *self.msg_types,
            "yield_request",
            "ping_kernel_shell_request",
        ]

        super().__init__(**kwargs)

        # Used to create strong references to tasks, such as notifying peer replicas that execution has complete.
        self.background_tasks = set()
        self.next_execute_request_msg_id: Optional[str] = None
        self.old_run_cell = None
        self.daemon_registration_socket = None
        self.prometheus_thread = None
        self.prometheus_server = None
        self.source = None
        self.kernel_notification_service_channel: Optional[grpc.Channel] = None
        self.kernel_notification_service_stub: Optional[KernelErrorReporterStub] = None
        self.message_acknowledgements_enabled: bool = True  # default to True
        self.shell_received_at: Optional[float] = None
        self.init_persistent_store_on_start_future: Optional[futures.Future] = None

        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        if "local_tcp_server_port" in kwargs:
            self.log.warning(f"Overriding default value of local_tcp_server_port ({self.local_tcp_server_port}) with "
                             f"value from keyword arguments: {kwargs['local_tcp_server_port']}")
            self.local_tcp_server_port = kwargs["local_tcp_server_port"]

        self.prometheus_enabled: bool = True
        prometheus_port_str: str | int = os.environ.get("PROMETHEUS_METRICS_PORT", 8089)

        try:
            self.prometheus_port: int = int(prometheus_port_str)
        except ValueError as ex:
            self.log.error(
                f"Failed to parse \"PROMETHEUS_METRICS_PORT\" environment variable value \"{prometheus_port_str}\": {ex}")
            self.log.error("Will use default prometheus metrics port of 8089.")
            self.prometheus_port: int = 8089

        if self.prometheus_port > 0:
            self.log.debug(f"Starting Prometheus HTTP server on port {self.prometheus_port}.")
            self.prometheus_server, self.prometheus_thread = start_http_server(self.prometheus_port)
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
                documentation="Total number of 'YIELD' proposals.")
            self.num_lead_proposals: Counter = Counter(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_lead_proposals_total",
                documentation="Total number of 'LEAD' proposals.")
            self.hdfs_read_latency_milliseconds: Histogram = Histogram(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_hdfs_read_latency_milliseconds",
                documentation="The amount of time the kernel spent reading data from HDFS.",
                unit="milliseconds",
                buckets=[1, 10, 30, 75, 150, 250, 500, 1000, 2000, 5000, 10e3, 20e3, 45e3, 90e3, 300e3])
            self.hdfs_write_latency_milliseconds: Histogram = Histogram(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_hdfs_write_latency_milliseconds",
                documentation="The amount of time the kernel spent writing data to HDFS.",
                unit="milliseconds",
                buckets=[1, 10, 30, 75, 150, 250, 500, 1000, 2000, 5000, 10e3, 20e3, 45e3, 90e3, 300e3])
            self.registration_time_milliseconds: Histogram = Histogram(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_registration_latency_milliseconds",
                documentation="The latency of a new kernel replica registering with its Local Daemon.",
                unit="milliseconds",
                buckets=[1, 10, 30, 75, 150, 250, 500, 1000, 2000, 5000, 10e3, 20e3, 45e3, 90e3, 300e3])
            self.execute_request_latency: Histogram = Histogram(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="kernel_execute_request_latency_milliseconds",
                documentation="Execution time of the kernels' execute_request method in milliseconds.",
                unit="milliseconds",
                buckets=[10, 100, 250, 500, 1e3, 5e3, 10e3, 20e3, 30e3, 60e3, 300e3, 600e3, 1.0e6, 3.6e6, 7.2e6, 6e7,
                         6e8,
                         6e9])
            self.checkpointing_write_latency_milliseconds: Histogram = Histogram(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="checkpointing_write_latency_milliseconds",
                documentation="Latency in milliseconds of writing checkpointed state to external storage.",
                unit="milliseconds",
                buckets=[10, 500, 1e3, 5e3, 10e3, 15e3, 30e3, 45e3, 60e3, 300e3, 600e3, 1.0e6, 3.6e6, 7.2e6, 6e7, 6e8,
                         6e9])
            self.checkpointing_read_latency_milliseconds: Histogram = Histogram(
                namespace="distributed_cluster",
                subsystem="jupyter",
                name="checkpointing_read_latency_milliseconds",
                documentation="Latency in milliseconds of reading checkpointed state from external storage.",
                unit="milliseconds",
                buckets=[10, 500, 1e3, 5e3, 10e3, 15e3, 30e3, 45e3, 60e3, 300e3, 600e3, 1.0e6, 3.6e6, 7.2e6, 6e7, 6e8,
                         6e9])

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

        self.checkpointing_enabled: bool = len(os.environ.get("SIMULATE_CHECKPOINTING_LATENCY", "")) > 0
        if self.checkpointing_enabled:
            self.log.debug("Checkpointing Latency Simulation is enabled.")
        else:
            self.log.debug("Checkpointing Latency Simulation is disabled.")

        connection_file_path = os.environ.get("CONNECTION_FILE_PATH", "")
        config_file_path = os.environ.get("IPYTHON_CONFIG_PATH", "")

        self.deployment_mode: str = os.environ.get("DEPLOYMENT_MODE", DeploymentMode_Local)
        if len(self.deployment_mode) == 0:
            raise ValueError("Could not determine deployment mode.")
        else:
            self.log.debug(f"Deployment mode: {self.deployment_mode}")

        session_id: str = os.environ.get("SESSION_ID", default=UNAVAILABLE)
        self.kernel_id = os.environ.get("KERNEL_ID", default=UNAVAILABLE)

        if self.deployment_mode == DeploymentMode_Kubernetes:
            self.pod_name = os.environ.get("POD_NAME", default=UNAVAILABLE)
            self.node_name = os.environ.get("NODE_NAME", default=UNAVAILABLE)
            self.docker_container_id: str = "N/A"
        elif self.deployment_mode == DeploymentMode_DockerSwarm or self.deployment_mode == DeploymentMode_DockerCompose:
            self.docker_container_id: str = socket.gethostname()
            self.pod_name = os.environ.get("POD_NAME", default=self.docker_container_id)
            self.node_name = os.environ.get("NODE_NAME", default="DockerNode")
        else:
            self.pod_name = os.environ.get("POD_NAME", default=UNAVAILABLE)
            self.node_name = os.environ.get("NODE_NAME", default=UNAVAILABLE)
            self.docker_container_id: str = "N/A"

        self.log.info("Connection file path: \"%s\"" % connection_file_path)
        self.log.info("IPython config file path: \"%s\"" % config_file_path)
        self.log.info("Session ID: \"%s\"" % session_id)
        self.log.info("Kernel ID: \"%s\"" % self.kernel_id)
        self.log.info("Pod name: \"%s\"" % self.pod_name)
        self.log.info("HDFS NameNode hostname: \"%s\"" %
                      self.hdfs_namenode_hostname)

        if len(self.hdfs_namenode_hostname) == 0:
            raise ValueError("The HDFS hostname is empty. Was it specified in the configuration file?")

        try:
            # The amount of CPU used by this kernel replica (when training) in millicpus (1/1000th of a CPU core).
            self.spec_cpu: int = int(float(os.environ.get("SPEC_CPU", "0")))
        except ValueError as ex:
            self.log.error(
                f"Failed to parse \"SPEC_CPU\" environment variable \"{os.environ.get('SPEC_CPU')}\" because: {ex}")
            self.spec_cpu = 1

        try:
            # The amount of RAM used by this kernel replica (when training) in megabytes (MB).
            self.spec_mem: float = float(os.environ.get("SPEC_MEM", "0"))
        except ValueError as ex:
            self.log.error(
                f"Failed to parse \"SPEC_MEM\" environment variable \"{os.environ.get('SPEC_MEM')}\" because: {ex}")
            self.spec_mem = 128

        try:
            # The number of GPUs used by this kernel replica (when training).
            self.spec_gpu: int = int(float(os.environ.get("SPEC_GPU", "0")))
        except ValueError as ex:
            self.log.error(
                f"Failed to parse \"SPEC_GPU\" environment variable \"{os.environ.get('SPEC_GPU')}\" because: {ex}")
            self.spec_gpu = 1

        try:
            # The amount of VRAM (i.e., GPU memory) used by this kernel replica (when training) in gigabytes (GB).
            self.spec_vram: float = float(os.environ.get("SPEC_VRAM", "0"))
        except ValueError as ex:
            self.log.error(
                f"Failed to parse \"SPEC_VRAM\" environment variable \"{os.environ.get('SPEC_VRAM')}\" because: {ex}")
            self.spec_vram = 0.128

        self.log.info("CPU: %s, Memory: %s, GPU: %s, VRAM: %s." %
                      (self.spec_cpu, str(self.spec_mem), str(self.spec_gpu), str(self.spec_vram)))

        # This should only be accessed from the control IO loop (rather than the main/shell IO loop).
        self.persistent_store_cv = asyncio.Condition()

        # If we're part of a migration operation, then it will be set when we register with the local daemon.
        self.should_read_data_from_hdfs: bool = False

        connection_info: dict[str, Any] = {}
        try:
            if len(connection_file_path) > 0:
                with open(connection_file_path, 'r') as connection_file:
                    connection_info = json.load(connection_file)
        except Exception as ex:
            self.log.error(
                "Failed to obtain connection info from file \"%s\" because: %s" % (connection_file_path, str(ex)))

        self.log.info("Connection info: %s" % str(connection_info))

        # Allow setting env variable to prevent registration altogether. 
        skip_registration_override: bool = (os.environ.get("SKIP_REGISTRATION", "false").lower() == "true")

        if self.should_register_with_local_daemon and not skip_registration_override:
            registration_start: float = time.time()
            self.register_with_local_daemon(connection_info, session_id)
            registration_duration: float = (time.time() - registration_start) * 1.0e3

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
            self.log.warning(f"[Local TCP Server] Port set to {self.local_tcp_server_port}. Returning immediately.")
            return

        self.log.info(f"[Local TCP Server] Starting. Port: {self.local_tcp_server_port}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('127.0.0.1', self.local_tcp_server_port))
        sock.listen(5)
        self.log.info(f"[Local TCP Server] Started. Port: {self.local_tcp_server_port}")

        while True:
            conn, addr = sock.accept()

            self.log.info(
                f"[Local TCP Server] Received incoming connection (addr={addr}). Training should have started.")

            queue.get(block=True, timeout=None)

            self.log.info("[Local TCP Server] Received 'STOP' instruction from Cluster.")

            conn.send(b"stop")

            self.log.info("[Local TCP Server] Sent 'STOP' instruction to shell thread via TCP.")

            conn.close()

            # config_info:dict,

    def register_with_local_daemon(self, connection_info: dict, session_id: str):
        self.log.info("Registering with local daemon now.")

        local_daemon_service_name = os.environ.get("LOCAL_DAEMON_SERVICE_NAME", default=self.local_daemon_addr)
        server_port = os.environ.get("LOCAL_DAEMON_SERVICE_PORT", default=8075)
        try:
            server_port = int(server_port)
        except ValueError:
            server_port = 8075

        self.log.info("Local Daemon network address: \"%s:%d\"" % (local_daemon_service_name, server_port))

        self.daemon_registration_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)

        start_time: float = time.time()
        try:
            self.daemon_registration_socket.connect((local_daemon_service_name, server_port))
        except Exception as ex:
            self.log.error("Failed to connect to LocalDaemon at %s:%d" % (local_daemon_service_name, server_port))
            self.log.error("Reason: %s" % str(ex))
            raise ex

        registration_payload = {
            "op": "register",
            "signatureScheme": connection_info.get("signature_scheme", "hmac-sha256"),
            "key": connection_info["key"],
            "replicaId": self.smr_node_id,
            "numReplicas": len(self.smr_nodes_map),
            "join": self.smr_join,
            "podName": self.pod_name,
            "nodeName": self.node_name,
            "connection-info": connection_info,
            "kernel": {
                "id": self.kernel_id,
                "session": session_id,
                "signature_scheme": connection_info["signature_scheme"],
                "key": connection_info["key"],
            }
        }

        self.log.info("Sending registration payload to local daemon: %s" % str(registration_payload))

        bytes_sent: int = self.daemon_registration_socket.send(json.dumps(registration_payload).encode())

        self.log.info("Sent %d byte(s) to local daemon." % bytes_sent)

        response: bytes = self.daemon_registration_socket.recv(1024)

        if len(response) == 0:
            self.log.error("Received empty (i.e., 0 bytes in length) response from local daemon during registration...")
            raise ValueError("received empty response from local daemon during registration procedure")

        self.log.info(f"Received {len(response)} byte(s) in response from LocalDaemon after "
                      f"{time.time() - start_time} seconds. Response payload: {str(response)}")

        response_dict = json.loads(response)
        self.smr_node_id: int = response_dict["smr_node_id"]
        self.hostname = response_dict["hostname"]

        # self.smr_nodes = [hostname + ":" + str(self.smr_port) for hostname in response_dict["replicas"]]

        if "replicas" not in response_dict:
            self.log.error("No replicas contained in registration response from local daemon.")
            self.log.error("Registration response:")
            for k, v in response_dict.items():
                self.log.error(f"\t{k}: {v}")
            raise ValueError("registration response from local daemon did not contained a \"replicas\" entry")

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
            raise ValueError("registration response from local daemon did not contained a \"replicas\" entry")

        self.num_replicas: int = len(replicas)
        self.smr_nodes_map = {int(node_id_str): (node_addr + ":" + str(self.smr_port))
                              for node_id_str, node_addr in replicas.items()}

        # If we're part of a migration operation, then we should receive both a persistent ID AND an HDFS Data Directory.
        # If we're not part of a migration operation, then we'll JUST receive the persistent ID.
        if "persistent_id" in response_dict:
            self.log.info("Received persistent ID from registration: \"%s\"" % response_dict["persistent_id"])
            self.persistent_id = response_dict["persistent_id"]

        # We'll also use this as an indicator of whether we should simulate additional checkpointing overhead.
        self.should_read_data_from_hdfs = response_dict.get("should_read_data_from_hdfs", False)

        if self.should_read_data_from_hdfs:
            self.log.debug("We SHOULD read data from HDFS.")
        else:
            self.log.debug("We should NOT read data from HDFS.")

        if "debug_port" in response_dict:
            self.debug_port: int = int(response_dict["debug_port"])
            self.log.info(f"Assigned debug port to {self.debug_port}")
        else:
            self.log.warning("No \"debug_port\" entry found in response from local daemon.")
            self.debug_port: int = -1

        if "message_acknowledgements_enabled" in response_dict:
            self.message_acknowledgements_enabled = bool(response_dict["message_acknowledgements_enabled"])

            if self.message_acknowledgements_enabled:
                self.log.debug("Message acknowledgements are enabled.")
            else:
                self.log.debug("Message acknowledgements are disabled.")
        else:
            self.log.warning("No \"message_acknowledgements_enabled\" entry in response from local daemon.")

        self.log.info("Received SMR Node ID after registering with local daemon: %d" % self.smr_node_id)
        self.log.info("Replica hostnames: %s" % str(self.smr_nodes_map))

        assert (self.smr_nodes_map[self.smr_node_id] == (
                self.hostname + ":" + str(self.smr_port)))

        grpc_port: int = response_dict.get("grpc_port", -1)
        if grpc_port == -1:
            self.log.error("Received -1 for gRPC port from registration payload.")
            exit(1)

        # Establish gRPC connection with Local Daemon so that we can send notifications if/when errors occur.
        self.kernel_notification_service_channel = grpc.insecure_channel(f"{local_daemon_service_name}:{grpc_port}")
        self.kernel_notification_service_stub = KernelErrorReporterStub(self.kernel_notification_service_channel)

        # self.kernel_notification_service_stub.Notify(gateway_pb2.KernelNotification(
        #     title="Kernel Replica Registered with Local Daemon",
        #     message=f"Replica {self.smr_node_id} of Kernel {self.kernel_id} has registered with its Local Daemon.",
        #     notificationType=InfoNotification,
        #     kernelId=self.kernel_id,
        #     replicaId=self.smr_node_id,
        # ))

        self.daemon_registration_socket.close()

    def init_debugpy(self):
        if not self.debugpy_enabled or self.debug_port < 0:
            self.log.debug("debugpy server is disabled or server port is set to a negative number.")
            return

        debugpy_port: int = self.debug_port + 1000
        self.log.debug(f"Starting debugpy server on 0.0.0.0:{debugpy_port}")
        debugpy.listen(("0.0.0.0", debugpy_port))

        if self.debugpy_wait_for_client:
            self.log.debug("Waiting for debugpy client to connect before proceeding.")
            debugpy.wait_for_client()
            self.log.debug("Debugpy client has connected. We may now proceed.")
            debugpy.breakpoint()
            self.log.debug("Should have broken on the previous line!")

    def start(self):
        self.log.info("DistributedKernel is starting. Persistent ID = \"%s\"" % self.persistent_id)

        super().start()

        self.init_debugpy()

        if self.persistent_id != Undefined and self.persistent_id != "" and self.persistent_id is not None:
            assert isinstance(self.persistent_id, str)

            def init_persistent_store_done_callback(f: futures.Future):
                """
                Simple callback to print a message when the initialization of the persistent store completes.
                """
                if f.cancelled():
                    self.log.error("Initialization of Persistent Store on-start has been cancelled...")

                    try:
                        self.log.error(
                            f"Initialization of Persistent Store apparently raised an exception: {f.exception()}")
                    except:  # noqa
                        self.log.error(f"No exception associated with cancelled initialization of Persistent Store.")
                elif f.done():
                    self.log.debug("Initialization of Persistent Store has completed on the Control Thread's IO loop.")

            self.log.debug(f"Scheduling creation of init_persistent_store_on_start_future. Loop is running: {self.control_thread.io_loop.asyncio_loop.is_running()}")
            self.init_persistent_store_on_start_future: futures.Future = asyncio.run_coroutine_threadsafe(
                self.init_persistent_store_on_start(self.persistent_id), self.control_thread.io_loop.asyncio_loop)
            self.init_persistent_store_on_start_future.add_done_callback(init_persistent_store_done_callback)
        else:
            self.log.warning(
                "Will NOT be initializing Persistent Store on start, as persistent ID is not yet available.")

    async def init_persistent_store_on_start(self, persistent_id: str):
        self.log.info(f"Initializing Persistent Store on start, as persistent ID is available: \"{persistent_id}\"")
        # Create future to avoid duplicate initialization
        future = asyncio.Future(loop=asyncio.get_running_loop())
        self.store = future
        self.store = await self.init_persistent_store_with_persistent_id(persistent_id)
        self.log.info(f"Persistent store confirmed on start: {self.store}")

        faulthandler.dump_traceback(file=sys.stderr)

    async def kernel_info_request(self, stream, ident, parent):
        """Handle a kernel info request."""
        if not self.session:
            return
        content = {"status": "ok"}
        content.update(self.kernel_info)

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell or process_control.
        buffers: Optional[list[bytes]] = self.extract_and_process_request_trace(parent, -1)

        msg = self.session.send(stream, "kernel_info_reply", content, parent, ident, buffers=buffers)
        self.log.debug(f"Sent \"kernel_info_reply\" message: {msg}")

    async def dispatch_shell(self, msg):
        """
        Override of the base class' dispatch_shell method. We completely override it here.

        That is, we do not call the base class' dispatch_shell method at all.
        """
        self.shell_received_at: float = time.time() * 1.0e3

        if not self.session:
            self.log.error(f"Received SHELL message, but our Session is None...")
            self.log.error(f"Will be discarding SHELL message: {msg}")
            return

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
                            "Shell \"{msg_type}\" message with ID=\"{msg_id}\" is invalid due to an incorrect signature.")
                        self.log.error(
                            "Signature generated from signing frames 1 through 5 of message does not produce "
                            "the signature included within the message.")
                        self.log.error(f"Signature included in message: \"{signature}\". "
                                       f"Signature produced during verification process: \"{check}\".")
                        self.log.error(f"Frames 1 through 5:")
                        for i in range(1, 5, 1):
                            self.log.error(f"Frame #{i}: {msg_list[i]}")
                        self.log.error("All frames of message:")
                        for i, frame in enumerate(msg_list):
                            self.log.error(f"Frame #{i}: {frame}")
            else:
                self.log.error(f"Invalid shell message \"{msg_id}\" of type \"{msg_type}\": {msg_list}\n\n")

            message["msg_id"] = header["msg_id"]
            message["msg_type"] = header["msg_type"]
            message["parent_header"] = extract_dates(self.session.unpack(msg_list[2]))
            message["metadata"] = self.session.unpack(msg_list[3])

            # Try to ACK anyway; we'll just have to use incomplete information. But we should be able to get the job done via the identities...
            self.send_ack(self.shell_stream, msg_type, msg_id, idents, message, stream_name="shell")  # Send an ACK.

            self.report_error(
                error_title="Kernel Replica Received an Invalid Shell Message",
                error_message=f"Replica {self.smr_node_id} of Kernel {self.kernel_id} has received an invalid shell message: {ex}."
            )

            return

        # Set the parent message for side effects.
        self.set_parent(idents, msg, channel="shell")
        self._publish_status("busy", "shell")

        # self.log.info(f"Received SHELL message: {str(msg_deserialized)}")
        msg_id: str = msg["header"]["msg_id"]
        msg_type: str = msg["header"]["msg_type"]
        self.log.debug("==============================================================================================")
        self.log.debug(f"Received SHELL message {msg_id} of type \"{msg_type}\": {msg}")
        self.log.debug("==============================================================================================")
        sys.stderr.flush()
        sys.stdout.flush()
        self.send_ack(self.shell_stream, msg_type, msg_id, idents, msg,
                      stream_name="shell")  # Send an ACK.

        # Only abort execute requests
        if self._aborting and msg_type == "execute_request":
            self.log.warning("We're aborting, and message is an \"execute_request\". Won't be processing it...")
            self._send_abort_reply(self.shell_stream, msg, idents)
            self._publish_status("idle", "shell")
            # flush to ensure reply is sent before
            # handling the next request
            if self.shell_stream:
                self.shell_stream.flush(zmq.POLLOUT)
            return

        if not self.should_handle(self.shell_stream, msg, idents):
            self.log.warning(f"We should not handle shell \"{msg_type}\" message \"{msg_id}\". Returning.")
            return

        # The first time we call 'extract_and_process_request_trace' for this request.
        # The second time will be in the handler itself.
        buffers: Optional[list[bytes]] = self.extract_and_process_request_trace(msg, self.shell_received_at)

        handler = self.shell_handlers.get(msg_type, None)
        if handler is None:
            self.log.warning("Unknown shell message type: \"%r\"", msg_type)
        else:
            try:
                self.pre_handler_hook()
            except Exception as ex:
                self.log.debug("Unable to signal in pre_handler_hook:", exc_info=True)
                self.report_error(
                    error_title=f"Kernel \"{self.kernel_id}\" Encountered {type(ex).__name__} Exception "
                                f"While Signaling in pre_handler_hook for Shell Request \"{msg_id}\" of "
                                f"type \"{msg_type}\"",
                    error_message=str(ex))
            try:
                result = handler(self.shell_stream, idents, msg)
                if inspect.isawaitable(result):
                    await result
            except Exception as ex:
                self.log.error(f"Exception in shell message handler for \"{msg_type}\" message \"{msg_id}\": {ex}",
                               exc_info=True)  # noqa: G201
                self.report_error(
                    error_title=f"Kernel \"{self.kernel_id}\" Encountered {type(ex).__name__} Exception "
                                f"While Executing Handler for Shell Request \"{msg_id}\" of type \"{msg_type}\"",
                    error_message=str(ex))
            except KeyboardInterrupt:
                # Ctrl-c shouldn't crash the kernel here.
                self.log.error("KeyboardInterrupt caught in kernel.")
            finally:
                try:
                    self.post_handler_hook()
                except Exception as ex:
                    self.log.debug("Unable to signal in post_handler_hook:", exc_info=True)
                    self.report_error(
                        error_title=f"Kernel \"{self.kernel_id}\" Encountered {type(ex).__name__} Exception "
                                    f"While Signaling in post_handler_hook for Shell Request \"{msg_id}\" of "
                                    f"type \"{msg_type}\"",
                        error_message=str(ex))

        self.log.debug(f"Finished processing shell message {msg_id} of type \"{msg_type}\"")
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
                    f"Received duplicate \"{msg_id}\" message with ID={msg_type} and \"force_reprocess\" equal to {force_reprocess}.")
                # Presumably this will be True, but if it isn't True, then we definitely shouldn't reprocess the message.
                return force_reprocess
            else:
                self.log.warning(f"Received duplicate \"{msg_id}\" message with ID={msg_type} and no "
                                 f"\"force_reprocess\" entry in the request's metadata...")

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
            self.kernel_notification_service_stub.Notify(gateway_pb2.KernelNotification(
                title="Kernel Replica Received an Invalid Control Message",
                message=f"Replica {self.smr_node_id} of Kernel {self.kernel_id} has received an invalid control message: {ex}.",
                notificationType=ErrorNotification,
                kernelId=self.kernel_id,
                replicaId=self.smr_node_id,
            ))

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
            self.send_ack(self.control_stream, msg_type, msg_id, idents, message, stream_name="control")  # Send an ACK.
            if self.control_stream:
                self.control_stream.flush(zmq.POLLOUT)

            return

        header: dict = msg["header"]
        msg_type: str = header["msg_type"]
        msg_id: str = header["msg_id"]

        self.log.debug("==============================================================================================")
        self.log.debug(f"Received control message {msg_id} of type \"{msg_type}\"")
        self.log.debug("==============================================================================================")
        sys.stderr.flush()
        sys.stdout.flush()

        # The first time we call this method for this request.
        self.extract_and_process_request_trace(msg, received_at)

        # Set the parent message for side effects.
        self.set_parent(idents, msg, channel="control")
        self._publish_status("busy", "control")

        self.send_ack(self.control_stream, msg_type, msg_id, idents, msg, stream_name="control")  # Send an ACK.
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

        self.log.debug(f"Finished processing control message {msg_id} of type \"{msg_type}\"")
        sys.stderr.flush()
        sys.stdout.flush()

    async def init_persistent_store(self, code):
        if await self.check_persistent_store():
            return self.gen_simple_response()

        self.log.info(
            "Initializing persistent datastore now using code \"%s\"" % str(code))

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
            self.log.info(
                "Successfully executed initialization code: \"%s\"" % str(code))
            # Reset execution_count
            self.shell.execution_count = execution_count  # type: ignore
            # type: ignore
            self.persistent_id = self.shell.user_ns[key_persistent_id]
            self.log.info("Persistent ID set: \"%s\"" % self.persistent_id)
            # Initialize persistent store
            self.store = await self.init_persistent_store_with_persistent_id(self.persistent_id)

            # Resolve future
            rsp = self.gen_simple_response()
            future.set_result(rsp)
            self.log.info("Persistent store confirmed: " + self.store)

            return rsp
        except Exception as e:
            self.log.error(
                "Exception encountered during code execution: %s" % str(e))

            self.shell.execution_count = execution_count  # type: ignore
            err_rsp = gen_error_response(e)

            assert isinstance(self.store, asyncio.Future)
            self.store.set_result(err_rsp)
            self.store = None
            return err_rsp

    async def init_persistent_store_with_persistent_id(self, persistent_id: str) -> str:
        """Initialize persistent store with persistent id. Return store path."""
        assert isinstance(self.storage_base, str)
        store_path = os.path.join(self.storage_base, "store", persistent_id)

        self.log.info(
            "Initializing the Persistent Store with Persistent ID: \"%s\"" % persistent_id)
        self.log.debug("Full path of Persistent Store: \"%s\"" % store_path)
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

        # Get synclog for synchronization.
        sync_log = await self.get_synclog(store_path)

        self.init_raft_log_event.set()

        # Start the synchronizer.
        # Starting can be non-blocking, call synchronizer.ready() later to confirm the actual execution_count.
        self.synchronizer = Synchronizer(
            sync_log, module=self.shell.user_module, opts=CHECKPOINT_AUTO)  # type: ignore

        self.init_synchronizer_event.set()

        self.synchronizer.start()

        self.start_synchronizer_event.set()

        sys.stderr.flush()
        sys.stdout.flush()

        # We do this here (and not earlier, such as right after creating the RaftLog), as the RaftLog needs to be started before we attempt to catch-up.
        # The catch-up process involves appending a new value and waiting until it gets committed. This cannot be done until the RaftLog has started.
        # And the RaftLog is started by the Synchronizer, within Synchronizer::start.
        if self.synclog.needs_to_catch_up:
            self.log.debug("RaftLog will need to propose a \"catch up\" value "
                           "so that it can tell when it has caught up with its peers.")
            await self.synclog.catchup_with_peers()

        # Send the 'smr_ready' message AFTER we've caught-up with our peers (if that's something that we needed to do).
        await self.smr_ready()

        self.log.info("Started Synchronizer.")

        self.init_persistent_store_event.set()

        # TODO(Ben): Should this go before the "smr_ready" send?
        # It probably shouldn't matter -- or if it does, then more synchronization is required.
        async with self.persistent_store_cv:
            self.log.debug("Calling `notify_all` on the Persistent Store condition variable.")
            self.persistent_store_cv.notify_all()

        return store_path

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

    def send_ack(self, stream, msg_type: str, msg_id: str, ident, parent, stream_name: str = ""):
        # If message acknowledgements are disabled, then just return immediately.
        if not self.message_acknowledgements_enabled:
            return

        ack_msg = self.session.send(  # type:ignore[assignment]
            stream,
            "ACK",
            {
                "type": msg_type,
                "msg_id": msg_id,
                "source": "PYTHON KERNEL"
            },
            parent,
            ident=ident,
        )
        self.log.debug(f"Sent 'ACK' for {stream_name} {msg_type} message \"{msg_id}\": {ack_msg}. Idents: {ident}")

    async def execute_request(self, stream, ident, parent):
        """Override for receiving specific instructions about which replica should execute some code."""
        start_time: float = time.time()

        parent_header: dict[str, Any] = extract_header(parent)

        self.log.debug(
            f"execute_request called for message with msg_id=\"{parent_header['msg_id']}\". "
            f"identity frame(s): {str(ident)}")

        self.next_execute_request_msg_id: str = parent_header["msg_id"]

        parent_header = extract_header(parent)
        self._associate_new_top_level_threads_with(parent_header)

        if not self.session:
            self.log.error("We don't have a Session. Cannot process 'execute_request'.")
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
            self.report_error("Got Bad \"execute_request\" Message", f"Error: {ex}. Message: {parent}")
            return

        stop_on_error = content.get("stop_on_error", True)

        metadata = self.init_metadata(parent)

        # Re-broadcast our input for the benefit of listening clients, and
        # start computing output
        if not silent:
            self.execution_count += 1
            self._publish_execute_input(code, parent, self.execution_count)

        # Arguments based on the do_execute signature
        do_execute_args = {
            "code": code,
            "silent": silent,
            "store_history": store_history,
            "user_expressions": user_expressions,
            "allow_stdin": allow_stdin,
        }

        if self._do_exec_accepted_params["cell_meta"]:
            do_execute_args["cell_meta"] = cell_meta
        if self._do_exec_accepted_params["cell_id"]:
            do_execute_args["cell_id"] = cell_id

        # Call do_execute with the appropriate arguments
        reply_content = self.do_execute(**do_execute_args)

        if inspect.isawaitable(reply_content):
            reply_content = await reply_content

        # Flush output before sending the reply.
        sys.stdout.flush()
        sys.stderr.flush()
        # FIXME: on rare occasions, the flush doesn't seem to make it to the
        # clients... This seems to mitigate the problem, but we definitely need
        # to better understand what's going on.
        if self._execute_sleep:
            time.sleep(self._execute_sleep)

        # Send the reply.
        reply_content = jsonutil.json_clean(reply_content)
        metadata = self.finish_metadata(parent, metadata, reply_content)

        # Schedule task to wait until this current election either fails (due to all replicas yielding)
        # or until the leader finishes executing the user-submitted code.
        current_election: Election = self.synchronizer.current_election
        term_number: int = current_election.term_number

        metadata["election_metadata"] = current_election.get_election_metadata()

        buffers: Optional[list[bytes]] = self.extract_and_process_request_trace(parent, -1)
        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "execute_reply",
            reply_content,
            parent,
            metadata=metadata,
            ident=ident,
            buffers=buffers,
        )

        self.log.debug(f"Sent \"execute_reply\" message: {reply_msg}")

        if not silent and reply_msg["content"]["status"] == "error" and stop_on_error:
            self._abort_queues()
        task: asyncio.Task = asyncio.create_task(self.synchronizer.wait_for_election_to_end(term_number))

        # We need to save a reference to this task to prevent it from being garbage collected mid-execution.
        # See the docs for details: https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
        self.background_tasks.add(task)

        # To prevent keeping references to finished tasks forever, we make each task remove its own reference
        # from the "background tasks" set after completion.
        task.add_done_callback(self.background_tasks.discard)

        # Wait for the task to end. By not returning here, we ensure that we cannot process any additional
        # "execute_request" messages until all replicas have finished.

        # TODO: Might there still be race conditions where one replica starts processing a future "exectue_request"
        #       message before the others, and possibly starts a new election and proposes something before the
        #       others do?
        self.log.debug(f"Waiting for election {term_number} "
                       "to be totally finished before returning from execute_request function.")
        await task

        end_time: float = time.time()
        duration_ms: float = (end_time - start_time) * 1.0e3

        if self.prometheus_enabled:
            self.execute_request_latency.observe(duration_ms)

    async def ping_kernel_ctrl_request(self, stream, ident, parent):
        """ Respond to a 'ping kernel' Control request. """
        self.log.debug("Ping-Kernel (CONTROL) received.")

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in process_control.
        buffers: Optional[list[bytes]] = self.extract_and_process_request_trace(parent, -1)
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
        """ Respond to a 'ping kernel' Shell request. """
        self.log.debug("Ping-Kernel (SHELL) received.")

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell.
        buffers: Optional[list[bytes]] = self.extract_and_process_request_trace(parent, -1)

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

    async def check_previous_election(self):
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
                self.report_error(
                    "Cannot Find First Election",
                    f"Replica {self.smr_node_id} of kernel {self.kernel_id} thinks it created the first election, but "
                    f"it cannot find any record of that election..."
                )
                exit(1)

            if not first_election.is_in_failed_state:
                self.log.error(f"Current term number is 0, and we've created the first election, "
                               f"but the election is not in failed state. Instead, it is in state {first_election.election_state}.")
                self.report_error(
                    "Election State Error",
                    f"Replica {self.smr_node_id} of kernel {self.kernel_id} has current term number of 0, "
                    f"and it has created the first election, but the election is not in failed state. "
                    f"Instead, it is in state {first_election.election_state}."
                )
                exit(1)

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
                error_title=f"ValueError While Checking Status of Election {term_number}",
                error_message=str(ex)
            )
        except Exception as ex:
            self.log.error(f"Error encountered while checking status of previous election: {ex}")
            self.report_error(
                error_title=f"Unexpected {type(ex).__name__} While Checking Status of Election {term_number}",
                error_message=str(ex)
            )

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
            f"yield_request with msg_id=\"{parent_header['msg_id']}\" called within the Distributed Python Kernel.")
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
            self.kernel_notification_service_stub.Notify(gateway_pb2.KernelNotification(
                title="Kernel Replica Received an Invalid \"yield_request\" Message",
                message=f"Replica {self.smr_node_id} of Kernel {self.kernel_id} has received an invalid \"yield_request\" message: {ex}.",
                notificationType=ErrorNotification,
                kernelId=self.kernel_id,
                replicaId=self.smr_node_id,
            ))
            return

        stop_on_error = content.get("stop_on_error", True)

        metadata = self.init_metadata(parent)

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

            current_term_number = self.synchronizer.execution_count + 1
            self.log.info(f"Calling synchronizer.ready({current_term_number}) now with YIELD proposal.")

            # Pass 'True' for the 'lead' parameter to propose LEAD.
            # Pass 'False' for the 'lead' parameter to propose YIELD.
            #
            # Pass 0 to lead the next execution based on history, which should be passed only if a duplicated execution is acceptable.
            # Pass value > 0 to lead a specific execution.
            # In either case, the execution will wait until states are synchronized.
            # type: ignore
            self.shell.execution_count = await self.synchronizer.ready(current_term_number, False)

            self.log.info(f"Completed call to synchronizer.ready({current_term_number}) with YIELD proposal. "
                          f"shell.execution_count: {self.shell.execution_count}")

            if self.prometheus_enabled:
                self.num_yield_proposals.inc()

            if self.shell.execution_count == 0:  # type: ignore
                self.log.debug("I will NOT leading this execution.")
                reply_content: dict[str, Any] = gen_error_response(err_failed_to_lead_execution)
                # reply_content['yield-reason'] = TODO(Ben): Add this once I figure out how to extract it from the message payloads.
            else:
                self.log.error(
                    f"I've been selected to lead this execution ({self.shell.execution_count}), but I'm supposed to yield!")

                # Notify the client that we will lead the execution (which is bad, in this case, as we were supposed to yield.)
                self.session.send(self.iopub_socket, "smr_lead_after_yield",
                                  {"term": self.synchronizer.execution_count + 1}, ident=self._topic(SMR_LEAD_TASK))
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

        # Be aggressive about clearing the payload because we don't want
        # it to sit in memory until the next execute_request comes in.
        self.shell.payload_manager.clear_payload()

        # Send the reply.
        reply_content: dict[str, Any] = jsonutil.json_clean(reply_content)
        metadata = self.finish_metadata(parent, metadata, reply_content)

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell.
        buffers: Optional[list[bytes]] = self.extract_and_process_request_trace(parent, -1)
        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
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
        term_number: int = current_election.term_number
        self.log.debug(f"Waiting for election {term_number} "
                       "to be totally finished before returning from yield_request function.")
        await self.synchronizer.wait_for_election_to_end(term_number)

    async def do_execute(
            self,
            code: str,
            silent: bool,
            store_history: bool = True,
            user_expressions: dict = None,
            allow_stdin: bool = False,
            *,
            cell_meta=None,
            cell_id=None
    ):
        """
        Execute user code. This is part of the official Jupyter kernel API.
        Reference: https://jupyter-client.readthedocs.io/en/latest/wrapperkernels.html#MyKernel.do_execute

        Args:
            code (str): The code to be executed.
            silent (bool): Whether to display output.
            store_history (bool, optional): Whether to record this code in history and increase the execution count. If silent is True, this is implicitly False. Defaults to True.
            user_expressions (dict, optional): Mapping of names to expressions to evaluate after the code has run. You can ignore this if you need to. Defaults to None.
            allow_stdin (bool, optional): Whether the frontend can provide input on request (e.g. for Python's raw_input()). Defaults to False.

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

        # Special code to initialize persistent store
        if code[:len(key_persistent_id)] == key_persistent_id:
            self.log.debug(
                "Using special code to initialize persistent store: \"%s\"" % code)
            return await asyncio.ensure_future(self.init_persistent_store(code))

        if not await self.check_persistent_store():
            if 'persistent_id' in self.shell.user_ns:
                self.persistent_id = self.shell.user_ns['persistent_id']
                code = "persistent_id = \"%s\"" % self.persistent_id
                await asyncio.ensure_future(self.init_persistent_store(code))

        # Check the status of the last election before proceeding.
        await self.check_previous_election()

        try:
            self.toggle_outstream(override=True, enable=False)

            # Ensure persistent store is ready
            if not await self.check_persistent_store():
                raise err_wait_persistent_store

            term_number: int = self.synchronizer.execution_count + 1
            self.log.info(f"Calling synchronizer.ready({term_number}) now with LEAD proposal.")

            # Pass 'True' for the 'lead' parameter to propose LEAD.
            # Pass 'False' for the 'lead' parameter to propose YIELD.
            #
            # Pass 0 to lead the next execution based on history, which should be passed only if a duplicated execution is acceptable.
            # Pass value > 0 to lead a specific execution.
            # In either case, the execution will wait until states are synchronized.
            # type: ignore
            self.shell.execution_count = await self.synchronizer.ready(term_number, True)

            self.log.info(f"Completed call to synchronizer.ready({term_number}) with LEAD proposal. "
                          f"shell.execution_count: {self.shell.execution_count}")

            if self.prometheus_enabled:
                self.num_lead_proposals.inc()

            if self.shell.execution_count == 0:  # type: ignore
                self.log.debug("I will NOT leading this execution.")
                raise err_failed_to_lead_execution

            self.log.debug(f"I WILL lead this execution ({self.shell.execution_count}).")

            # Notify the client that we will lead the execution.
            # TODO: Eventually, we could pass "gpu" as True or False depending on whether we really
            #       do need GPUs for this training task, assuming we'd know about that here.
            content: dict[str, str | float | bool] = {
                "gpu": True,
                "unix_milliseconds": time.time_ns() // 1_000_000,
                "execute_request_msg_id": self.next_execute_request_msg_id
            }

            self.session.send(self.iopub_socket, SMR_LEAD_TASK, content,
                              ident=self._topic(SMR_LEAD_TASK))  # type: ignore

            self.log.debug("Executing the following code now: %s" % code)

            # Execute code
            reply_routing = super().do_execute(
                code, silent, store_history=store_history,
                user_expressions=user_expressions, allow_stdin=allow_stdin)

            # Wait for the settlement of variables.
            reply_content = await reply_routing

            self.log.info("Returning the following message for do_execute: \"%s\"" % str(reply_content))

            # Disable stdout and stderr forwarding.
            self.toggle_outstream(override=True, enable=False)

            if self.execution_ast is None:
                self.log.warning("Execution AST is None. Synchronization will likely fail...")
            else:
                self.log.debug("Synchronizing now. Execution AST is NOT None.")

            # Synchronize
            coro = self.synchronizer.sync(self.execution_ast, self.source)

            self.execution_ast = None
            self.source = None

            try:
                await coro
            except Exception as ex:
                self.log.error(f"Synchronization failed: {ex}")
                self.kernel_notification_service_stub.Notify(gateway_pb2.KernelNotification(
                    title="Synchronization Error",
                    message=f"Replica {self.smr_node_id} of Kernel {self.kernel_id} has experienced a synchronization error: {ex}.",
                    notificationType=ErrorNotification,
                    kernelId=self.kernel_id,
                    replicaId=self.smr_node_id,
                ))

            assert self.execution_count is not None
            self.log.info("Synchronized. End of sync execution: {}".format(term_number))

            await self.schedule_notify_execution_complete(term_number)

            return reply_content
        except ExecutionYieldError as eye:
            self.log.info("Execution yielded: {}".format(eye))

            return gen_error_response(eye)
        except Exception as e:
            self.log.error("Execution error: {}...".format(e))

            self.report_error("Execution Error", str(e))

            return gen_error_response(e)

    async def schedule_notify_execution_complete(self, term_number: int):
        """
        Schedule the proposal of an "execution complete" notification for this election.
        """

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

    async def do_shutdown(self, restart):
        self.log.info("Replica %d of kernel %s is shutting down.",
                      self.smr_node_id, self.kernel_id)

        if self.synchronizer:
            self.log.info("Closing the Synchronizer.")
            self.synchronizer.close()
            self.log.info("Successfully closed the Synchronizer.")

        # The value of self.synclog_stopped will be True if `prepare_to_migrate` was already called.
        if self.synclog and self.remove_on_shutdown:
            self.log.info(
                "Removing node %d (that's me) from the SMR cluster.", self.smr_node_id)
            try:
                await self.synclog.remove_node(self.smr_node_id)
                self.log.info(
                    "Successfully removed node %d (that's me) from the SMR cluster.", self.smr_node_id)
            except TimeoutError:
                self.log.error(
                    "Removing self (node %d) from the SMR cluster timed-out. Continuing onwards.", self.smr_node_id)
            except Exception as ex:
                self.log.error(
                    "Failed to remove replica %d of kernel %s.", self.smr_node_id, self.kernel_id)
                return gen_error_response(ex), False

            if not self.synclog_stopped:
                self.log.info(
                    "Closing the SyncLog (and therefore the etcd-Raft process) now.")
                try:
                    self.synclog.close()
                    self.log.info(
                        "SyncLog closed successfully. Writing etcd-Raft data directory to HDFS now.")
                except Exception:
                    self.log.error(
                        "Failed to close the SyncLog for replica %d of kernel %s.", self.smr_node_id, self.kernel_id)
            else:
                self.log.info(
                    "I've already removed myself from the SMR cluster and closed my sync-log.")
        else:
            self.log.info("Not stopping/removing node from etcd/raft cluster.")

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

    async def prepare_to_migrate(self) -> tuple[dict, bool]:
        self.log.info("Preparing for migration of replica %d of kernel %s.",
                      self.smr_node_id, self.kernel_id)

        # We don't want to remove this node from the SMR raft cluster when we shut down the kernel,
        # as we're migrating the replica and want to reuse the ID when the raft process resumes
        # on another node.
        self.remove_on_shutdown = False

        if not self.synclog:
            self.log.warning("We do not have a SyncLog. Nothing to do in order to prepare to migrate...")
            return {'status': 'ok', "id": self.smr_node_id,
                    "kernel_id": self.kernel_id}, True  # Didn't fail, we just have nothing to migrate.
        #
        # Reference: https://etcd.io/docs/v2.3/admin_guide/#member-migration
        #
        # According to the official documentation, we must first stop the Raft node before copying the data directory.

        # Step 1: stop the raft node
        self.log.info(
            "Closing the SyncLog (and therefore the etcd-Raft process) now.")
        try:
            self.synclog.close()

            self.synclog_stopped = True
            self.log.info(
                "SyncLog closed successfully. Writing etcd-Raft data directory to HDFS now.")
        except Exception as e:
            self.log.error("Failed to close the SyncLog for replica %d of kernel %s.",
                           self.smr_node_id, self.kernel_id)
            tb: list[str] = traceback.format_exception(e)
            for frame in tb:
                self.log.error(frame)

            # Report the error to the cluster dashboard (through the Local Daemon and Cluster Gateway).
            self.report_error(f"Failed to Close SyncLog for Replica {self.smr_node_id} of Kernel {self.kernel_id}",
                              error_message=str(e))

            # Attempt to close the HDFS client.
            self.synclog.close_hdfs_client()

            return gen_error_response(e), False

        # Step 2: copy the data directory to HDFS
        try:
            write_start: float = time.time()
            waldir_path: str = await self.synclog.write_data_dir_to_hdfs()
            write_duration_ms: float = (time.time() - write_start) * 1.0e3
            if self.prometheus_enabled:
                self.hdfs_write_latency_milliseconds.observe(write_duration_ms)
            self.log.info(
                "Wrote etcd-Raft data directory to HDFS. Path: \"%s\"" % waldir_path)
        except Exception as e:
            self.log.error("Failed to write the data directory of replica %d of kernel %s to HDFS: %s",
                           self.smr_node_id, self.kernel_id, str(e))
            tb: list[str] = traceback.format_exception(e)
            for frame in tb:
                self.log.error(frame)

            # Report the error to the cluster dashboard (through the Local Daemon and Cluster Gateway).
            self.report_error("Failed to Write HDFS Data Directory", error_message=str(e))

            # Attempt to close the HDFS client.
            self.synclog.close_hdfs_client()

            return gen_error_response(e), False

        try:
            self.synclog.close_hdfs_client()
        except Exception as e:
            self.log.error("Failed to close the HDFS client within the LogNode.")
            tb: list[str] = traceback.format_exception(e)
            for frame in tb:
                self.log.error(frame)

            # Report the error to the cluster dashboard (through the Local Daemon and Cluster Gateway).
            self.report_error(
                f"Failed to Close HDFS Client within LogNode of Kernel {self.kernel_id}-{self.smr_node_id}",
                error_message=str(e))

            # We don't return an error here, though. 

        return {'status': 'ok', "id": self.smr_node_id,
                "kernel_id": self.kernel_id}, True  # "data_directory": waldir_path,

    async def stop_running_training_code_request(self, stream, ident, parent):
        """
        Set the global `run_training_code` flag to False.
        """
        self.log.info("Received 'stop training' instruction.")
        self.local_tcp_server_queue.put(b"stop", block=True, timeout=None)
        self.log.info("Sent 'stop training' instruction to local TCP server.")

        content: dict = {
            'status': 'ok',
            # The base class increments the execution count
            'execution_count': self.execution_count,
            'payload': [],
            'user_expressions': {},
        }

        self.session.send(stream, "stop_running_training_code_reply", content, parent, ident=ident)

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
                self.log.debug(
                    "Persistent store is not ready yet. Waiting to handle 'add-replica' request.")
                await self.persistent_store_cv.wait()

        content, success = await self.prepare_to_migrate()

        if not success:
            self.log.error("Failed to prepare to migrate...")

        self.log.debug("Sending 'prepare_to_migrate_reply' response now.")

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell.
        buffers: Optional[list[bytes]] = self.extract_and_process_request_trace(parent, -1)
        sent_message = self.session.send(stream, "prepare_to_migrate_reply", content, parent, ident=ident,
                                         buffers=buffers)

        self.log.debug("Sent 'prepare_to_migrate_reply' message: %s" % str(sent_message))

    async def do_add_replica(self, replicaId, addr) -> tuple[dict, bool]:
        """Add a replica to the SMR cluster"""
        if not await self.check_persistent_store():
            return gen_error_response(err_wait_persistent_store), False

        self.log.info("Adding replica %d at addr %s now.", replicaId, addr)

        # We didn't check if synclog is ready
        try:
            await self.synclog.add_node(replicaId, "http://{}".format(addr))
            self.log.info(
                "Replica {} at {} has joined the SMR cluster.".format(replicaId, addr))
            return {'status': 'ok'}, True
        except Exception as e:
            self.log.error("A replica fails to join: {}...".format(e))
            return gen_error_response(e), False

    def decode_request_trace_from_buffers(
            self,
            buffers,
            msg_id: str = "N/A",
            msg_type: str = "N/A"
    ) -> dict[str, Any]:
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
        # self.log.debug(f"Buffers is a list of length {len(buffers)} for \"{msg_type}\" message \"{msg_id}\".")
        if len(buffers) > 0:
            first_buffers_frame = buffers[0]
            # self.log.debug(f"First buffers frame of \"{msg_type}\" message \"{msg_id}\": {str(first_buffers_frame)}")
        else:
            return {}

        if isinstance(first_buffers_frame, memoryview):
            first_buffers_frame: bytes = first_buffers_frame.tobytes()
            first_buffers_frame: str = first_buffers_frame.decode('utf-8')
        elif isinstance(first_buffers_frame, bytes):
            first_buffers_frame: str = first_buffers_frame.decode('utf-8')
        else:
            first_buffers_frame: str = str(first_buffers_frame)

        try:
            buffers_decoded = json.loads(first_buffers_frame)
            # self.log.debug(f"Successfully decoded buffers \"{msg_type}\" message \"{msg_id}\" "
            #                f"using JSON (type={type(buffers_decoded).__name__}): {buffers_decoded}")
            return buffers_decoded
        except json.decoder.JSONDecodeError as ex:
            self.log.warning(
                f"Failed to decode buffers of \"{msg_type}\" message \"{msg_id}\" using JSON because: {ex}")
            self.log.debug(f"Returning empty dictionary for buffers from \"{msg_type}\" "
                           f"message \"{msg_id}\" (type={type(first_buffers_frame).__name__}): {first_buffers_frame}")
            return {}

    def extract_and_process_request_trace(self, msg: dict[str, Any], received_at: float) -> Optional[list[bytes]]:
        """
        Attempt to extract the RequestTrace dictionary from the (first) buffers frame of the request.

        If successful, populate the RequestTrace with a "request_received_by_kernel_replica" entry or an
        "reply_sent_by_kernel_replica" entry depending on whether a positive value was passed for the
        received_at argument. Then, re-encode the RequestTrace and return a value in the Buffers format that
        can be directly passed to the Session class' send method.

        received_at should be unix milliseconds.

        Returns:
            If the extraction of the request trace was successful, then this returns the message's buffers
            with the first frame modified to contain an updated request trace.

            Otherwise, this returns None.
        """
        msg_header: dict[str, Any] = msg.get('header', {})
        msg_type: str = msg_header.get('msg_type', "N/A")
        msg_id: str = msg_header.get('msg_id', "N/A")
        # self.log.debug(f"Extracting buffers from \"{msg_type}\" message \"{msg_id}\" with {len(msg)} frames now...")

        if "buffers" in msg:
            # self.log.debug(f"Found buffers frame in \"{msg_type}\" message \"{msg_id}\".")
            buffers = msg["buffers"]
        else:
            # frame_names: str = ", ".join(list(msg.keys()))
            # self.log.warning(f"No buffers frame found in \"{msg_type}\" message \"{msg_id}\". "
            #                  f"Message only has the following frames: {frame_names}")
            buffers = []

        request_trace_frame: dict[str, Any] = self.decode_request_trace_from_buffers(buffers, msg_id=msg_id,
                                                                                     msg_type=msg_type)
        if isinstance(request_trace_frame, dict) and "request_trace" in request_trace_frame:
            request_trace: dict[str, Any] = request_trace_frame["request_trace"]

            if received_at > 0:
                received_at = int(math.floor(received_at))
                # self.log.debug(f"Updating \"requestReceivedByKernelReplica\" field in RequestTrace found in "
                #                f"buffers of \"{msg_type}\" message \"{msg_id}\" with value {received_at} now.")
                request_trace["requestReceivedByKernelReplica"] = received_at
            else:
                reply_sent_by_kernel_replica: int = int(math.floor((time.time() * 1.0e3)))
                # self.log.debug(f"Updating \"replySentByKernelReplica\" field in RequestTrace found in "
                #                f"buffers of \"{msg_type}\" message \"{msg_id}\" with value "
                #                f"{reply_sent_by_kernel_replica} now.")
                request_trace["replySentByKernelReplica"] = reply_sent_by_kernel_replica

            request_trace["replicaId"] = self.smr_node_id

            buffers[0] = json.dumps(request_trace_frame).encode('utf-8')
            # self.log.debug(f"Contents of \"buffers\" frame(s) after processing: {str(buffers)}")
            msg["buffers"] = buffers
            return buffers
        # else:
        #     self.log.warning(f"Could not find \"request_trace\" entry in request_trace_frame: {request_trace_frame}")

        return None

    # customized control message handlers
    async def add_replica_request(self, stream, ident, parent):
        """Add a replica to the SMR cluster"""
        params = parent['content']
        self.log.info("Received 'add-replica' request for replica with id %d, addr %s" %
                      (params['id'], params['addr']))

        async with self.persistent_store_cv:
            # TODO(Ben): Do I need to use 'while', or can I just use 'if'?
            if not await self.check_persistent_store():
                self.log.debug(
                    "Persistent store is not ready yet. Waiting to handle 'add-replica' request.")
                await self.persistent_store_cv.wait()

        if 'id' not in params or 'addr' not in params:
            err_content: dict = gen_error_response(err_invalid_request)

            # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
            # The first was in dispatch_shell.
            buffers: Optional[list[bytes]] = self.extract_and_process_request_trace(parent, -1)
            self.session.send(stream, "add_replica_reply", err_content, parent, ident=ident, buffers=buffers)
            return

        content, success = await self.do_add_replica(params['id'], params['addr'])

        if success:
            self.log.debug("Notifying session that SMR node was added.")
            self.session.send(self.iopub_socket, "smr_node_added",
                              {"success": True, "persistent_id": self.persistent_id, "id": params[
                                  'id'], "addr": params['addr'], "kernel_id": self.kernel_id},
                              ident=self._topic("smr_node_added"))  # type: ignore
        else:
            self.log.debug("Notifying session that SMR node addition failed.")
            self.session.send(self.iopub_socket, "smr_node_added",
                              {"success": False, "persistent_id": self.persistent_id, "id": params[
                                  'id'], "addr": params['addr'], "kernel_id": self.kernel_id},
                              ident=self._topic("smr_node_added"))  # type: ignore

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell.
        buffers: Optional[list[bytes]] = self.extract_and_process_request_trace(parent, -1)
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
            self.log.info(
                "Replica {} at {} has been updated.".format(replicaId, addr))
            return {'status': 'ok'}, True
        except Exception as e:
            self.log.error("Failed to update replica: {}...".format(e))
            return gen_error_response(e), False

    async def update_replica_request(self, stream, ident, parent):
        """Update a replica to have a new address"""
        params = parent['content']
        self.log.info("Received 'update-replica' request for replica with id %d, addr %s" %
                      (params['id'], params['addr']))

        async with self.persistent_store_cv:
            # TODO(Ben): Do I need to use 'while', or can I just use 'if'?
            if not await self.check_persistent_store():
                self.log.debug(
                    "Persistent store is not ready yet. Waiting to handle 'update-replica' request.")
                await self.persistent_store_cv.wait()

        if 'id' not in params or 'addr' not in params:
            return gen_error_response(err_invalid_request)

        val = self.do_update_replica(params['id'], params['addr'])
        if inspect.isawaitable(val):
            content, success = await val
        else:
            content, success = val

        if success:
            self.log.debug("Notifying session that SMR node was updated.")

            self.session.send(self.iopub_socket, "smr_node_updated",
                              {"success": True, "persistent_id": self.persistent_id, "id": params[
                                  'id'], "addr": params['addr'], "kernel_id": self.kernel_id},
                              ident=self._topic("smr_node_updated"))  # type: ignore
        else:
            self.log.debug("Notifying session that SMR node update failed.")

            self.session.send(self.iopub_socket, "smr_node_updated",
                              {"success": False, "persistent_id": self.persistent_id, "id": params[
                                  'id'], "addr": params['addr'], "kernel_id": self.kernel_id},
                              ident=self._topic("smr_node_updated"))  # type: ignore

        # This is the SECOND time we're calling 'extract_and_process_request_trace' for this request.
        # The first was in dispatch_shell.
        buffers: Optional[list[bytes]] = self.extract_and_process_request_trace(parent, -1)
        self.session.send(stream, "update_replica_reply",
                          content, parent, ident=ident, buffers=buffers)  # type: ignore

    def report_error(self, error_title: str = "", error_message: str = ""):
        """
        Send an error report/message to our local daemon via our IOPub socket.
        """
        if self.kernel_notification_service_stub is None:
            self.log.error(f"Cannot send 'error_report' for error \"{error_title}\" as our gRPC connection was never setup.")
            return

        self.log.debug(f"Sending 'error_report' message for error \"{error_title}\" now...")
        self.kernel_notification_service_stub.Notify(gateway_pb2.KernelNotification(
            title=error_title,
            message=error_message,
            notificationType=ErrorNotification,
            kernelId=self.kernel_id,
            replicaId=self.smr_node_id,
        ))

    def send_notification(self, notification_title: str = "", notification_body: str = "", notification_type: int = 2):
        if notification_type < 0 or notification_type > 3:
            raise ValueError(f"Invalid notification type specified: \"%d\"", notification_type)

        if self.kernel_notification_service_stub is None:
            self.log.error(f"Cannot send '{notification_type}' notification '{notification_title}' as our gRPC connection was never setup.")
            return

        self.log.debug(f"Sending \"{notification_title}\" notification of type {notification_type} now...")
        self.kernel_notification_service_stub.Notify(gateway_pb2.KernelNotification(
            title=notification_title,
            message=notification_body,
            notificationType=notification_type,
            kernelId=self.kernel_id,
            replicaId=self.smr_node_id,
        ))

    def gen_simple_response(self, execution_count=0):
        return {'status': 'ok',
                # The base class increments the execution count
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
                }

    async def override_shell(self):
        """Override IPython Core"""
        self.old_run_cell = self.shell.run_cell  # type: ignore
        self.shell.run_cell = self.run_cell  # type: ignore
        self.shell.transform_ast = self.transform_ast  # type: ignore

    async def smr_ready(self) -> None:
        """
        Inform our local daemon that we've joined our SMR cluster.

        If we've been started following a migration, then this should only be called once we're fully caught-up.
        """
        # Notify the client that the SMR is ready.
        self.log.info(f"Sending \"smr_ready\" notification to Local Daemon. Time elapsed since I was created: "
                      f"{time.time() - self.created_at} seconds.")

        self.session.send(self.iopub_socket, "smr_ready", {
            "persistent_id": self.persistent_id}, ident=self._topic("smr_ready"))  # type: ignore

        self.log.info(f"Notified local daemon that SMR is ready. Time elapsed since I was created: "
                      f"{time.time() - self.created_at} seconds.")

    async def get_synclog(self, store_path) -> RaftLog:
        assert isinstance(self.smr_nodes, list)
        assert isinstance(self.smr_nodes_map, dict)
        assert isinstance(self.smr_node_id, int)
        assert isinstance(self.smr_join, bool)

        # self.log.info("Confirmed node {}".format(self.smr_nodes[self.smr_node_id-1]))
        self.log.info("Confirmed node {}".format(
            self.smr_nodes_map[self.smr_node_id]))

        peer_addresses = []
        ids = []
        for node_id, addr in self.smr_nodes_map.items():
            peer_addresses.append("http://" + addr)
            ids.append(node_id)

        # Implement dynamic later
        # peer_addresses = map(lambda x: "http://{}".format(x), self.smr_nodes)
        self.log.debug(
            "Passing the following addresses to RaftLog: %s" % str(peer_addresses))
        store = ""
        if enable_storage:
            store = store_path

        sys.stderr.flush()
        sys.stdout.flush()

        self.log.debug("Creating RaftLog now.")
        try:
            self.synclog = RaftLog(self.smr_node_id,
                                   base_path=store,
                                   kernel_id=self.kernel_id,
                                   num_replicas=self.num_replicas,
                                   hdfs_hostname=self.hdfs_namenode_hostname,
                                   should_read_data_from_hdfs=self.should_read_data_from_hdfs,
                                   # data_directory = self.hdfs_data_directory,
                                   peer_addresses=peer_addresses,
                                   peer_ids=ids,
                                   join=self.smr_join,
                                   debug_port=self.debug_port,
                                   report_error_callback=self.report_error,
                                   send_notification_func=self.send_notification,
                                   hdfs_read_latency_callback=self.hdfs_read_latency_callback,
                                   deployment_mode=self.deployment_mode,
                                   election_timeout_seconds=self.election_timeout_seconds)
        except Exception as ex:
            self.log.error("Error while creating RaftLog: %s" % str(ex))

            # Print the stack.
            stack: list[str] = traceback.format_exception(ex)
            for stack_entry in stack:
                self.log.error(stack_entry)

            self.report_error(error_title="Failed to Create RaftLog", error_message=str(ex))

            # Sleep for 10 seconds to provide plenty of time for the error-report message to be sent before exiting. 
            await asyncio.sleep(10)

            # Terminate.
            await self.do_shutdown(False)

            exit(1)

        self.log.debug("Successfully created RaftLog.")

        return self.synclog

    def hdfs_read_latency_callback(self, latency_ms: int):
        """
        This is a callback passed to the RaftLog so that it can publish the HDFS read latency to Prometheus.
        Args:
            latency_ms: the latency incurred by the LogNode's HDFS read operation(s).
        """
        if latency_ms < 0:
            return

        if self.prometheus_enabled:
            self.hdfs_read_latency_milliseconds.observe(latency_ms)

    def run_cell(self, raw_cell, store_history=False, silent=False, shell_futures=True, cell_id=None):
        self.log.debug("Running cell: %s" % str(raw_cell))
        self.source = raw_cell
        self.toggle_outstream(override=True, enable=True)
        result = self.old_run_cell(raw_cell, store_history=store_history,
                                   silent=silent, shell_futures=shell_futures, cell_id=cell_id)
        self.toggle_outstream(override=True, enable=False)
        self.log.debug("Ran cell %s: %s" % (str(raw_cell), str(result)))
        return result

    def transform_ast(self, node):
        self.log.debug("Assigning execution_tree to %s" % str(node))
        self.execution_ast = node
        return node

    def toggle_outstream(self, override=False, enable=True):
        # Is sys.stdout has attribute 'disable'?
        if not hasattr(sys.stdout, 'disable'):
            self.log.warning(
                "sys.stdout didn't initialized with kernel.OutStream.")
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
