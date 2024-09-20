from __future__ import annotations

import asyncio
import faulthandler
import inspect
import json
import logging
import os
import signal
import socket
import sys
import time
import traceback
import typing as t
import uuid
from multiprocessing import Process, Queue
from threading import Lock
from typing import Union, Optional, Dict, Any

import zmq
from ipykernel import jsonutil
from ipykernel.ipkernel import IPythonKernel
from jupyter_client.jsonutil import extract_dates
from traitlets import List, Integer, Unicode, Bool, Undefined

from .util import extract_header
from ..sync import Synchronizer, RaftLog, CHECKPOINT_AUTO


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


signal.signal(signal.SIGABRT, sigabrt_handler)
signal.signal(signal.SIGINT, sigint_handler)
signal.signal(signal.SIGTERM, sigterm_handler)


class ExecutionYieldError(Exception):
    """Exception raised when execution is yielded."""

    def __init__(self, message):
        super().__init__(message)


DeploymentMode_Kubernetes: str = "kubernetes"
DeploymentMode_Docker: str = "docker"
DeploymentMode_Local: str = "local"

SMR_LEAD_TASK: str = "smr_lead_task"

storage_base_default = os.path.dirname(os.path.realpath(__file__))
smr_port_default = 10000
err_wait_persistent_store = RuntimeError(
    "Persistent store not ready, try again later.")
err_failed_to_lead_execution = ExecutionYieldError(
    "Failed to lead the exectuion.")
err_invalid_request = RuntimeError("Invalid request.")
key_persistent_id = "persistent_id"
enable_storage = True

# Used as the value for an environment variable that was not set.
UNAVAILABLE: str = "N/A"

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s [%(threadName)s (%(thread)d)] ")


# TODO(Ben): Fix this, potentially.
class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s [%(threadName)s (%(thread)d)] "  # type: ignore
    # format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,  # type: ignore
        logging.INFO: grey + format + reset,  # type: ignore
        logging.WARNING: yellow + format + reset,  # type: ignore
        logging.ERROR: red + format + reset,  # type: ignore
        logging.CRITICAL: bold_red + format + reset  # type: ignore
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def tracefunc(frame, event, arg, indent: list[int] = [0]):
    if event == "call":
        indent[0] += 2
        print("-" * indent[0] + "> call function", frame.f_code.co_name, flush=True)
    elif event == "return":
        print("<" + "-" * indent[0], "exit function", frame.f_code.co_name, flush=True)
        indent[0] -= 2
    return tracefunc


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

    persistent_id: Union[str, Unicode] = Unicode(help="""Persistent id for storage""").tag(config=True)

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
        self.source = None
        # Used to create strong references to tasks, such as notifying peer replicas that execution has complete.
        self.background_tasks = set()
        self.next_execute_request_msg_id: Optional[str] = None
        self.old_run_cell = None
        self.daemon_registration_socket = None
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
            "yield_execute",
            "ping_kernel_shell_request",
        ]

        super().__init__(**kwargs)

        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(CustomFormatter())
        self.log.addHandler(ch)

        from ipykernel.debugger import _is_debugpy_available
        if _is_debugpy_available:
            self.log.info("The Debugger IS available/enabled.")

            assert self.debugger is not None
        else:
            self.log.warning("The Debugger is NOT available/enabled.")

        # sys.setprofile(tracefunc)

        # self.log.info("TEST -- INFO")
        # self.log.debug("TEST -- DEBUG")
        # self.log.warning("TEST -- WARN")
        # self.log.error("TEST -- ERROR")   
        # self.log.critical("TEST -- CRITICAL")     

        # self.log.info("Calling Go-level `PrintTestMessage` function now...")
        # PrintTestMessage()
        # self.log.info("Called Go-level `PrintTestMessage` function now...")

        self.received_message_ids: set = set()

        self.run_training_code_mutex: Lock = Lock()
        self.run_training_code: bool = False

        # The time at which we were created (i.e., the time at which the DistributedKernel object was instantiated)
        self.created_at: float = time.time()

        self.execution_ast = None
        self.smr_nodes_map = {}
        self.synclog_stopped = False
        # By default, we do not want to remove the replica from the raft SMR cluster on shutdown.
        # We'll only do this if we're explicitly told to do so.
        self.remove_on_shutdown = False

        # Single node mode
        if not isinstance(self.smr_nodes, list) or len(self.smr_nodes) == 0:
            self.smr_nodes = [f":{self.smr_port}"]
            self.smr_nodes_map = {0: f":{self.smr_port}"}
        else:
            for i, host in enumerate(self.smr_nodes):
                self.smr_nodes_map[i] = f"{host}:{self.smr_port}"

        self.log.info("Kwargs: %s" % str(kwargs))

        connection_file_path = os.environ.get("CONNECTION_FILE_PATH", "")
        config_file_path = os.environ.get("IPYTHON_CONFIG_PATH", "")

        self.deployment_mode: str = os.environ.get("DEPLOYMENT_MODE", "local")
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
        elif self.deployment_mode == DeploymentMode_Docker:
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

        self.spec_cpu: str = os.environ.get("SPEC_CPU", "0")
        self.spec_mem: str = os.environ.get("SPEC_MEM", "0")
        self.spec_gpu: str = os.environ.get("SPEC_GPU", "0")

        self.log.info("CPU: %s, Memory: %s, GPU: %s." %
                      (self.spec_cpu, str(self.spec_mem), self.spec_gpu))

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
            self.register_with_local_daemon(connection_info, session_id)
            self.__init_tcp_server()
        else:
            self.log.warning("Skipping registration step with local daemon.")
            self.__init_tcp_server()
            self.smr_node_id: int = int(os.environ.get("smr_node_id", "1"))
            self.hostname = os.environ.get("hostname", str(socket.gethostname()))
            self.num_replicas: int = 1
            self.persistent_id = os.environ.get("persistent_id", str(uuid.uuid4()))
            self.smr_nodes_map = {1: str(self.hostname) + ":" + str(self.smr_port)}
            self.debug_port: int = int(os.environ.get("debug_port", "31000"))
            # self.start()

        # TODO: Remove this after finish debugging the ACK stuff.
        # self.auth = None

    def __init_tcp_server(self):
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
            "signature_scheme": connection_info["signature_scheme"],
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

        self.should_read_data_from_hdfs = response_dict.get("should_read_data_from_hdfs", False)

        if self.should_read_data_from_hdfs:
            self.log.debug("We SHOULD read data from HDFS.")
        else:
            self.log.debug("We should NOT read data from HDFS.")

        # if "data_directory" in response_dict:
        #     self.log.info("Received path to data directory in HDFS from registration: \"%s\"" %
        #                   response_dict["data_directory"])
        # self.hdfs_data_directory = response_dict["data_directory"]

        if "debug_port" in response_dict:
            self.debug_port: int = int(response_dict["debug_port"])
            self.log.info(f"Assigned debug port to {self.debug_port}")
        else:
            self.log.warning("No \"debug_port\" entry found in response from local daemon.")
            self.debug_port: int = -1

        self.log.info("Received SMR Node ID after registering with local daemon: %d" % self.smr_node_id)
        self.log.info("Replica hostnames: %s" % str(self.smr_nodes_map))

        assert (self.smr_nodes_map[self.smr_node_id] == (
                self.hostname + ":" + str(self.smr_port)))

        self.daemon_registration_socket.close()

    def start(self):
        self.log.info("DistributedKernel is starting. Persistent ID = \"%s\"" % self.persistent_id)

        super().start()

        # debugpy_port:int = self.debug_port + 1000
        # self.log.debug(f"Starting debugpy server on 0.0.0.0:{debugpy_port}")
        # debugpy.listen(("0.0.0.0", debugpy_port))

        # We use 'should_read_data_from_hdfs' as the criteria here, as only replicas that are started after
        # a migration will have should_read_data_from_hdfs equal to True.
        # if self.should_read_data_from_hdfs:
        # self.log.debug("Sleeping for 15 seconds to allow for attaching of a debugger.")
        # time.sleep(15)
        # self.log.debug("Done sleeping.")
        # self.log.debug("Waiting for debugpy client to connect before proceeding.")
        # debugpy.wait_for_client()
        # self.log.debug("Debugpy client has connected. We may now proceed.")
        # debugpy.breakpoint()
        # self.log.debug("Should have broken on the previous line!")

        if self.persistent_id != Undefined and self.persistent_id != "":
            assert isinstance(self.persistent_id, str)

            asyncio.run_coroutine_threadsafe(self.init_persistent_store_on_start(self.persistent_id),
                                             self.control_thread.io_loop.asyncio_loop)
        else:
            self.log.warning(
                "Will NOT be initializing Persistent Store on start, as persistent ID is not yet available.")

    async def init_persistent_store_on_start(self, persistent_id: str):
        self.log.info(f"Initializing Persistent Store on start, as persistent ID is available: \"{persistent_id}\"")
        # Create future to avoid duplicate initialization
        future = asyncio.Future(loop=asyncio.get_running_loop())
        self.store = future
        self.store = await self.init_persistent_store_with_persistent_id(persistent_id)
        # future.set_result(self.gen_simple_response())
        self.log.info(f"Persistent store confirmed: {self.store}")

    async def dispatch_shell(self, msg):
        self.log.debug(f"Received SHELL message: {msg}")
        sys.stderr.flush()
        sys.stdout.flush()
        assert self.session is not None

        idents, msg_without_idents = self.session.feed_identities(msg, copy=False)
        try:
            msg_deserialized = self.session.deserialize(msg_without_idents, content=False, copy=False)
        except Exception as ex:
            self.log.error(f"Received invalid SHELL message: {ex}")  # noqa: G201

            minlen = 5
            msg_list = t.cast(t.List[zmq.Message], msg)
            msg_list_beginning = [bytes(msg.bytes) for msg in msg_list[:minlen]]
            msg_list = t.cast(t.List[bytes], msg_list)
            msg_list = msg_list_beginning + msg_list[minlen:]

            self.log.error(f"Invalid shell message: {msg_list}\n\n")

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
            self.send_ack(self.shell_stream, msg_type, msg_id, idents, message, stream_name="shell")  # Send an ACK.
            return

        # self.log.info(f"Received SHELL message: {str(msg_deserialized)}")
        msg_id: str = msg_deserialized["header"]["msg_id"]
        msg_type: str = msg_deserialized["header"]["msg_type"]
        self.log.debug(f"Received SHELL message {msg_id} of type \"{msg_type}\": {msg_deserialized}")
        sys.stderr.flush()
        sys.stdout.flush()
        self.send_ack(self.shell_stream, msg_type, msg_id, idents, msg_deserialized,
                      stream_name="shell")  # Send an ACK.

        await super().dispatch_shell(msg)

        self.log.debug(f"Finished processing shell message {msg_id} of type \"{msg_type}\"")
        sys.stderr.flush()
        sys.stdout.flush()

    def should_handle(self, stream, msg, idents):
        """Check whether a (shell-channel?) message should be handled"""
        msg_id = msg["header"]["msg_id"]
        msg_type = msg["header"]["msg_type"]
        if msg_id in self.received_message_ids:
            # Is it safe to assume a msg_id will not be resubmitted?
            self.log.warning(f"Received duplicate \"{msg_id}\" message with ID={msg_type}.")

            # Check if the message has a "ForceReprocess" field in its metadata frame with a value of "true".
            # If so, then we'll reprocess it anyway -- as these are likely resubmitted 'yield_execute' or 'execute_request' messages.
            metadata: dict[str, Any] = msg.get("metadata", {})
            if "force_reprocess" in metadata:
                # Presumably this will be True, but if it isn't True, then we definitely shouldn't reprocess the message.
                return metadata["force_reprocess"]

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
        if not self.session:
            self.log.error("We have no session. Cannot process control message...")
            return

        idents, msg = self.session.feed_identities(msg, copy=False)
        try:
            msg = self.session.deserialize(msg, content=True, copy=False)
        except Exception as ex:
            self.log.error(f"Received invalid CONTROL message: {ex}")  # noqa: G201

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
        self.log.debug(f"Received control message {msg_id} of type \"{msg_type}\"")
        sys.stderr.flush()
        sys.stdout.flush()

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
            err_rsp = self.gen_error_response(e)

            assert isinstance(self.store, asyncio.Future)
            self.store.set_result(err_rsp)
            self.store = None
            return err_rsp

    async def init_persistent_store_with_persistent_id(self, persistent_id: str) -> str:
        """Initialize persistent store with persistent id. Return store path."""
        assert isinstance(self.storage_base, str)
        store = os.path.join(self.storage_base, "store", persistent_id)

        self.log.info(
            "Initializing the Persistent Store with Persistent ID: \"%s\"" % persistent_id)
        self.log.info("Full path of Persistent Store: \"%s\"" % store)
        self.log.info("Disabling `outstream` now.")

        sys.stderr.flush()
        sys.stdout.flush()

        # Disable outstream
        self.toggle_outstream(override=True, enable=False)

        self.log.info("Disabled `outstream`.")
        self.log.info("Overriding shell hooks now.")

        # Override shell hooks
        await self.override_shell(store)

        self.log.info("Overrode shell hooks.")

        # Notify the client that the SMR is ready.
        # await self.smr_ready() 

        # TODO(Ben): Should this go before the "smr_ready" send?
        # It probably shouldn't matter -- or if it does, then more synchronization is recuired.
        async with self.persistent_store_cv:
            self.persistent_store_cv.notify_all()

        return store

    async def check_persistent_store(self):
        """Check if persistent store is ready. If initializing, wait. The futrue return True if ready."""
        store = self.store
        if store == None:
            return False
        elif inspect.isawaitable(store):
            response = await store
            return response["status"] == "ok"
        else:
            return True

    def send_ack(self, stream, msg_type: str, msg_id: str, ident, parent, stream_name: str = ""):
        # self.log.debug(f"Sending 'ACK' for {msg_type} message \"{msg_id}\".")
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
        parent_header: dict[str, Any] = extract_header(parent)

        self.log.debug(
            f"execute_request with msg_id=\"{parent_header['msg_id']}\" called within the Distributed Python Kernel.")

        self.next_execute_request_msg_id: str = parent_header["msg_id"]

        self.log.debug("parent: %s", str(parent))
        self.log.debug("ident: %s" % str(ident))

        await super().execute_request(stream, ident, parent)

        # TODO: Need to figure out what value to pass to wait_for_election_to_end here...

        # Schedule task to wait until this current election either fails (due to all replicas yielding)
        # or until the leader finishes executing the user-submitted code.
        task: asyncio.Task = asyncio.create_task(self.synchronizer.wait_for_election_to_end(self.execution_count - 1))

        # To prevent keeping references to finished tasks forever, we make each task remove its own reference from
        # the set after completion.
        task.add_done_callback(self.background_tasks.discard)

        # Wait for the task to end. By not returning here, we ensure that we cannot process any additional
        # "execute_request" messages until all replicas have finished.

        # TODO: Might there still be race conditions where one replica starts processing a future "exectue_request"
        #       message before the others, and possibly starts a new election and proposes something before the
        #       others do?
        await task

    async def ping_kernel_ctrl_request(self, stream, ident, parent):
        """ Respond to a 'ping kernel' Control request. """
        self.log.debug("Ping-Kernel (CONTROL) received.")
        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "ping_reply",
            {"timestamp": time.time()},
            parent,
            metadata={},
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
        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "ping_reply",
            {"timestamp": time.time()},
            parent,
            metadata={},
            ident=ident,
        )
        self.log.debug(f"Sent ping_reply (shell): {str(reply_msg)}")

    async def yield_execute(self, stream, ident, parent):
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

        self.log.info("DistributedKernel is preparing to yield the execution of some code to another replica.\n\n")
        self.log.debug("Parent: %s" % str(parent))
        parent_header: dict[str, Any] = extract_header(parent)
        self._associate_new_top_level_threads_with(parent_header)

        if not self.session:
            return
        try:
            content = parent["content"]
            self.log.debug("Content: %s" % content)
            code = content["code"]
            silent = content.get("silent", False)
        except Exception as ex:
            self.log.error(f"Got bad msg: {parent}. Exception: {ex}")
            return

        stop_on_error = content.get("stop_on_error", True)

        metadata = self.init_metadata(parent)

        # Re-broadcast our input for the benefit of listening clients, and
        # start computing output
        if not silent:
            assert self.execution_count is not None
            self.execution_count += 1
            self._publish_execute_input(code, parent, self.execution_count)

        self.log.debug("yield_execute has been called.")

        try:
            self.toggle_outstream(override=True, enable=False)

            # Ensure persistent store is ready
            if not await self.check_persistent_store():
                raise err_wait_persistent_store

            current_term_number: int = self.synchronizer.execution_count + 1
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

            if self.shell.execution_count == 0:  # type: ignore
                self.log.debug("I will NOT leading this execution.")
                reply_content: dict[str, Any] = self.gen_error_response(err_failed_to_lead_execution)
                # reply_content['yield-reason'] = TODO(Ben): Add this once I figure out how to extract it from the message payloads.
            else:
                self.log.error(
                    f"I've been selected to lead this execution ({self.shell.execution_count}), but I'm supposed to yield!")

                # Notify the client that we will lead the execution (which is bad, in this case, as we were supposed to yield.)
                self.session.send(self.iopub_socket, "smr_lead_after_yield",
                                  {"term": self.synchronizer.execution_count + 1}, ident=self._topic(SMR_LEAD_TASK))
        except Exception as e:
            self.log.error(f"Error while yielding execution for term {current_term_number}: {e}")
            reply_content = self.gen_error_response(e)
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
        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "execute_reply",
            reply_content,
            parent,
            metadata=metadata,
            ident=ident,
        )

        self.log.debug("Sent the following reply after yield_execute: %s" % reply_msg)

        # Block until the leader finishes executing the code, or until we know that all replicas yielded,
        # in which case we can just return, as the code will presumably be resubmitted shortly.
        await self.synchronizer.wait_for_election_to_end(self.synchronizer.execution_count + 1)

        if not silent and error_occurred and stop_on_error:  # reply_msg["content"]["status"] == "error"
            self._abort_queues()

    async def do_execute(self, code: str, silent: bool, store_history: bool = True, user_expressions: dict = None,
                         allow_stdin: bool = False):
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
            await coro

            assert self.execution_count is not None
            self.log.info("Synchronized. End of sync execution: {}".format(self.execution_count - 1))

            # Add task to the set. This creates a strong reference.
            # We don't await this here so that we can go ahead and send the shell response back.
            # We'll notify our peer replicas in time.
            #
            # TODO: Is this okay, or should we await this before returning?
            task: asyncio.Task = asyncio.create_task(self.synchronizer.notify_execution_complete(self.execution_count - 1))

            # To prevent keeping references to finished tasks forever, we make each task remove its own reference from
            # the set after completion.
            task.add_done_callback(self.background_tasks.discard)

            return reply_content
        except ExecutionYieldError as eye:
            self.log.info("Execution yielded: {}".format(eye))

            return self.gen_error_response(eye)
        except Exception as e:
            self.log.error("Execution error: {}...".format(e))

            return self.gen_error_response(e)

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
                return self.gen_error_response(ex), False

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

        # We don't want to remove this node from the SMR raft cluster when we shutdown the kernel,
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
            self.synclog.closeHdfsClient()

            return self.gen_error_response(e), False

        # Step 2: copy the data directory to HDFS
        try:
            waldir_path: str = await self.synclog.write_data_dir_to_hdfs()
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
            self.synclog.closeHdfsClient()

            return self.gen_error_response(e), False

        try:
            self.synclog.closeHdfsClient()
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

        sent_message = self.session.send(stream, "prepare_to_migrate_reply", content, parent, ident=ident)

        self.log.debug("Sent 'prepare_to_migrate_reply message: %s" % str(sent_message))

    async def do_add_replica(self, id, addr) -> tuple[dict, bool]:
        """Add a replica to the SMR cluster"""
        if not await self.check_persistent_store():
            return self.gen_error_response(err_wait_persistent_store), False

        self.log.info("Adding replica %d at addr %s now.", id, addr)

        # We didn't check if synclog is ready
        try:
            await self.synclog.add_node(id, "http://{}".format(addr))
            self.log.info(
                "Replica {} at {} has joined the SMR cluster.".format(id, addr))
            return {'status': 'ok'}, True
        except Exception as e:
            self.log.error("A replica fails to join: {}...".format(e))
            return self.gen_error_response(e), False

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
            err_content: dict = self.gen_error_response(err_invalid_request)
            self.session.send(stream, "add_replica_reply", err_content, parent, ident=ident)
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

        self.session.send(stream, "add_replica_reply", content, parent, ident=ident)  # type: ignore

    async def do_update_replica(self, id, addr) -> tuple:
        """
        Update a replica to have a new address
        
        We also reset certain SMR-related state for this replica, as it will have restarted. 
        For example, its attempt number(s) for the current term will be starting over.
        """
        if not await self.check_persistent_store():
            return self.gen_error_response(err_wait_persistent_store), False

        self.log.info("Updating replica %d with new addr %s now.", id, addr)

        # We didn't check if synclog is ready
        try:
            await self.synclog.update_node(id, "http://{}".format(addr))
            self.log.info(
                "Replica {} at {} has been updated.".format(id, addr))
            return {'status': 'ok'}, True
        except Exception as e:
            self.log.error("Failed to update replica: {}...".format(e))
            return self.gen_error_response(e), False

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
            return self.gen_error_response(err_invalid_request)

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

        self.session.send(stream, "update_replica_reply",
                          content, parent, ident=ident)  # type: ignore

    def report_error(self, error_title: str = "", error_message: str = ""):
        """
        Send an error report/message to our local daemon via our IOPub socket.
        """
        self.log.debug(f"Sending 'error_report' message for error \"{error_title}\" now...")
        err_msg = self.session.send(self.iopub_socket, "error_report",
                                    {"error": error_title, "message": error_message, "kernel_id": self.kernel_id},
                                    ident=self._topic("error_report"))
        self.log.debug(f"Sent 'error_report' message: {str(err_msg)}")

    def gen_simple_response(self, execution_count=0):
        return {'status': 'ok',
                # The base class increments the execution count
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
                }

    def gen_error_response(self, err):
        return {'status': 'error',
                'ename': str(type(err).__name__),
                'evalue': str(err),
                'traceback': [],
                }

    async def override_shell(self, store_path):
        """Override IPython Core"""
        self.old_run_cell = self.shell.run_cell  # type: ignore
        self.shell.run_cell = self.run_cell  # type: ignore
        self.shell.transform_ast = self.transform_ast  # type: ignore

        # Get synclog for synchronization.
        sync_log = await self.get_synclog(store_path)

        self.log.info("Creating Synchronizer now.")

        # Start the synchronizer.
        # Starting can be non-blocking, call synchronizer.ready() later to confirm the actual execution_count.
        self.synchronizer = Synchronizer(
            sync_log, module=self.shell.user_module, opts=CHECKPOINT_AUTO)  # type: ignore

        self.log.info("Created Synchronizer. Starting Synchronizer now.")

        # if self.control_thread:
        #     control_loop = self.control_thread.io_loop
        # else:
        #     control_loop = self.io_loop
        # asyncio.run_coroutine_threadsafe(self.synchronizer.start(), control_loop.asyncio_loop)
        self.synchronizer.start()

        sys.stderr.flush()
        sys.stdout.flush()

        # We do this here (and not earlier, such as right after creating the RaftLog), as the RaftLog needs to be started before we attempt to catch-up.
        # The catch-up process involves appending a new value and waiting until it gets committed. This cannot be done until the RaftLog has started.
        # And the RaftLog is started by the Synchronizer, within Synchronizer::start.
        if self.synclog.needs_to_catch_up:
            self.log.debug("RaftLog needs to propose new value.")
            await self.synclog.catchup_with_peers()

        # Send the 'smr_ready' message AFTER we've caught-up with our peers (if that's something that we needed to do).
        await self.smr_ready()

        self.log.info("Started Synchronizer.")

    async def smr_ready(self) -> None:
        """
        Inform our local daemon that we've joined our SMR cluster.

        If we've been started following a migration, then this should only be called once we're fully caught-up.
        """
        # Notify the client that the SMR is ready.
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

        addrs = []
        ids = []
        for node_id, addr in self.smr_nodes_map.items():
            addrs.append("http://" + addr)
            ids.append(node_id)

        # Implement dynamic later
        # addrs = map(lambda x: "http://{}".format(x), self.smr_nodes)
        self.log.debug(
            "Passing the following addresses to RaftLog: %s" % str(addrs))
        store = ""
        if enable_storage:
            store = store_path

        sys.stderr.flush()
        sys.stdout.flush()

        self.log.debug("Creating RaftLog now.")
        try:
            self.synclog = RaftLog(self.smr_node_id,
                                   base_path=store,
                                   num_replicas=self.num_replicas,
                                   hdfs_hostname=self.hdfs_namenode_hostname,
                                   should_read_data_from_hdfs=self.should_read_data_from_hdfs,
                                   # data_directory = self.hdfs_data_directory,
                                   peer_addrs=addrs,
                                   peer_ids=ids,
                                   join=self.smr_join,
                                   debug_port=self.debug_port)
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
            self.log.error(
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
