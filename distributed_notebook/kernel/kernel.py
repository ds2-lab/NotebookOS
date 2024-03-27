from __future__ import annotations

import asyncio
import inspect
import os
import logging
import json
import sys
import socket
import time

from typing import Union, Optional
from traitlets import List, Integer, Unicode, Bool, Undefined
from ipykernel.ipkernel import IPythonKernel
from ipykernel import jsonutil
from ..sync import Synchronizer, RaftLog, CHECKPOINT_AUTO
from .util import extract_header


class ExecutionYieldError(Exception):
    """Exception raised when execution is yielded."""

    def __init__(self, message):
        super().__init__(message)


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

    persistent_id: Union[str, Unicode] = Unicode(
        help="""Persistent id for storage"""
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

    hdfs_namenode_hostname: Union[str, Unicode] = Unicode(
        help="""Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this."""
    ).tag(config=True)

    kernel_id: Union[str, Unicode] = Unicode(
        help="""The ID of the kernel."""
    ).tag(config=False)

    data_directory: Union[str, Unicode] = Unicode(
        help="""The etcd-raft WAL/data directory. This will always be equal to the empty string unless we're created during a migration operation."""
    ).tag(config=False)

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
        print(f' Kwargs: {kwargs}')

        self.control_msg_types = [
            *self.control_msg_types,
            "add_replica_request",
            "update_replica_request",
            "prepare_to_migrate_request",
        ]

        self.msg_types = [
            *self.msg_types,
            "yield_execute",
        ]

        super().__init__(**kwargs)

        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)

        self.log.info("TEST -- INFO")
        self.log.debug("TEST -- DEBUG")
        self.log.warn("TEST -- WARN")
        self.log.error("TEST -- ERROR")

        self.execution_ast = None
        self.smr_nodes_map = {}
        self.synclog_stopped = False
        # By default, we do want to remove the replica from the raft smr cluster on shutdown.
        self.remove_on_shutdown = True

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
        session_id = os.environ.get("SESSION_ID", default=UNAVAILABLE)
        self.kernel_id = os.environ.get("KERNEL_ID", default=UNAVAILABLE)
        self.pod_name = os.environ.get("POD_NAME", default=UNAVAILABLE)
        self.node_name = os.environ.get("NODE_NAME", default=UNAVAILABLE)

        self.log.info("Connection file path: \"%s\"" % connection_file_path)
        self.log.info("IPython config file path: \"%s\"" % config_file_path)
        self.log.info("Session ID: \"%s\"" % session_id)
        self.log.info("Kernel ID: \"%s\"" % self.kernel_id)
        self.log.info("Pod name: \"%s\"" % self.pod_name)
        self.log.info("HDFS NameNode hostname: \"%s\"" %
                      self.hdfs_namenode_hostname)

        self.spec_cpu = os.environ.get("SPEC_CPU", "0")
        self.spec_mem = os.environ.get("SPEC_MEM", "0")
        self.spec_gpu = os.environ.get("SPEC_GPU", "0")

        self.log.info("CPU: %s, Memory: %s, GPU: %s." %
                      (self.spec_cpu, self.spec_mem, self.spec_gpu))

        self.persistent_store_cv = asyncio.Condition()
        # Initialize to this. If we're part of a migration operation, then it will be set when we register with the local daemon.
        self.hdfs_data_directory = ""

        connection_info = None
        try:
            if len(connection_file_path) > 0:
                with open(connection_file_path, 'r') as connection_file:
                    connection_info = json.load(connection_file)
        except Exception as ex:
            self.log.error(
                "Failed to obtain connection info from file \"%s\"" % connection_file_path)
            self.log.error("Error: %s" % str(ex))

        self.log.info("Connection info: %s" % str(connection_info))
        # self.log.info("IPython config info: %s" % str(config_info))

        # TODO(Ben): Connect to LocalDaemon.
        self.register_with_local_daemon(
            connection_info, session_id)  # config_info

    # config_info:dict,
    def register_with_local_daemon(self, connection_info: dict, session_id: str):
        self.log.info("Registering with local daemon now.")

        local_daemon_service_name = os.environ.get(
            "LOCAL_DAEMON_SERVICE_NAME", default="local-daemon-network")
        server_port = os.environ.get("LOCAL_DAEMON_SERVICE_PORT", default=8075)
        try:
            server_port = int(server_port)
        except ValueError:
            server_port = 8075

        self.log.info("Local Daemon network address: \"%s:%d\"" %
                      (local_daemon_service_name, server_port))

        self.daemon_registration_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.daemon_registration_socket.connect(
                (local_daemon_service_name, server_port))
        except Exception as ex:
            self.log.error("Failed to connect to LocalDaemon at %s:%d" %
                           (local_daemon_service_name, server_port))
            self.log.error("Reason: %s" % str(ex))
            return

        registration_payload = {
            "op": "register",
            "signature_scheme": connection_info["signature_scheme"],
            "key": connection_info["key"],
            "replicaId": self.smr_node_id,  # config_info["smr_node_id"],
            # len(config_info["smr_nodes"]),
            "numReplicas": len(self.smr_nodes_map),
            "join": self.smr_join,  # config_info["smr_join"],
            "podName": self.pod_name,
            "nodeName": self.node_name,
            "resourceSpec": {
                "cpu": self.spec_cpu,
                "mem": self.spec_mem,
                "gpu": self.spec_gpu,
            },
            "kernel": {
                # , config_info["smr_nodes"][0][7:-7], # Chop off the kernel- prefix and :<port> suffix.
                "id": self.kernel_id,
                # config_info["smr_nodes"][0][7:-7], # Chop off the kernel- prefix and :<port> suffix.
                "session": session_id,
                "signature_scheme": connection_info["signature_scheme"],
                "key": connection_info["key"],
            }
        }

        self.log.info("Sending registration payload to local daemon: %s" %
                      str(registration_payload))

        bytes_sent = self.daemon_registration_socket.send(
            json.dumps(registration_payload).encode())

        self.log.info("Sent %d byte(s) to local daemon." % bytes_sent)

        response = self.daemon_registration_socket.recv(1024)

        if len(response) == 0:
            self.log.error(
                "Received empty (i.e., 0 bytes in length) response from local daemon during registration...")
            exit(1)

        self.log.info("Received %d byte(s) in response from LocalDaemon: %s", len(
            response), str(response))

        response_dict = json.loads(response)
        self.smr_node_id = response_dict["smr_node_id"]
        self.hostname = response_dict["hostname"]

        # self.smr_nodes = [hostname + ":" + str(self.smr_port) for hostname in response_dict["replicas"]]

        # Note: we expect the keys to be integers; however, they will have been converted to strings.
        # See https://stackoverflow.com/a/1451857 for details.
        # We convert the string keys, which are node IDs, back to integers.
        #
        # We also append ":<SMR_PORT>" to each address before storing it in the map.
        self.smr_nodes_map = {int(node_id_str): (node_addr + ":" + str(self.smr_port))
                              for node_id_str, node_addr in response_dict["replicas"].items()}

        # If we're part of a migration operation, then we should receive both a persistent ID AND an HDFS Data Directory.
        # If we're not part of a migration operation, then we'll JUST receive the persistent ID.
        if "persistent_id" in response_dict:
            self.log.info("Received persistent ID from registration: \"%s\"" %
                          response_dict["persistent_id"])
            self.persistent_id = response_dict["persistent_id"]

        if "data_directory" in response_dict:
            self.log.info("Received path to data directory in HDFS from registration: \"%s\"" %
                          response_dict["data_directory"])
            self.hdfs_data_directory = response_dict["data_directory"]

        self.log.info(
            "Received SMR Node ID after registering with local daemon: %d" % self.smr_node_id)
        self.log.info("Replica hostnames: %s" % str(self.smr_nodes_map))

        assert (self.smr_nodes_map[self.smr_node_id] == (
            self.hostname + ":" + str(self.smr_port)))

        self.daemon_registration_socket.close()

    def start(self):
        super().start()

        self.log.info(
            "DistributedKernel is starting. Persistent ID = \"%s\"" % self.persistent_id)

        if self.persistent_id != Undefined and self.persistent_id != "":
            assert isinstance(self.persistent_id, str)

            asyncio.run_coroutine_threadsafe(
                self.init_persistent_store_on_start(
                    self.persistent_id), self.control_thread.io_loop.asyncio_loop  # type: ignore
            )

    async def init_persistent_store_on_start(self, persistent_id: str):
        self.log.info(
            "Initializing Persistent Store on start, as persistent ID is available: \"%s\"" % persistent_id)
        future = asyncio.Future(loop=asyncio.get_running_loop())
        self.store = future
        self.store = await self.init_persistent_store_with_persistent_id(persistent_id)
        future.set_result(self.gen_simple_response())
        self.log.info("Persistent store confirmed: " + self.store)

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

        # Disable outstream
        self.toggle_outstream(override=True, enable=False)

        self.log.info("Disabled `outstream`.")
        self.log.info("Overriding shell hooks now.")

        # Override shell hooks
        await self.override_shell(store)

        self.log.info("Overrode shell hooks.")

        # Notify the client that the SMR is ready.
        self.session.send(self.iopub_socket, "smr_ready", {
                          "persistent_id": self.persistent_id}, ident=self._topic("smr_ready"))  # type: ignore

        self.log.info("Notified local daemon that SMR is ready.")

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

    async def execute_request(self, stream, ident, parent):
        """Override for receiving specific instructions about which replica should execute some code."""
        self.log.debug(
            "execute_request called within the Distributed Python Kernel.")
        await super().execute_request(stream, ident, parent)

    async def yield_execute(self, stream, ident, parent):
        """
        Similar to the do_execute method, but this method ALWAYS proposes "YIELD" instead of "LEAD".
        Kernel replicas are directed to call this instead of do_execute by their local daemon if there are resource constraints
        preventing them from being able to execute the user's code (e.g., insufficient GPUs available).

        Args:
            code (str): The code to be executed.
            silent (bool): Whether to display output.
            store_history (bool, optional): Whether to record this code in history and increase the execution count. If silent is True, this is implicitly False. Defaults to True.
            user_expressions (dict, optional): Mapping of names to expressions to evaluate after the code has run. You can ignore this if you need to.. Defaults to None.
            allow_stdin (bool, optional): Whether the frontend can provide input on request (e.g. for Python's raw_input()). Defaults to False.

        Raises:
            err_wait_persistent_store: If the persistent data store is not available yet.

        Returns:
            dict: A dict containing the fields described in the "Execution results" Jupyter documentation available here:
            https://jupyter-client.readthedocs.io/en/latest/messaging.html#execution-results
        """
        self.log.info(
            "DistributedKernel is preparing to yield the execution of some code to another replica.")
        parent_header = extract_header(parent)
        self._associate_new_top_level_threads_with(parent_header)

        if not self.session:
            return
        try:
            content = parent["content"]
            code = content["code"]
            silent = content.get("silent", False)
            # store_history = content.get("store_history", not silent)
            # user_expressions = content.get("user_expressions", {})
            # allow_stdin = content.get("allow_stdin", False)
            # cell_meta = parent.get("metadata", {})
            # cell_id = cell_meta.get("cellId")
        except Exception:
            self.log.error("Got bad msg: ")
            self.log.error("%s", parent)
            return

        stop_on_error = content.get("stop_on_error", True)

        metadata = self.init_metadata(parent)

        # Re-broadcast our input for the benefit of listening clients, and
        # start computing output
        if not silent:
            self.execution_count += 1
            self._publish_execute_input(code, parent, self.execution_count)

        self.log.debug("yield_execute has been called.")

        try:
            self.toggle_outstream(override=True, enable=False)

            # Ensure persistent store is ready
            if not await self.check_persistent_store():
                raise err_wait_persistent_store

            self.log.info("Calling synchronizer.ready(%d) now with YIELD proposal." % (
                self.synchronizer.execution_count + 1))

            # Pass 'True' for the 'lead' parameter to propose LEAD.
            # Pass 'False' for the 'lead' parameter to propose YIELD.
            #
            # Pass 0 to lead the next execution based on history, which should be passed only if a duplicated execution is acceptable.
            # Pass value > 0 to lead a specific execution.
            # In either case, the execution will wait until states are synchornized.
            # type: ignore
            self.shell.execution_count = await self.synchronizer.ready(self.synchronizer.execution_count + 1, False)

            self.log.info("Completed call to synchronizer.ready(%d) with YIELD proposal. shell.execution_count: %d" % (
                self.synchronizer.execution_count + 1, self.shell.execution_count))

            if self.shell.execution_count == 0:  # type: ignore
                self.log.debug("I will NOT leading this execution.")
                raise err_failed_to_lead_execution

            self.log.error(
                "I've been selected to lead this execution (%d), but I'm supposed to yield!" % self.shell.execution_count)

            # Notify the client that we will lead the execution.
            self.session.send(self.iopub_socket, "smr_lead_after_yield", {
                              "term": self.synchronizer.execution_count+1}, ident=self._topic("smr_lead_task"))
        except ExecutionYieldError as eye:
            self.log.info("Execution yielded: {}".format(eye))
            return self.gen_error_response(eye)
        except Exception as e:
            self.log.error("Execution error: {}...".format(e))
            return self.gen_error_response(e)

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

        reply_msg: dict[str, t.Any] = self.session.send(  # type:ignore[assignment]
            stream,
            "execute_reply",
            reply_content,
            parent,
            metadata=metadata,
            ident=ident,
        )

        self.log.debug("%s", reply_msg)

        if not silent and reply_msg["content"]["status"] == "error" and stop_on_error:
            self._abort_queues()

    async def do_execute(self, code: str, silent: bool, store_history: bool = True, user_expressions: dict = None, allow_stdin: bool = False):
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
        self.log.info(
            "DistributedKernel is preparing to execute some code: %s", code)

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

            self.log.info("Calling synchronizer.ready(%d) now with LEAD proposal." % (
                self.synchronizer.execution_count + 1))

            # Pass 'True' for the 'lead' parameter to propose LEAD.
            # Pass 'False' for the 'lead' parameter to propose YIELD.
            #
            # Pass 0 to lead the next execution based on history, which should be passed only if a duplicated execution is acceptable.
            # Pass value > 0 to lead a specific execution.
            # In either case, the execution will wait until states are synchornized.
            # type: ignore
            self.shell.execution_count = await self.synchronizer.ready(self.synchronizer.execution_count + 1, True)

            self.log.info("Completed call to synchronizer.ready(%d) with LEAD proposal. shell.execution_count: %d" % (
                self.synchronizer.execution_count + 1, self.shell.execution_count))

            if self.shell.execution_count == 0:  # type: ignore
                self.log.debug("I will NOT leading this execution.")
                raise err_failed_to_lead_execution

            self.log.debug("I WILL lead this execution (%d)." %
                           self.shell.execution_count)
            # Notify the client that we will lead the execution.
            self.session.send(self.iopub_socket, "smr_lead_task", {
                              "gpu": False}, ident=self._topic("smr_lead_task"))  # type: ignore

            self.log.debug("Executing the following code now: %s" % code)

            # Execute code
            reply_routing = super().do_execute(
                code, silent, store_history=store_history,
                user_expressions=user_expressions, allow_stdin=allow_stdin)

            # Wait for the settlement of variables.
            reply_content = await reply_routing

            self.log.info(
                "Returning the following message for do_execute: \"%s\"" % str(reply_content))

            # Disable stdout and stderr forwarding.
            self.toggle_outstream(override=True, enable=False)

            if self.execution_ast is None:
                self.log.warn(
                    "Execution AST is None. Synchronization will likely fail...")
            else:
                self.log.debug("Synchronizing now. Execution AST is NOT None.")

            # Synchronize
            coro = self.synchronizer.sync(self.execution_ast, self.source)
            self.execution_ast = None
            self.source = None
            await coro

            self.log.info("Synchronized. End of sync execution {}".format(
                self.execution_count - 1))
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

        # Give time for the "smr_node_removed" message to be sent.
        # time.sleep(2)
        return super().do_shutdown(restart)

    async def prepare_to_migrate(self):
        self.log.info("Preparing for migration of replica %d of kernel %s.",
                      self.smr_node_id, self.kernel_id)

        # We don't want to remove this node from the SMR raft cluster when we shutdown the kernel,
        # as we're migrating the replica and want to reuse the ID when the raft process resumes
        # on another node.
        self.remove_on_shutdown = False

        if not self.synclog:
            return

        # self.log.info("Removing node %d (that's me) from the SMR cluster.", self.smr_node_id)
        # try:
        #     await self.synclog.remove_node(self.smr_node_id)
        #     self.synclog_removed = True
        #     self.log.info("Successfully removed node %d (that's me) from the SMR cluster.", self.smr_node_id)
        # except Exception as e:
        #     self.log.error("Failed to remove replica %d of kernel %s.", self.smr_node_id, self.kernel_id)
        #     return self.gen_error_response(e), False

        #
        # Reference: https://etcd.io/docs/v2.3/admin_guide/#member-migration
        #

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
            return self.gen_error_response(e), False

        try:
            data_dir_path = await self.synclog.write_data_dir_to_hdfs()
            self.log.info(
                "Wrote etcd-Raft data directory to HDFS. Path: \"%s\"" % data_dir_path)
            return {'status': 'ok', "data_directory": data_dir_path, "id": self.smr_node_id, "kernel_id": self.kernel_id}, True
        except Exception as e:
            self.log.error("Failed to write the data directory of replica %d of kernel %s to HDFS: %s",
                           self.smr_node_id, self.kernel_id, str(e))
            return self.gen_error_response(e), False

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

        self.session.send(stream, "prepare_to_migrate_reply",
                          content, parent, ident=ident)

    async def do_add_replica(self, id, addr) -> tuple:
        """Add a replica to the SMR cluster"""
        if not await self.check_persistent_store():
            return self.gen_error_response(err_wait_persistent_store)

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
            return self.gen_error_response(err_invalid_request)

        val = self.do_add_replica(params['id'], params['addr'])
        if inspect.isawaitable(val):
            content, success = await val
        else:
            content, success = val

        if success:
            self.log.debug("Notifying session that SMR node was added.")
            self.session.send(self.iopub_socket, "smr_node_added", {"success": True, "persistent_id": self.persistent_id, "id": params[
                              'id'], "addr": params['addr'], "kernel_id": self.kernel_id}, ident=self._topic("smr_node_added"))  # type: ignore
        else:
            self.log.debug("Notifying session that SMR node addition failed.")
            self.session.send(self.iopub_socket, "smr_node_added", {"success": False, "persistent_id": self.persistent_id, "id": params[
                              'id'], "addr": params['addr'], "kernel_id": self.kernel_id}, ident=self._topic("smr_node_added"))  # type: ignore

        self.session.send(stream, "add_replica_reply", content,
                          parent, ident=ident)  # type: ignore

    async def do_update_replica(self, id, addr) -> tuple:
        """Update a replica to have a new address"""
        if not await self.check_persistent_store():
            return self.gen_error_response(err_wait_persistent_store)

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
            self.session.send(self.iopub_socket, "smr_node_updated", {"success": True, "persistent_id": self.persistent_id, "id": params[
                              'id'], "addr": params['addr'], "kernel_id": self.kernel_id}, ident=self._topic("smr_node_updated"))  # type: ignore
        else:
            self.log.debug("Notifying session that SMR node update failed.")
            self.session.send(self.iopub_socket, "smr_node_updated", {"success": False, "persistent_id": self.persistent_id, "id": params[
                              'id'], "addr": params['addr'], "kernel_id": self.kernel_id}, ident=self._topic("smr_node_updated"))  # type: ignore

        self.session.send(stream, "update_replica_reply",
                          content, parent, ident=ident)  # type: ignore

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

        self.log.info("Started Synchronizer.")

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

        self.log.debug("Creating RaftLog now.")
        try:
            self.synclog = RaftLog(store, self.smr_node_id, self.hdfs_namenode_hostname,
                                   self.hdfs_data_directory, addrs, ids, join=self.smr_join)
        except Exception as ex:
            self.log.error("Error while creating RaftLog: %s" % str(ex))
            exit(1)

        self.log.debug("Successfully created RaftLog.")
        return self.synclog

    def run_cell(self, raw_cell, store_history=False, silent=False, shell_futures=True, cell_id=None):
        self.log.debug("Running cell: %s" % str(raw_cell))
        self.source = raw_cell
        self.toggle_outstream(override=True, enable=True)
        result = self.old_run_cell(raw_cell, store_history=store_history,
                                   silent=silent, shell_futures=shell_futures, cell_id=cell_id)
        self.log.debug("Ran cell %s: %s" % (str(raw_cell), str(result)))
        return result

    def transform_ast(self, node):
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
