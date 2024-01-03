import asyncio
import inspect
import os
import logging
import sys

from typing import Union, Optional
from traitlets import List, Integer, Unicode, Bool, Undefined
from ipykernel.ipkernel import IPythonKernel
from .iostream import OutStream
from ..sync import Synchronizer, RaftLog, CHECKPOINT_AUTO

class ExecutionYieldError(Exception):
    """Exception raised when execution is yielded."""
    def __init__(self, message):
        super().__init__(message)

storage_base_default = os.path.dirname(os.path.realpath(__file__))
smr_port_default = 10000
err_wait_persistent_store = RuntimeError("Persistent store not ready, try again later.")
err_failed_to_lead_execution = ExecutionYieldError("Failed to lead the exectuion.")
err_invalid_request = RuntimeError("Invalid request.")
key_persistent_id = "persistent_id"
enable_storage = True

logging.basicConfig(level=logging.INFO)

class DistributedKernel(IPythonKernel):
    # Configurable properties
    storage_base: Union[str, Unicode] = Unicode(storage_base_default, # type: ignore
        help="""Base directory for storage"""
    ).tag(config=True)

    smr_port = Integer(smr_port_default, # type: ignore
        help="""Port for SMR"""
    ).tag(config=True)

    smr_node_id = Integer(1, # type: ignore
        help="""Node id for SMR"""
    ).tag(config=True)

    smr_nodes = List([], 
        help="""Nodes in the SMR cluster"""
    ).tag(config=True)

    smr_join = Bool(False, # type: ignore
        help="""Join the SMR cluster"""
    ).tag(config=True)

    persistent_id: Union[str, Unicode] = Unicode(
        help="""Persistent id for storage"""
    ).tag(config=True)

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
    synchronizer: Synchronizer

    def __init__(self, **kwargs):
        self.control_msg_types = [
            *self.control_msg_types,
            "add_replica_request",
        ]

        super().__init__(**kwargs)

        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)

        # Single node mode
        if not isinstance(self.smr_nodes, list) or len(self.smr_nodes) == 0:
            self.smr_nodes = [f":{self.smr_port}"]

    def start(self):
        super().start()

        if self.persistent_id != Undefined and self.persistent_id != "":
            assert isinstance(self.persistent_id, str)

            asyncio.run_coroutine_threadsafe(
                self.init_persistent_store_on_start(self.persistent_id), self.control_thread.io_loop.asyncio_loop # type: ignore
            )

    # # async def wait_and_close(self):
    # #     await asyncio.sleep(30)
    # #     self.synchronizer.close()

    async def init_persistent_store_on_start(self, persistent_id: str):
        future = asyncio.Future(loop = asyncio.get_running_loop())
        self.store = future
        self.store = await self.init_persistent_store_with_persistent_id(persistent_id)
        future.set_result(self.gen_simple_response())
        self.log.info("persistent store confirmed: " + self.store)

    async def init_persistent_store(self, code):
        if await self.check_persistent_store():
            return self.gen_simple_response()
        
        # By executing code, we can get persistent id later.
        # The execution_count should not be counted and will reset later.
        execution_count = self.shell.execution_count # type: ignore

        # continue to initialize store
        try:
            # Create future to avoid duplicate initialization
            future = asyncio.Future(loop = asyncio.get_running_loop())
            self.store = future
            
            # Execute code to get persistent id
            await asyncio.ensure_future(super().do_execute(code, True, store_history=False))
            # Reset execution_count
            self.shell.execution_count = execution_count # type: ignore
            self.persistent_id = self.shell.user_ns[key_persistent_id] # type: ignore

            # Initialize persistent store
            self.store = await self.init_persistent_store_with_persistent_id(self.persistent_id)

            # Resolve future
            rsp = self.gen_simple_response()
            future.set_result(rsp)
            self.log.info("persistent store confirmed: " + self.store)

            # Notify the client that the SMR is ready
            self.session.send(self.iopub_socket, "smr_ready", {"persistent_id": self.persistent_id}, ident=self._topic("smr_ready")) # type: ignore

            return rsp
        except Exception as e:
            self.shell.execution_count = execution_count # type: ignore
            err_rsp = self.gen_error_response(e)

            assert isinstance(self.store, asyncio.Future)
            self.store.set_result(err_rsp)
            self.store = None
            return err_rsp
        
    async def init_persistent_store_with_persistent_id(self, persistent_id: str) -> str:
        """Initialize persistent store with persistent id. Return store path."""
        assert isinstance(self.storage_base, str)
        store = os.path.join(self.storage_base, "store", persistent_id)
        
        self.log.info("Initializing the Persistent Store with Persistent ID: \"%s\"" % persistent_id)
        self.log.info("Full path of Persistent Store: \"%s\"" % store)
        self.log.info("Disabling `outstream` now.")

        # Disable outstream
        self.toggle_outstream(override=True, enable=False)

        self.log.info("Disabled `outstream`.")
        self.log.info("Overriding shell hooks now.")

        # Override shell hooks
        await self.override_shell(store)
        
        self.log.info("Overrode shell hooks.")

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

    async def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        # Special code to initialize persistent store
        if code[:len(key_persistent_id)] == key_persistent_id:
            return await asyncio.ensure_future(self.init_persistent_store(code))

        try:
            self.toggle_outstream(override=True, enable=False)

            # Ensure persistent store is ready
            if not await self.check_persistent_store():
                raise err_wait_persistent_store

            # Pass 0 to lead the next execution based on history, which should be passed only if a duplicated execution is acceptable.
            # Pass value > 0 to lead a specific execution.
            # In either case, the execution will wait until states are synchornized.
            self.shell.execution_count = await self.synchronizer.ready(self.synchronizer.execution_count + 1) # type: ignore
            if self.shell.execution_count == 0: # type: ignore
                raise err_failed_to_lead_execution
            
            # Notify the client that we will lead the execution.
            self.session.send(self.iopub_socket, "smr_lead_task", {"gpu": False}, ident=self._topic("smr_lead_task")) # type: ignore

            # Execute code
            reply_routing = super().do_execute(
                code, silent, store_history=store_history,
                user_expressions=user_expressions, allow_stdin=allow_stdin)

            # Wait for the settlement of variables.
            reply_content = await reply_routing
            
            self.log.info("Returning the following message for do_execute: \"%s\"" % str(reply_content))

            # Disable stdout and stderr forwarding.
            self.toggle_outstream(override=True, enable=False)
            
            # Synchronize
            coro = self.synchronizer.sync(self.execution_ast, self.source)
            self.execution_ast = None
            self.source = None
            await coro

            self.log.info("End of sync execution {}".format(self.execution_count - 1))
            return reply_content
        except ExecutionYieldError as eye:
            self.log.info("Execution yielded: {}".format(eye))
            return self.gen_error_response(eye)
        except Exception as e:
            self.log.error("Execution error: {}...".format(e))
            return self.gen_error_response(e)
    
    async def do_shutdown(self, restart):
        if self.synchronizer:
            self.synchronizer.close()

        if self.synclog:
            await self.synclog.remove_node(self.smr_node_id)
            self.synclog.close()

        return super().do_shutdown(restart)
    
    async def do_add_replica(self, id, addr):
        """Add a replica to the SMR cluster"""
        if not await self.check_persistent_store():
            return self.gen_error_response(err_wait_persistent_store)

        # We didn't check if synclog is ready
        try:
            await self.synclog.add_node(id, "http://{}".format(addr))
            self.log.error("A replica({}) is notified to join: {}".format(id, addr))
            return {'status': 'ok'}
        except Exception as e:
            self.log.error("A replica fails to join: {}...".format(e))
            return self.gen_error_response(e)
    
    # customized control message handlers
    async def add_replica_request(self, stream, ident, parent):
        """Add a replica to the SMR cluster"""
        params = parent['content']
        if 'id' not in params or 'addr' not in params:
            return self.gen_error_response(err_invalid_request)

        content = self.do_add_replica(params['id'], params['addr'])
        if inspect.isawaitable(content):
            content = await content
        
        self.session.send(stream, "add_replica_reply", content, parent, ident=ident) # type: ignore

    def gen_simple_response(self, execution_count = 0):
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
        self.old_run_cell = self.shell.run_cell # type: ignore
        self.shell.run_cell = self.run_cell # type: ignore
        self.shell.transform_ast = self.transform_ast # type: ignore
        
        # Get synclog for synchronization.
        synclog = await self.get_synclog(store_path)

        # Start the synchronizer. 
        # Starting can be non-blocking, call synchronizer.ready() later to confirm the actual execution_count.
        self.synchronizer = Synchronizer(synclog, module = self.shell.user_module, opts=CHECKPOINT_AUTO) # type: ignore

        # if self.control_thread:
        #     control_loop = self.control_thread.io_loop
        # else:
        #     control_loop = self.io_loop
        # asyncio.run_coroutine_threadsafe(self.synchronizer.start(), control_loop.asyncio_loop)
        self.synchronizer.start()

    async def get_synclog(self, store_path):
        assert isinstance(self.smr_nodes, list)
        assert isinstance(self.smr_node_id, int)
        assert isinstance(self.smr_join, bool)

        self.log.info("Confirmed node {}".format(self.smr_nodes[self.smr_node_id-1]))
        
        # Implement dynamic later
        addrs = map(lambda x: "http://{}".format(x), self.smr_nodes)
        store = ""
        if enable_storage:
            store = store_path
        self.synclog = RaftLog(store, self.smr_node_id, addrs, join=self.smr_join)
        return self.synclog

    def run_cell(self, raw_cell, store_history=False, silent=False, shell_futures=True, cell_id=None):
        self.source = raw_cell
        self.toggle_outstream(override=True, enable=True)
        result = self.old_run_cell(raw_cell, store_history=store_history, silent=silent, shell_futures=shell_futures, cell_id=cell_id)
        return result

    def transform_ast(self, node):
        self.execution_ast = node
        return node

    def toggle_outstream(self, override=False, enable=True):
        # Is sys.stdout has attribute 'disable'?
        if not hasattr(sys.stdout, 'disable'):
            self.log.error("sys.stdout didn't initialized with kernel.OutStream.")
            return
        
        if override:
            sys.stdout.disable = not enable # type: ignore
            sys.stderr.disable = not enable # type: ignore
        else:
            sys.stdout.disable = not sys.stdout.disable # type: ignore
            sys.stderr.disable = not sys.stderr.disable # type: ignore

