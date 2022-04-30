import asyncio
import os
import logging
import random
import sys

from ipykernel.ipkernel import IPythonKernel
from .iostream import OutStream
from ..sync import Synchronizer, RaftLog, CHECKPOINT_AUTO

base = os.path.dirname(os.path.realpath(__file__))
err_wait_persistent_store = RuntimeError("Persistent store not ready, try again later.")
err_failed_to_lead_execution = RuntimeError("Failed to lead the exectuion.")
key_persistent_id = "persistent_id"
key_replica_id = "replica_id"
base_port = 10000

logging.basicConfig(level=logging.INFO)

class DistributedKernel(IPythonKernel):
    implementation = 'Distributed Python 3'
    implementation_version = '0.2'
    language = 'no-op'
    language_version = '0.2'
    language_info = {
        'name': 'Any text',
        'mimetype': 'text/plain',
        'file_extension': '.txt',
    }
    banner = "Distributed kernel - as useful as a parrot"
    synchronizer = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)

        # Delay persistent store initialization to kernel.js
        self.store = None

    # def start(self):
    #     super().start()

    #     self.shell.user_ns[key_persistent_id] = "6d1e4d69-3a21-4a1e-86f4-5790fe8b52ae"
    #     self.store = os.path.join(base, "store", self.shell.user_ns[key_persistent_id]) # Default perisistent id for testing.
    #     self.log.info("persistent store assumption confirmed: " + self.store)

    #     asyncio.run_coroutine_threadsafe(self.override_shell(), self.io_loop.asyncio_loop)

    # #     asyncio.run_coroutine_threadsafe(self.wait_and_close(), self.io_loop.asyncio_loop)

    # # async def wait_and_close(self):
    # #     await asyncio.sleep(30)
    # #     self.synchronizer.close()

    async def init_persistent_store(self, code):
        if self.store == None:
            # By executing code, we can get persistent id later.
            # The execution_count should not be counted and will reset later.
            execution_count = self.shell.execution_count
            await asyncio.ensure_future(super().do_execute(code, True, store_history=False))

            self.store = os.path.join(base, "store", self.shell.user_ns[key_persistent_id])
            self.log.info("persistent store confirmed: " + self.store)

            # Reset execution_count, override_shell may update again.
            self.shell.execution_count = execution_count
            # Override shell hooks
            await self.override_shell()
        
        return self.gen_simple_response()

    def check_persistent_store(self):
        if self.store == None:
            raise err_wait_persistent_store

    async def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        # Special code to initialize persistent store
        if code[:len(key_persistent_id)] == key_persistent_id:
            return await asyncio.ensure_future(self.init_persistent_store(code))

        self.toggle_outstream(override=True, enable=False)

        # Ensure persistent store is ready
        self.check_persistent_store()

        # Pass 0 to lead the next execution based on history, which should be 
        # passed only if a duplicated execution is acceptable.
        # Pass value > 0 to lead a specific execution.
        # In either case, the execution will wait until states are synchornized.
        self.shell.execution_count = await self.synchronizer.ready(0)
        if self.shell.execution_count == 0:
            raise err_failed_to_lead_execution

        reply_routing = super().do_execute(
            code, silent, store_history=store_history,
            user_expressions=user_expressions, allow_stdin=allow_stdin)

        # Wait for the settlement of variables.
        reply_content = await reply_routing

        # Disable stdout and stderr forwarding.
        self.toggle_outstream(override=True, enable=False)
        
        # Synchronize
        # self.log.info("synced tree:\n{}".format(ast.dump(self.execution_ast)))
        coro = self.synchronizer.sync(self.execution_ast, self.source)
        self.execution_ast = None
        self.source = None
        await coro

        self.log.info("End of sync execution {}".format(self.execution_count - 1))
        return reply_content
    
    def do_shutdown(self, restart):
        if self.synchronizer:
            self.synchronizer.close()

        return super().do_shutdown(restart)

    def gen_simple_response(self, execution_count = 0):
        return {'status': 'ok',
                # The base class increments the execution count
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
               }

    async def override_shell(self):
        """Override IPython Core"""
        self.old_run_cell = self.shell.run_cell
        self.shell.run_cell = self.run_cell
        self.shell.transform_ast = self.transform_ast
        
        # Get synclog for synchronization.
        synclog = await self.get_synclog()

        # Start the synchronizer. Starting can be non-blocking, call 
        # synchronizer.ready() later to confirm the actual execution_count.
        self.synchronizer = Synchronizer(synclog, module = self.shell.user_module, opts=CHECKPOINT_AUTO)

        # if self.control_thread:
        #     control_loop = self.control_thread.io_loop
        # else:
        #     control_loop = self.io_loop
        # asyncio.run_coroutine_threadsafe(self.synchronizer.start(), control_loop.asyncio_loop)
        self.synchronizer.start()

    async def get_synclog(self):
        port = base_port
        node_id = 1
        random.seed(self.shell.user_ns[key_persistent_id])
        port = port + random.randint(0, 10000)
        if key_replica_id in self.shell.user_ns:
            node_id = int(self.shell.user_ns[key_replica_id])

        self.log.info("Confirmed node {} at port {}".format(node_id, port + node_id - 1))
        
        # Implement dynamic later
        addrs = ["http://127.0.0.1:{}".format(port), "http://127.0.0.1:{}".format(port + 1), "http://127.0.0.1:{}".format(port + 2)]
        published = [3, 3, 3]
        return RaftLog(self.store, node_id, addrs[:published[node_id-1]])

    def run_cell(self, raw_cell, store_history=False, silent=False, shell_futures=True):
        self.source = raw_cell
        self.toggle_outstream(override=True, enable=True)
        result = self.old_run_cell(raw_cell, store_history=store_history, silent=silent, shell_futures=shell_futures)
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
            sys.stdout.disable = not enable
        else:
            sys.stdout.disable = not sys.stdout.disable

