import asyncio
import os
import logging
import random

from ipykernel.ipkernel import IPythonKernel
from ..sync import Synchronizer, RaftLog, CHECKPOINT_AUTO

base = os.path.dirname(os.path.realpath(__file__))
err_wait_persistent_store = RuntimeError("Persistent store not ready, try again later.")
err_failed_to_lead_execution = RuntimeError("Failed to lead the exectuion.")
key_persistent_id = "persistent_id"

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
        
        # Synchronize
        # self.log.info("synced tree:\n{}".format(ast.dump(self.synchronizer._ast.tree)))
        await self.synchronizer.sync(self.execution_ast, self.source)

        return reply_content

    # def do_complete(self, code, cursor_pos):
    #     ret = super(DistributedKernel, self).do_complete(code, cursor_pos)

    #     self.sync(self.synchronizer)
    #     self.log.info("synced tree:\n{}".format(ast.dump(self.synchronizer.tree)))

    #     return ret

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
        await self.synchronizer.start()

    async def get_synclog(self):
        return RaftLog(self.store, 1, ["http://127.0.0.1:19800"])

    def run_cell(self, raw_cell, store_history=False, silent=False, shell_futures=True):
        self.source = raw_cell
        result = self.old_run_cell(raw_cell, store_history=store_history, silent=silent, shell_futures=shell_futures)
        return result

    def transform_ast(self, node):
        self.execution_ast = node
        return node
