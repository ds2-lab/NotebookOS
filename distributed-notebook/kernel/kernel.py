import asyncio
import os
import pickle
import sys
import ast
import logging

from ipykernel.ipkernel import IPythonKernel
from ..sync.synchronizer import Synchronizer

base = os.path.dirname(os.path.realpath(__file__))
err_wait_persistent_store = RuntimeError("Persistent store not ready, try again later.")
key_persistent_id = "persistent_id"

logging.basicConfig(level=logging.INFO)

class DistributedKernel(IPythonKernel):
    implementation = 'Distributed Python 3'
    implementation_version = '0.1'
    language = 'no-op'
    language_version = '0.1'
    language_info = {
        'name': 'Any text',
        'mimetype': 'text/plain',
        'file_extension': '.txt',
    }
    banner = "Distributed kernel - as useful as a parrot"
    synchronizer = None
    synced = 0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)

        # Delay persistent store initialization to kernel.js
        self.store = None
        

    async def init_persistent_store(self, code):
        if self.store == None:
            execution_count = self.shell.execution_count
            await asyncio.ensure_future(super().do_execute(code, True, store_history=False))

            self.store = os.path.join(base, "store", self.shell.user_ns[key_persistent_id])
            self.log.info("persistent store confirmed: " + self.store)

            # Reset execution_count, override_shell may update again.
            self.shell.execution_count = execution_count
            # Override shell hooks
            self.override_shell()
        
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

        reply_routing = super().do_execute(
            code, silent, store_history=store_history,
            user_expressions=user_expressions, allow_stdin=allow_stdin)

        reply_content = await asyncio.ensure_future(reply_routing)
        
        self.sync(self.synchronizer)
        self.log.info("synced tree:\n{}".format(ast.dump(self.synchronizer.tree)))

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

    def override_shell(self):
        """Override IPython Core"""
        self.old_run_cell = self.shell.run_cell
        self.shell.run_cell = self.run_cell
        self.shell.transform_ast = self.transform_ast
        self.synchronizer = self.sync(self.synchronizer)

    def run_cell(self, raw_cell, store_history=False, silent=False, shell_futures=True):
        self.log.info("in run_cell: ")
        self.source = raw_cell
        result = self.old_run_cell(raw_cell, store_history=store_history, silent=silent, shell_futures=shell_futures)
        self.source = None
        return result

    def transform_ast(self, node):
        self.log.info("in transform_ast")
        return self.synchronizer.sync(node, self.source)

    def sync(self, synchronizer=None):
        if synchronizer is None:
            synchronizer = Synchronizer()
             # load
            if not os.path.exists(os.path.join(self.store, "tree.dat")):
                return synchronizer

            with open(os.path.join(self.store, "tree.dat"), "rb") as file:
                dumped = pickle.load(file)
                self.synced = synchronizer.restore(dumped)
                self.log.info("loaded history:\n{}".format(ast.dump(synchronizer.tree)))
                self.shell.execution_count = self.synced

            # Redeclare modules, classes, and functions.
            compiled = compile(synchronizer.tree, "restore", "exec")
            exec(compiled, self.shell.user_global_ns, self.shell.user_ns)
            
            with open(os.path.join(self.store, "data.dat"), "rb") as file:
                # overwrite self.shell.user_global_ns
                old_main_modules = sys.modules["__main__"]
                sys.modules["__main__"] = self.shell.user_module
                data = pickle.load(file)
                sys.modules["__main__"] = old_main_modules
                for key in data.keys():
                    self.shell.user_ns[key] = data[key]
                return synchronizer

        elif self.execution_count != self.synced:
            dumping = synchronizer.dump(self.execution_count)
            ns = {}
            for key in synchronizer.globals.keys():
                ns[key] = self.shell.user_global_ns[key]

            if not os.path.exists(self.store):
                os.mkdir(self.store, 0o755)

            with open(os.path.join(self.store, "tree.dat"), "wb") as file:
                pickle.dump(dumping, file)

            with open(os.path.join(self.store, "data.dat"), "wb") as file:
                old_main_modules = sys.modules["__main__"]
                sys.modules["__main__"] = self.shell.user_module
                pickle.dump(ns, file)
                sys.modules["__main__"] = old_main_modules
            self.synced = self.execution_count
            return synchronizer
