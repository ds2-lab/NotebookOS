import io
import pickle
import asyncio
import ast
import sys
import types

from .log import Checkpointer, SyncLog, SyncValue
from .ast import SyncAST
from .object import SyncObject, SyncObjectWrapper, SyncObjectMeta
from .referer import SyncReferer
from ..smr.go import Slice_byte

KEY_SYNC_AST = "_ast_"
CHECKPOINT_AUTO = 1
CHECKPOINT_ON_CHANGE = 2

class SyncModule(object):
  """A dummy module used for Synchronizer for customizing __dict__"""
  __spec__ = None

class Synchronizer:
  def __init__(self, synclog: SyncLog, module=None, ns = None, opts = 0):
    if module is None and ns is not None:
      self.module = SyncModule()
      ns.setdefault("__name__", "__main__")
      self.module.__dict__ = ns
    elif module is None:
      self.module = types.ModuleType("__main__", doc="Automatically created module for python environment")
    else:
      self.module = module

    # Set callbacks for synclog
    synclog.set_should_checkpoint_callback(self.should_checkpoint_callback)
    synclog.set_checkpoint_callback(self.checkpoint_callback)
    
    self.tags = {}
    self.ast = SyncAST()
    self.referer = SyncReferer()
    self.synclog = synclog
    self.synclog.start(self.change_handler)
    self.opts = opts

  # def _change_handler(self, buff, id):
  #   # Ignore local proposal.
  #   print("got changed: {}".format(id))
  #   if id != "":
  #     return
    
  #   reader = io.BytesIO(bytes(Slice_byte(handle=buff)))
  #   syncval = pickle.load(reader)
  #   self.change_handler(syncval)

  @property
  def global_ns(self):
    return self.module.__dict__

  @property
  def execution_count(self):
    return self.ast.execution_count

  def change_handler(self, val: SyncValue):
    """Change handler"""
    ## TODO: Buffer changes of one execution and apply changes atomically
    existed = None
    if val.key == KEY_SYNC_AST:
      existed = self.ast
    elif val.key in self.tags:
      existed = self.tags[val.key]

    if existed == None: 
      if isinstance(val.val, SyncObject):
        existed = val.val
      else:
        existed = SyncObjectWrapper(self.referer)

    # Switch context
    old_main_modules = sys.modules["__main__"]
    sys.modules["__main__"] = self.module
    
    # print("restoring {}...".format(val.key))
    diff = existed.update(val)
    # print("{}:{}".format(val.key, type(diff)))

    sys.modules["__main__"] = old_main_modules
    # End of switch context

    if val.key == KEY_SYNC_AST:
      self.ast = existed
      # Redeclare modules, classes, and functions.
      compiled = compile(diff, "sync", "exec")
      exec(compiled, self.global_ns, self.global_ns)
    else:
      self.tags[val.key] = existed
      self.global_ns[val.key] = existed.object

  async def ready(self, execution_count) -> int:
    while True:
      # Validate execution_count
      term = execution_count
      if term == 1:
        # execution_count can be uninitialized, reset to 0
        term = 0
      
      if await self.synclog.lead(term):
        return execution_count
      
      # Update execution count
      execution_count = self.execution_count

  async def sync(self, execution_count, execution_ast, source=None, checkpointer=None):
    self.referer.module_id = execution_count
    synclog = self.synclog
    checkpointing = checkpointer is not None
    if checkpointing:
      synclog = checkpointer

    # Moved to ready
    # synclog.lead(execution_count) 

    if checkpointing:
      sync_val = self.ast.dump(meta=source)
    else:
      self.ast.execution_count = execution_count
      sync_val = self.ast.diff(execution_ast, meta=source)
    sync_val.term = self.ast.execution_count
    sync_val.key = KEY_SYNC_AST
    await synclog.append(sync_val)
      
    keys = self.ast.globals.keys()
    syncing = 0
    meta = SyncObjectMeta(batch=(execution_count if not checkpointing else "{}c".format(execution_count)))
    for key in keys:
      syncing = syncing + 1
      await self.sync_key(synclog, key, self.global_ns[key], end_execution=syncing==len(keys), checkpointing=checkpointing, meta=meta)

  async def sync_key(self, synclog, key, val, end_execution=False, checkpointing=False, meta=None):
    existed = None
    if key in self.tags:
      existed = self.tags[key]
    else:
      # TODO: Add support to SyncObject factory
      existed = SyncObjectWrapper(self.referer)

    # Switch context
    old_main_modules = sys.modules["__main__"]
    sys.modules["__main__"] = self.module

    # print("syncing {}...".format(key))
    if checkpointing:
      sync_val = existed.dump(meta=meta)
    else:
      # On checkpointing, the syncobject must have been available in tags.
      sync_val = existed.diff(val, meta=meta)

    sys.modules["__main__"] = old_main_modules
    # End of switch context

    if sync_val is not None:
      sync_val.term = self.ast.execution_count
      sync_val.key = key
      sync_val.end = end_execution
      await synclog.append(sync_val)
    elif end_execution:
      # Synthecize end
      await synclog.append(SyncValue(None, None, term=self.ast.execution_count, end=True))

  def should_checkpoint_callback(self, synclog: SyncLog):
    print("in should_checkpoint_callback")
    return ((self.opts & CHECKPOINT_AUTO and synclog.num_changes >= len(self.tags.__dict__.keys())) or
      (self.opts & CHECKPOINT_ON_CHANGE and synclog.num_changes > 0))

  def checkpoint_callback(self, checkpointer: Checkpointer):
    checkpointer.lead(self.ast.execution_count)
    self.sync(self.ast.execution_count, self.ast.tree, source="checkpoint", checkpointer=checkpointer)