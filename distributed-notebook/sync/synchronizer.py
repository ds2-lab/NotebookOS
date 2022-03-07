import io
import pickle
import ast
import sys
import types

from .log import SyncLog, SyncValue
from .ast import SyncAST
from .object import SyncObject, SyncObjectWrapper, SyncObjectMeta
from .referer import SyncReferer

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
    
    self.tags = {}
    self.ast = SyncAST()
    self.referer = SyncReferer()
    self.synclog = synclog
    self.synclog.start(self.change_handler)
    self.opts = opts

  @property
  def global_ns(self):
    return self.module.__dict__

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

  def sync(self, execution_count, execution_ast, source=None, checkpoint=False):
    self.referer.module_id = execution_count
    synclog = self.synclog
    if checkpoint:
      synclog = self.synclog.checkpoint()

    synclog.lead(execution_count)

    if checkpoint:
      sync_val = self.ast.dump(meta=source)
    else:
      self.ast.execution_count = execution_count
      sync_val = self.ast.diff(execution_ast, meta=source)
    sync_val.term = self.ast.execution_count
    sync_val.key = KEY_SYNC_AST
    self.synclog.append(sync_val)
      
    keys = self.ast.globals.keys()
    syncing = 0
    meta = SyncObjectMeta(batch=(execution_count if not checkpoint else "{}c".format(execution_count)))
    for key in keys:
      syncing = syncing + 1
      self.sync_key(synclog, key, self.global_ns[key], end_execution=syncing==len(keys), checkpoint=checkpoint, meta=meta)

    if self.opts & CHECKPOINT_AUTO and synclog.num_changes > len(self.tags.__dict__.keys()):
      self.sync(execution_count, execution_ast, source=source, checkpoint=True)
    elif self.opts & CHECKPOINT_ON_CHANGE and synclog.num_changes > len(self.tags.__dict__.keys()):
      self.sync(execution_count, execution_ast, source=source, checkpoint=True)

  def sync_key(self, synclog, key, val, end_execution=False, checkpoint=False, meta=None):
    if isinstance(val, ast.AST):
      self.start_sync(key, val)
      return

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
    if checkpoint:
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
      synclog.append(sync_val)