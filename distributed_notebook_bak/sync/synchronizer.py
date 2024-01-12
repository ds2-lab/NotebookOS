import asyncio
import sys
import types
import logging
import time
from typing import Any, Optional, Union

from .log import Checkpointer, SyncLog, SyncValue, KEY_SYNC_END
from .ast import SyncAST
from .object import SyncObject, SyncObjectWrapper, SyncObjectMeta
from .referer import SyncReferer
from .errors import print_trace, SyncError

KEY_SYNC_AST = "_ast_"
CHECKPOINT_AUTO = 1
CHECKPOINT_ON_CHANGE = 2
MIN_CHECKPOINT_LOGS = 10

class SyncModule(object):
  """A dummy module used for Synchronizer for customizing __dict__"""
  __spec__ = None

class Synchronizer:
  _ast: SyncAST
  _module: types.ModuleType
  _async_loop: asyncio.AbstractEventLoop

  def __init__(self, synclog: SyncLog, module:Optional[types.ModuleType]=None, ns = None, opts = 0):
    if module is None and ns is not None:
      self._module = SyncModule() # type: ignore
      ns.setdefault("__name__", "__main__")
      self._module.__dict__ = ns
    elif module is None:
      self._module = types.ModuleType("__main__", doc="Automatically created module for python environment")
    else:
      self._module = module

    # Set callbacks for synclog
    synclog.set_should_checkpoint_callback(self.should_checkpoint_callback)
    synclog.set_checkpoint_callback(self.checkpoint_callback)
    
    self._log = logging.getLogger(__class__.__name__)
    self._log.setLevel(logging.DEBUG)
    self._async_loop = asyncio.get_running_loop()
    self._tags = {}
    self._ast = SyncAST()
    self._referer = SyncReferer()
    self._opts = opts
    self._syncing = False  # Avoid checkpoint in the middle of syncing.

    self._synclog = synclog


  def start(self):
    self._async_loop = asyncio.get_running_loop()
    self._synclog.start(self.change_handler)

  def close(self):
    pass

  # def _change_handler(self, buff, id):
  #   # Ignore local proposal.
  #   print("got changed: {}".format(id))
  #   if id != "":
  #     return
    
  #   reader = io.BytesIO(bytes(Slice_byte(handle=buff)))
  #   syncval = pickle.load(reader)
  #   self.change_handler(syncval)

  @property
  def module(self):
    return self._module

  @property
  def global_ns(self):
    return self._module.__dict__

  @property
  def execution_count(self) -> int:
    return self._ast.execution_count

  def change_handler(self, val: SyncValue):
    """Change handler"""
    ## TODO: Buffer changes of one execution and apply changes atomically
    if not val.end:
      if not self._syncing:
        self._log.debug("enter execution syncing...")
        self._syncing = True
    elif val.key == KEY_SYNC_END:
      self._syncing = False
      self._log.debug("exit execution syncing")
      return

    try:
      self._log.debug("Updating: {}, ended: {}".format(val.key, val.end))
      existed: Optional[SyncObject] = None
      if val.key == KEY_SYNC_AST:
        existed = self._ast
      elif val.key in self._tags:
        existed = self._tags[val.key]

      if existed == None: 
        if isinstance(val.val, SyncObject):
          existed = val.val
        else:
          existed = SyncObjectWrapper(self._referer) # Initialize an empty object wrapper.

      # Switch context
      old_main_modules = sys.modules["__main__"]
      sys.modules["__main__"] = self._module

      # print("restoring {}...".format(val.key))
      diff = existed.update(val)
      # print("{}:{}".format(val.key, type(diff)))

      sys.modules["__main__"] = old_main_modules
      # End of switch context

      if val.key == KEY_SYNC_AST:
        assert isinstance(existed, SyncAST)
        self._ast = existed
        # Redeclare modules, classes, and functions.
        compiled = compile(diff, "sync", "exec")
        exec(compiled, self.global_ns, self.global_ns)
      else:
        assert isinstance(existed, SyncObjectWrapper)
        assert val.key != None
        self._tags[val.key] = existed
        self.global_ns[val.key] = existed.object

      if val.end:
        self._syncing = False
        self._log.debug("exit execution syncing")
    except Exception as e:
      print_trace()

  async def ready(self, execution_count) -> int:
    """Wait the ready of the synchronization and propose to lead a execution.
       Returns the execution count that granted to lead or 0 if denied.
       Note that if the execution_count is 0, the execution is guaranteed to be
       granted, which may cause duplication execution."""
    if execution_count < 0:
      return 0
    
    try:
      # Propose to lead specified term. 
      # Term 0 tries to lead the next term whatever and will always success.
      if await self._synclog.lead(execution_count):
        # Synchronized, execution_count was updated to last execution.
        self._async_loop = asyncio.get_running_loop() # Update async_loop.
        return self._synclog.term
    except SyncError as se:
      self._log.warning("SyncError: {}".format(se))
    except Exception as e:
      print_trace()
    
    # Failed to lead the term
    return 0

  async def sync(self, execution_ast, source: Optional[str]=None, checkpointer: Optional[Checkpointer]=None):
    synclog = self._synclog
    checkpointing = checkpointer is not None
    if checkpointing:
      synclog = checkpointer

    try:
      sync_ast: Optional[SyncValue]
      if checkpointing:
        sync_ast = self._ast.dump(meta=source)
      else:
        sync_ast = self._ast.diff(execution_ast, meta=source)
        assert sync_ast != None
      # execution_count updated.
      # self._referer.module_id = self._ast.execution_count # TODO: Verify this.

      # self._log.debug("Syncing execution, ast: {}...".format(sync_ast))
      keys = self._ast.globals
      meta = SyncObjectMeta(batch=(str(sync_ast.term) if not checkpointing else "{}c".format(sync_ast.term)))
      # TODO: Recalculate the number of expected synchronizations within the execution.
      expected = len(keys) # globals + the ast
      synced = 0

      self._syncing = True
      sync_ast.term = self._ast.execution_count
      sync_ast.key = KEY_SYNC_AST
      if expected == 0:
        sync_ast.end = expected == 0
        # Because should_checkpoint_callback will be called during final append call, 
        # set the end of _syncing before the final append call.
        self._syncing = False

      await synclog.append(sync_ast)
      for key in keys:
        synced = synced + 1
        await self.sync_key(synclog, key, self.global_ns[key], end_execution=synced==expected, checkpointing=checkpointing, meta=meta)
      
      if checkpointing:
        checkpointer.close()
    except SyncError as se:
      self._log.warning("SyncError: {}".format(se))
    except Exception as e:
      print_trace()

  async def sync_key(self, synclog, key, val, end_execution=False, checkpointing=False, meta=None):
    existed = None
    if key in self._tags:
      existed = self._tags[key]
    else:
      assert not checkpointing
      # TODO: Add support to SyncObject factory
      existed = SyncObjectWrapper(self._referer)
      self._tags[key] = existed

    # self._log.debug("Syncing {}...".format(key))

    # Switch context
    old_main_modules = sys.modules["__main__"]
    sys.modules["__main__"] = self._module

    if checkpointing:
      sync_val = existed.dump(meta=meta)
    else:
      # On checkpointing, the syncobject must have been available in tags.
      # Get start time of the execution.
      # start_time = time.time()
      sync_val = existed.diff(val, meta=meta)
      # Print time elapsed.
      # self._log.debug("Time elapsed in diff: {}".format(time.time() - start_time))

    sys.modules["__main__"] = old_main_modules
    # End of switch context

    # Because should_checkpoint_callback will be called during final append call, 
    # set the end of _syncing before the final append call.
    if end_execution:
      self._syncing = False

    if sync_val is not None:
      sync_val.term = self._ast.execution_count
      sync_val.key = key
      sync_val.end = end_execution
      await synclog.append(sync_val)
    elif end_execution:
      # Synthecize end
      await synclog.append(SyncValue(None, None, term=self._ast.execution_count, end=True, key=KEY_SYNC_END))

  def should_checkpoint_callback(self, synclog: SyncLog) -> bool:
    cp = False
    if self.execution_count < 2 or self._syncing or synclog.num_changes < MIN_CHECKPOINT_LOGS:
      pass
    else:
      cp = (((self._opts & CHECKPOINT_AUTO) > 0 and synclog.num_changes >= len(self._tags.keys())) or
            ((self._opts & CHECKPOINT_ON_CHANGE) > 0 and synclog.num_changes > 0))
    # self._log.debug("in should_checkpoint_callback: {}".format(cp))
    return cp

  def checkpoint_callback(self, checkpointer: Checkpointer) -> None:
    self._log.debug("checkpointing...")
    checkpointer.lead(self._ast.execution_count)
    # await self.sync(None, source="checkpoint", checkpointer=checkpointer)
    # checkpointer.close()
    asyncio.run_coroutine_threadsafe(
      self.sync(None, source="checkpoint", checkpointer=checkpointer),
      self._async_loop)