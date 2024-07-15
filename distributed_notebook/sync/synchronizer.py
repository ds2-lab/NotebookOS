import asyncio
import sys
import types
import logging
import traceback
from typing import Any, Optional, Union

from .log import Checkpointer, SyncLog, SynchronizedValue, KEY_SYNC_END
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

  def __init__(self, sync_log: SyncLog, module:Optional[types.ModuleType]=None, ns = None, opts = 0):
    if module is None and ns is not None:
      self._module = SyncModule() # type: ignore
      ns.setdefault("__name__", "__main__")
      self._module.__dict__ = ns
    elif module is None:
      self._module = types.ModuleType("__main__", doc="Automatically created module for python environment")
    else:
      self._module = module

    # Set callbacks for synclog
    sync_log.set_should_checkpoint_callback(self.should_checkpoint_callback)
    sync_log.set_checkpoint_callback(self.checkpoint_callback)
    
    self._log = logging.getLogger(__class__.__name__)
    self._log.setLevel(logging.DEBUG)

    self._log.debug("Finished setting callbacks for Synclog (within Synchronizer).")

    self._async_loop = asyncio.get_running_loop()
    self._tags = {}
    self._ast = SyncAST()
    self._referer = SyncReferer()
    self._opts = opts
    self._syncing = False  # Avoid checkpoint in the middle of syncing.

    self._synclog: SyncLog = sync_log


  def start(self):
    self._async_loop = asyncio.get_running_loop()
    self._synclog.start(self.change_handler)

  def close(self):
    pass

  @property
  def module(self):
    return self._module

  @property
  def global_ns(self):
    return self._module.__dict__

  @property
  def execution_count(self) -> int:
    return self._ast.execution_count

  def change_handler(self, val: SynchronizedValue, restoring:bool = False):
    """Change handler"""
    ## TODO: Buffer changes of one execution and apply changes atomically
    if not val.should_end_execution:
      if not self._syncing:
        self._log.debug(">> enter execution syncing...")
        self._syncing = True
    elif val.key == KEY_SYNC_END:
      self._syncing = False
      self._log.debug("<< exit execution syncing [1]")
      return

    try:
      # self._log.debug("Updating: \"{}\", ended: {}".format(val.key, val.should_end_execution))
      self._log.debug(f"Updating: {val}")
      existed: Optional[SyncObject] = None
      if val.key == KEY_SYNC_AST:
        existed = self._ast
      elif val.key in self._tags:
        existed = self._tags[val.key]

      if existed == None: 
        if isinstance(val.data, SyncObject):
          existed = val.data
        else:
          existed = SyncObjectWrapper(self._referer) # Initialize an empty object wrapper.

      # Switch context
      old_main_modules = sys.modules["__main__"]
      sys.modules["__main__"] = self._module

      self._log.debug("restoring {}...".format(val.key))
      diff = existed.update(val)
      self._log.debug("{}:{}".format(val.key, type(diff)))

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

      if val.should_end_execution:
        self._syncing = False
        self._log.debug("<< exit execution syncing [2]")
    except Exception as e:
      # print_trace(limit = 10)
      self._log.error("Exception encountered in change handler for synchronizer: %s" % str(e))
      tb: list[str] = traceback.format_exception(e)
      for frame in tb:
        self._log.error(frame)
      
  async def propose_lead(self, execution_count: int) -> int:
    """Propose to lead the next execution.
    
    Wait the ready of the synchronization and propose to lead a execution.
    Returns the execution count that granted to lead or 0 if denied.
    Note that if the execution_count is 0, the execution is guaranteed to be
    granted, which may cause duplication execution.
    """
    self._log.debug("Synchronizer is proposing to lead term %d" % execution_count)
    try:
      # Propose to lead specified term. 
      # Term 0 tries to lead the next term whatever and will always success.
      if await self._synclog.try_lead_execution(execution_count):
        self._log.debug("We won the election to lead term %d" % execution_count)
        # Synchronized, execution_count was updated to last execution.
        self._async_loop = asyncio.get_running_loop() # Update async_loop.
        return self._synclog.term
    except SyncError as se:
      self._log.warning("SyncError: {}".format(se))
      # print_trace(limit = 10)
      stack:list[str] = traceback.format_exception(se)
      for frame in stack:
        self._log.error(frame)
    except Exception as e:
      self._log.error("Exception encountered while proposing LEAD: %s" % str(e))
      # print_trace(limit = 10)
      stack:list[str] = traceback.format_exception(e)
      for frame in stack:
        self._log.error(frame)
      raise e 
    
    self._log.debug("We lost the election to lead term %d" % execution_count)
    # Failed to lead the term
    return 0 
  
  async def propose_yield(self, execution_count: int) -> int:
    """Propose to yield the next execution to another replica.
    
    Wait the ready of the synchronization and propose to lead a execution.
    Returns the execution count that granted to lead or 0 if denied.
    Note that if the execution_count is 0, the execution is guaranteed to be
    granted, which may cause duplication execution.
    """
    self._log.debug("Synchronizer is proposing to yield term %d" % execution_count)
    try:
      if await self._synclog.try_yield_execution(execution_count):
        self._log.error("synclog.yield_exection returned true despite the fact that we're yielding...")
        raise ValueError("synclog.yield_exection returned true despite the fact that we're yielding")
    except SyncError as se:
      self._log.warning("SyncError: {}".format(se))
      # print_trace(limit = 10)
      stack:list[str] = traceback.format_exception(se)
      for frame in stack:
        self._log.error(frame)
    except Exception as e:
      self._log.error("Exception encountered while proposing YIELD: %s" % str(e))
      # print_trace(limit = 10)
      stack:list[str] = traceback.format_exception(e)
      for frame in stack:
        self._log.error(frame)
      raise e
    
    self._log.debug("Successfully yielded the execution to another replica for term %d" % execution_count)
    # Failed to lead the term, which is what we want to happen since we're YIELDING.
    return 0 
  
  async def ready(self, execution_count:int, lead:bool) -> int:
    """
    Wait the ready of the synchronization and propose to lead a execution.
    Returns the execution count that granted to lead or 0 if denied.
    Note that if the execution_count is 0, the execution is guaranteed to be
    granted, which may cause duplication execution.
    
    Pass 'True' for the 'lead' parameter to propose LEAD.
    Pass 'False' for the 'lead' parameter to propose YIELD.
    """
    if execution_count < 0:
      return 0
    
    if lead:
      self._log.debug("Synchronizer::Ready(LEAD): Proposing to lead now.")
      res = await self.propose_lead(execution_count)
      self._log.debug("Synchronizer::Ready(LEAD): Done with proposal protocol for lead. Result: %d" % res)
      return res 
    else:
      self._log.debug("Synchronizer::Ready(YIELD): Proposing to yield now.")
      res = await self.propose_yield(execution_count)
      self._log.debug("Synchronizer::Ready(YIELD): Done with proposal protocol for yield. Result: %d" % res)
      return res 

  async def sync(self, execution_ast, source: Optional[str]=None, checkpointer: Optional[Checkpointer]=None):
    """
    Note: `execution_ast` may be None if the user's code had a syntax error. 
    TODO(Ben): See what happens if there are other errors, such as dividing by zero or array out-of-bounds.
    """
    self._log.debug("Synchronizing execution AST: %s" % str(execution_ast))
    synclog = self._synclog
    checkpointing = checkpointer is not None
    if checkpointing:
      synclog = checkpointer

    try:
      sync_ast: Optional[SynchronizedValue]
      if checkpointing:
        sync_ast = self._ast.dump(meta=source)
      else:
        sync_ast = self._ast.diff(execution_ast, meta=source)
        assert sync_ast != None
      # execution_count updated.
      # self._referer.module_id = self._ast.execution_count # TODO: Verify this.

      self._log.debug("Syncing execution, ast: {}...".format(sync_ast))
      keys = self._ast.globals
      meta = SyncObjectMeta(batch=(str(sync_ast.election_term) if not checkpointing else "{}c".format(sync_ast.election_term)))
      # TODO: Recalculate the number of expected synchronizations within the execution.
      expected = len(keys) # globals + the ast
      synced = 0

      self._syncing = True
      self._log.debug("Setting sync_ast.term to term of AST: %d" % (self._ast.execution_count))
      sync_ast.set_election_term(self._ast.execution_count)
      sync_ast.set_key(KEY_SYNC_AST)
      if expected == 0:
        sync_ast.set_should_end_execution(expected == 0)
        # Because should_checkpoint_callback will be called during final append call, 
        # set the end of _syncing before the final append call.
        self._syncing = False
      
      self._log.debug("Appending value \"%s\" now." % str(sync_ast))
      await synclog.append(sync_ast)
      self._log.debug("Successfully appended value \"%s\"." % str(sync_ast))
      for key in keys:
        synced = synced + 1
        self._log.debug("Syncing key \"%s\" now." % key)
        await self.sync_key(synclog, key, self.global_ns[key], end_execution=synced==expected, checkpointing=checkpointing, meta=meta)
        self._log.debug("Successfully synchronized key \"%s\"." % key)
      
      if checkpointing:
        checkpointer.close()
    except SyncError as se:
      tb = traceback.format_exc()
      self._log.error("SyncError: {}".format(se))
      self._log.error(tb)
      
    except Exception as e:
      tb = traceback.format_exc()
      self._log.error("Exception Encountered: %s" % str(e))
      self._log.error(tb)

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
      sync_val.set_election_term(self._ast.execution_count)
      sync_val.set_key(key)
      sync_val.set_should_end_execution(end_execution)
      await synclog.append(sync_val)
    elif end_execution:
      # Synthecize end
      await synclog.append(SynchronizedValue(None, None, election_term=self._ast.execution_count, should_end_execution=True, key=KEY_SYNC_END))

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