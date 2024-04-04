from typing import Tuple, Optional, Any
from typing_extensions import Protocol, runtime_checkable
import time 
import datetime

KEY_SYNC_END = "_end_"
OP_SYNC_ADD = "add"
OP_SYNC_PUT = "put"
OP_SYNC_DEL = "del"

class SyncValue:
  """A value for log proposal."""

  def __init__(self, tag, val: Any, term:int=0, proposed_node:Optional[int] = -1, timestamp:Optional[float] = time.time(), key:Optional[str]=None, op:Optional[str]=None, prmap:Optional[list[str]]=None, end:bool=False):
    self.term:int = term
    self.key:str = key
    self.prmap = prmap
    self.tag = tag
    self.val = val
    self.proposed_node = proposed_node # Only used by 'SYNC' proposals to specify the node that should serve as the leader.
    self.end = end
    self.op = op
    self.timestamp:float = timestamp # The time at which the proposal/value was issued.

    self._reset = False
  
  def __str__(self)->str:
    ts:str = datetime.datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
    return "SyncValue[Key='%s',Term=%d,Timestamp='%s',Tag='%s',Val='%s']" % (self.key, self.term, ts, str(self.tag), str(self.val)[0:25])
  
  @property
  def reset(self):
    return self._reset

@runtime_checkable
class SyncLog(Protocol):
  @property
  def num_changes(self) -> int: # type: ignore
    """The number of incremental changes since first set or the latest checkpoint."""

  @property
  def term(self) -> int: # type: ignore
    """Current term."""

  def start(self, handler):
    """Register change handler, restore internel states, and start monitoring changes. 
      handler will be in the form listerner(key, val: SyncValue)"""

  def set_should_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
      callback will be in the form callback(SyncLog) bool"""

  def set_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides to checkpoint.
      callback will be in the form callback(Checkpointer)."""

  async def yield_execution(self, term) -> bool:
    """Request yield the update of a term to another replica."""

  async def lead(self, term) -> bool:
    """Request to lead the update of a term. A following append call 
       without leading status will fail."""
    return False

  async def append(self, val: SyncValue):
    """Append the difference of the value of specified key to the synchronization queue."""

  def sync(self, term):
    """Manually trigger the synchronization of changes since specified term."""

  def reset(self, term, logs: Tuple[SyncValue]):
    """Clear logs equal and before specified term and replaced with specified logs"""

  def close(self):
    """Ensure all async coroutines end and clean up."""

@runtime_checkable
class Checkpointer(Protocol):
  @property
  def num_changes(self) -> int:
    """The number of values checkpointed."""
    return 0

  def lead(self, term) -> bool:
    """Set the term to checkpoint. False if any error."""
    return False
       
  async def append(self, val: SyncValue):
    """Append the value of specified key to the writer."""

  def close(self):
    """Ensure all async coroutines end and clean up."""

