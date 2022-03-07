from typing import Tuple
from typing_extensions import Protocol, runtime_checkable

CHECKPOINT_ARCHIVE = "checkpoint.dat"

class SyncValue:
  def __init__(self, tag, val, term=None, key=None, op=None, prmap=None, end = False):
    self.term = term
    self.key = key
    self.prmap = prmap
    self.tag = tag
    self.val = val
    self.end = end
    self.op = op

    self._reset = False
  
  @property
  def reset(self):
    return self._reset

@runtime_checkable
class SyncLog(Protocol):
  @property
  def num_changes(self) -> int:
    """The number of incremental changes since first term or the latest checkpoint."""

  def start(self, handler):
    """Register change handler, restore internel states, and start monitoring changes, """

  def checkpoint(self):
    """Get a SyncLog instance for checkpointing. After which the reset can be called to reset num_changes."""

  def lead(self, term) -> bool:
    """Request to lead the update of a term. A following append call 
       without leading status will fail."""

  def append(self, val: SyncValue):
    """Append the difference of the value of specified key to the synchronization queue."""

  def on_change(self, handler):
    """Register handler function that will be callbacked on changing of value. 
       handler will be in the form listerner(key, val: SyncValue)"""

  def sync(self, term):
    """Manually trigger the synchronization of changes since specified term."""

  def reset(self, term, logs: Tuple[SyncValue]):
    """Clear logs equal and before specified term and replaced with specified logs"""

  def close(self):
    """Ensure all async coroutines end and clean up."""

