from typing import Tuple
from typing_extensions import Protocol, runtime_checkable

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
    """Register change handler, restore internel states, and start monitoring changes. 
      handler will be in the form listerner(key, val: SyncValue)"""

  def set_should_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
      callback will be in the form callback(SyncLog) bool"""

  def set_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides to checkpoint.
      callback will be in the form callback(Checkpointer)."""

  def lead(self, term) -> bool:
    """Request to lead the update of a term. A following append call 
       without leading status will fail."""

  def append(self, val: SyncValue):
    """Append the difference of the value of specified key to the synchronization queue."""

  def sync(self, term):
    """Manually trigger the synchronization of changes since specified term."""

  def reset(self, term, logs: Tuple[SyncValue]):
    """Clear logs equal and before specified term and replaced with specified logs"""

  def close(self):
    """Ensure all async coroutines end and clean up."""

@runtime_checkable
class Checkpointer(Protocol):
  def lead(self, term) -> bool:
    """Set the term to checkpoint. False if any error."""
       
  def append(self, val: SyncValue):
    """Append the value of specified key to the writer."""

  def close(self):
    """Ensure all async coroutines end and clean up."""

