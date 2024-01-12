import pickle

from .log import SyncValue

class Checkpoint:
  def __init__(self, file):
    self._writer = file
    self._pickler = pickle.Pickler(self._writer)
    self._size = 0

  @property
  def num_changes(self) -> int:
    """The number of values checkpointed."""
    return self._size
  
  def lead(self, term) -> bool:
    """Set the term to checkpoint. False if any error."""
    return True

  async def append(self, val: SyncValue):
    """Append the difference of the value of specified key to the synchronization queue"""
    if val.key is not None:
      self._pickler.dump(val)
      self._size = self._size + 1

  def close(self):
    """Ensure all async coroutines end and clean up."""
    self._writer.close()