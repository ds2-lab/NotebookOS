import pickle

from .log import SyncValue

class Checkpoint:
  def __init__(self, file):
    self.writer = file
    self.pickler = pickle.Pickler(self.writer)
  
  def lead(self, term) -> bool:
    """Set the term to checkpoint. False if any error."""
    return True

  def append(self, val: SyncValue):
    """Append the difference of the value of specified key to the synchronization queue"""

    if val.key is not None:
      self.pickler.dump(val)

  def close(self):
    """Ensure all async coroutines end and clean up."""
    self.writer.close()