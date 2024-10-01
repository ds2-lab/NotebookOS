import os
import pickle
from typing import Tuple

from .log import SyncLog, SynchronizedValue

FILELOG_ARCHIVE = "lineage.dat"
CHECKPOINT_ARCHIVE = "checkpoint.dat"

FILELOG_VERSION = 1

class FileLog:
  def __init__(self, base_path):
    self.store = base_path
    self.ensure_path(self.store)
    self.term = 0 # Term start from 1, initialization is required.
    self.skip_terms = 0 
    self.logs = []
    self._num_changes = 0 # Number of changes since term 1 or latest checkpoint.

    self._handlers = []
    self.shouldCheckpointCallback = None
    self.checkpointCallback = None

  def __getstate__(self):
    return (FILELOG_VERSION, self.term, self.skip_terms, self.logs, self._num_changes)

  def __setstate__(self, state):
    self.term = state[1]
    self.skip_terms = state[2]
    self.logs = state[3]
    self._num_changes = state[4]

  @property
  def num_changes(self) -> int:
    """The number of incremental changes since first term or the latest checkpoint."""
    return self._num_changes

  async def start(self, handler):
    """Register change handler, restore internel states, and start monitoring changes, """
    self.on_change(handler)

  def restore(self, filename) -> bool:
    """Restore history"""
    # Try restore from checkpoint first
    if self.term == 0 and filename != CHECKPOINT_ARCHIVE:
      self.restore(CHECKPOINT_ARCHIVE)

    if not os.path.exists(os.path.join(self.store, filename)):
      return False

    term = self.term
    incremental = False
    with open(os.path.join(self.store, filename), "rb") as file:
      # Restore variabled
      log = pickle.load(file)
      if self.term == 0:
        for key in log.__dict__.keys():
          self.__dict__[key] = log.__dict__[key]
        term = self.skip_terms
      elif log.term > self.term:
        if self.term < log.skip_terms:
          print("Missing the lineage of term: {}-{}".format(self.term+1, log.skip_terms))
          return False
        # Merge from checkpoint
        self.logs.extend(log.logs[self.term-log.skip_terms:])
        self.term = log.term
        incremental = True

    print("{} restored: {}".format(filename, self.term))
    for term in range(term+1, self.term+1):
      print("apply term: {}".format(term))
      for filepath in self.logs[term-self.skip_terms-1]:
        val = self.load(os.path.join(self.store, filepath))
        # Update for incremental changes only.
        if incremental:
          self._num_changes = self._num_changes + 1
        # Call change handlers
        for handlers in self._handlers:
          handlers(val)
    
    return True

  def save(self, filename=FILELOG_ARCHIVE):
    self.ensure_path(self.store)
    
    with open(os.path.join(self.store, filename), "wb") as file:
      # backup unpersistable variables
      handlers = self._handlers

      # Persist variables
      self._handlers = None
      pickle.dump(self, file)

      # Restore unpersistable variables
      self._handlers = handlers
    
    if (filename == FILELOG_ARCHIVE and 
        self.shouldCheckpointCallback is not None and self.checkpointCallback is not None and
        self.shouldCheckpointCallback(self)):
      checkpointer = self.checkpoint()
      self.checkpointCallback(checkpointer)
      checkpointer.close()

  def set_should_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
      callback will be in the form callback(SyncLog) bool"""
    self.shouldCheckpointCallback = callback

  def set_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides to checkpoint.
      callback will be in the form callback(Checkpointer)."""
    self.checkpointCallback = callback

  def checkpoint(self):
    """Get a SyncLog instance for checkpointing."""
    return FileCheckpoint(self.store, self)

  async def lead(self, term) -> bool:
    """Request to lead the update of a term. A following append call 
       without leading status will fail."""
    if self.term == 0:
      self.restore(FILELOG_ARCHIVE)

    # Add support for term dectection: term == 0. Always success.
    if term == 0:
      self.logs.append([])
      return True

    if term <= self.skip_terms or term <= len(self.logs) + self.skip_terms:
      return False

    while len(self.logs) + self.skip_terms < term:
      self.logs.append([])
    return True

  async def append(self, val: SynchronizedValue):
    """Append the difference of the value of specified key to the synchronization queue"""
    if val.key is not None:
      relative_path = self.get_path(val.election_term, val)
      filepath = os.path.join(self.store, relative_path)
      self.ensure_path(os.path.dirname(filepath))

      with open(filepath, "wb") as file:
        pickle.dump(val, file)

      self.term = val.election_term
      # In some cases, log slot may not be allocated, so we don't need to update the number of changes.
      if len(self.logs) > val.election_term-self.skip_terms-1:
        self.logs[val.election_term-self.skip_terms-1].append(relative_path)
      # Update if not first term or the checkpoint term: self.term == self.skip_terms + 1
      if val.election_term > self.skip_terms + 1:
        self._num_changes = self._num_changes + 1

    if val.should_end_execution:
      # Call save without filename, so inherited classes may customize default value of save().
      self.save()

    return relative_path

  def _load(self, filepath: str) -> SynchronizedValue:
    """Load the value of the specified filepath."""
    with open(os.path.join(self.store, filepath), "rb") as file:
      return pickle.load(file)

  def on_change(self, handler):
    """Register handler function that will be callbacked on changing of value. handler will be in the form listerner(key, val: SynchronizedValue)"""
    self._handlers.append(handler)

    if self.term == 0:
      self.restore(FILELOG_ARCHIVE)

  def sync(self, term):
    """Synchronization changes since specified execution counter."""
    self.restore(FILELOG_ARCHIVE)

  def reset(self, term, logs: Tuple[SynchronizedValue]):
    """Clear logs equal and before specified term and replaced with specified logs"""
    self.term = term
    self.skip_terms = term - 1
    self.logs = [logs]
    self._num_changes = 0

  def close(self):
    """Ensure all async coroutines end and clean up."""

  def ensure_path(self, base_path):
    if not os.path.exists(base_path):
      os.makedirs(base_path, 0o750, exist_ok=True)

  def get_path(self, term, val: SynchronizedValue):
    # TODO: Sanitize the key.
    return os.path.join("t{}".format(term), val.key)

class FileCheckpoint(FileLog):
  def __init__(self, base_path, sync_log: SyncLog):
    super().__init__(base_path)
    self._sync_log = sync_log
    self.logs.append(None)

  def save(self, filename=CHECKPOINT_ARCHIVE):
    super().save(filename)
    if self._sync_log is not None:
      self._sync_log.reset(self.term, self.logs[0])

  def lead(self, term) -> bool:
    """Initiate the checkpoint"""
    self.logs[0] = []
    self.skip_terms = term - 1
    self.term = term
    return True

  def get_path(self, term, val: SynchronizedValue):
    # TODO: Sanitize the key.
    return os.path.join("c{}".format(term), val.key)