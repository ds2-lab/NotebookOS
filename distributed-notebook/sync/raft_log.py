import io
import os
import pickle
import asyncio
from typing import Tuple, List

from ..smr.smr import NewLogNode, NewConfig, WriteCloser
from ..smr.go import Slice_byte, Slice_string
from .log import SyncValue
from .checkpoint import Checkpoint
from .future import Future

KEY_LEAD = "_lead_"

class writerCloser:
  def __init__(self, wc: WriteCloser):
    self.wc = wc

  def write(self, b):
    self.wc.Write(Slice_byte(b))

  def close(self):
    self.wc.Close()

# def getChangeHandler(handler):
#   def changeHandler(buff, id):
#     print("got changed: {}".format(id))
#     if id != "":
#         return
      
#     reader = io.BytesIO(bytes(Slice_byte(handle=buff)))
#     syncval = pickle.load(reader)
#     handler(syncval)

#   return changeHandler

# globalChangeHandler = None

class RaftLog:
  def __init__(self, base_path: str, id: int, peers: List[str], join: bool = False):
    self.store = base_path
    self.id = id
    self.term = 0   # Term will only change if we got value update from the term. Leading status will not change it.
    self.ensure_path(self.store)
    self.node = NewLogNode(self.store, id, Slice_string(peers), join)
    self.handler = None
    self.changeCallback = self._changeHandler  # No idea why this walkaround works
    self.restoreCallback = self._restore       # No idea why this walkaround works
    self.shouldSnapshotCallback = None
    self.snapshotCallback = None

  @property
  def num_changes(self) -> int:
    """The number of incremental changes since first term or the latest checkpoint."""
    return self.node.NumChanges()

  def start(self, handler):
    """Register change handler, restore internel states, and start monitoring changes, """
    self.handler = handler

    config = NewConfig()
    config = config.WithChangeCallback(self.changeCallback).WithRestoreCallback(self.restoreCallback)
    # if self.shouldSnapshotCallback is not None:
    #   config = config.WithShouldSnapshotCallback(self.shouldSnapshotCallback)
    # if self.snapshotCallback is not None:
    #   config = config.WithSnapshotCallback(self.snapshotCallback)
    self.node.Start(config)

  def _changeHandler(self, buff, id):
    print("got changed: {}".format(id))
    if id != "":
        return
    
    try:
      reader = io.BytesIO(bytes(Slice_byte(handle=buff)))
      syncval = pickle.load(reader)
      if syncval.key == KEY_LEAD:
        return
    except Exception as e:
      print("Failed to handle change: {}".format(e))

    self.term = syncval.term
    self.handler(syncval)

  def _restore(self, buff):
    """Restore history"""
    print("Restoring...")

    reader = io.BytesIO(bytes(Slice_byte(handle=buff)))
    unpickler = pickle.Unpickler(reader)
    
    try:
      syncval = None
      syncval = unpickler.load()
    except Exception:
      pass
    
    while syncval is not None:
      self.handler(syncval)

      try:
        syncval = None
        syncval = unpickler.load()
      except Exception:
        pass

  def set_should_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
      callback will be in the form callback(SyncLog) bool"""
    if callback is None:
      self.shouldSnapshotCallback = None
      return
    
    def shouldSnapshotCallback(logNode):
      # Initialize object using LogNode(handle=logNode) if neccessary.
      print("in direct shouldSnapshotCallback")
      return callback(self)
    
    self.shouldSnapshotCallback = shouldSnapshotCallback

  def set_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides to checkpoint.
      callback will be in the form callback(Checkpointer)."""
    if callback is None:
      self.snapshotCallback = None
      return

    def snapshotCallback(wc):
      print("in direct snapshotCallback")
      checkpointer = Checkpoint(writerCloser(WriteCloser(handle=wc)))
      callback(checkpointer)
      checkpointer.close()

    self.snapshotCallback = snapshotCallback

  async def lead(self, term) -> bool:
    """Request to lead the update of a term. A following append call 
       without leading status will fail."""
    if term > 0 and term <= self.term:
      return False

    # Append is blocking and ensure to gaining leading status if terms match.
    await self.append(SyncValue(None, self.id, term=term, key=KEY_LEAD))
    # term 0 is for single node, or we need to check the term
    if term == 0 or self.term < term:
      return True

    return False

  async def append(self, val: SyncValue):
    """Append the difference of the value of specified key to the synchronization queue"""
    self.term = val.term
    if val.key is not None:
      dumped = pickle.dumps(val)
      print("Appending {}:{} bytes".format(val.key, len(dumped)))

      loop = asyncio.get_running_loop()
      future = Future(loop=loop)
      def resolve(key):
        asyncio.run_coroutine_threadsafe(future.resolve(key), loop)

      self.node.Propose(Slice_byte(dumped), resolve, val.key)
      await future.result()

  def sync(self, term):
    """Synchronization changes since specified execution counter."""
    pass

  def reset(self, term, logs: Tuple[SyncValue]):
    """Clear logs equal and before specified term and replaced with specified logs"""
    pass

  def close(self):
    """Ensure all async coroutines end and clean up."""
    self.node.Close()

  def ensure_path(self, base_path):
    if not os.path.exists(base_path):
      os.makedirs(base_path, 0o750)