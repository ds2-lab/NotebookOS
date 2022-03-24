import io
import os
import pickle
import asyncio
from typing import Tuple, List

from pexpect import ExceptionPexpect

from ..smr.smr import NewLogNode, NewConfig, WriteCloser
from ..smr.go import Slice_byte, Slice_string
from .log import SyncValue, KEY_SYNC_END
from .checkpoint import Checkpoint
from .future import Future
from .errors import SyncError, GoError, GoNilError

KEY_LEAD = "_lead_"

class writerCloser:
  def __init__(self, wc: WriteCloser):
    self.wc = wc

  def write(self, b):
    ret = self.wc.Write(Slice_byte(b))

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
    self._ignore_changes = 0
    self._async_loop = None

  @property
  def num_changes(self) -> int:
    """The number of incremental changes since first term or the latest checkpoint."""
    return self.node.NumChanges() - self._ignore_changes

  async def start(self, handler):
    """Register change handler, restore internel states, and start monitoring changes, """
    self.handler = handler

    config = NewConfig()
    config = config.WithChangeCallback(self.changeCallback).WithRestoreCallback(self.restoreCallback)
    if self.shouldSnapshotCallback is not None:
      config = config.WithShouldSnapshotCallback(self.shouldSnapshotCallback)
    if self.snapshotCallback is not None:
      config = config.WithSnapshotCallback(self.snapshotCallback)

    self.async_loop = asyncio.get_running_loop()
    self.node.Start(config)

  def _changeHandler(self, buff, id) -> str:
    print("got changed: {}".format(id))
    if id != "":
        return GoNilError()
    
    try:
      reader = io.BytesIO(bytes(Slice_byte(handle=buff)))
      syncval = pickle.load(reader)
      if syncval.key == KEY_LEAD:
        self._ignore_changes = self._ignore_changes + 1
        return GoNilError()

      self.term = syncval.term
      # For values synchronized from other replicas or replayed, count _ignore_changes
      if syncval.op is None or syncval.op == "":
        self._ignore_changes = self._ignore_changes + 1
      self.handler(syncval)

      return GoNilError()
    except Exception as e:
      print("Failed to handle change: {}".format(e))
      return GoError(e)

  def _restore(self, buff) -> str:
    """Restore history"""
    print("Restoring...")

    reader = io.BytesIO(bytes(Slice_byte(handle=buff)))
    unpickler = pickle.Unpickler(reader)

    try:
      syncval = None
      syncval = unpickler.load()
    except Exception:
      pass
    
    # Recount _ignore_changes
    self._ignore_changes = 0
    restored = 0
    while syncval is not None:
      try:
        self.handler(syncval)
        restored = restored + 1

        syncval = None
        syncval = unpickler.load()
      except SyncError as se:
        print("Error on restoreing snapshot: {}".format(se))
        return GoError(se)
      except Exception:
        pass

    print("Restored {}".format(restored))
    return GoNilError()

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

    def snapshotCallback(wc) -> str:
      try:
        checkpointer = Checkpoint(writerCloser(WriteCloser(handle=wc)))
        # asyncio.run_coroutine_threadsafe(callback(checkpointer), self.async_loop)
        callback(checkpointer)
        # checkpointer.close()
        # Reset _ignore_changes
        self._ignore_changes = 0
        return GoNilError()
      except Exception as e:
        print("Error on snapshoting: {}".format(e))
        return GoError(e)

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
    if val.key != KEY_LEAD:
      self.term = val.term

    if val.op is None or val.op == "":
      # Count _ignore_changes
      self._ignore_changes = self._ignore_changes + 1

    # Ensure key is specified.
    if val.key is not None:
      # Serialize the value. 
      dumped = pickle.dumps(val)
      print("Appending {}:{} bytes".format(val.key, len(dumped)))

      # Prepare callback settings. 
      # Callback can be called from a different thread. Schedule the resolve
      # of the future object to the await thread.
      loop = asyncio.get_running_loop()
      future = Future(loop=loop)
      self._async_loop = loop
      def resolve(key):
        asyncio.run_coroutine_threadsafe(future.resolve(key), loop) # must use local variable

      # Propose and wait the future.
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