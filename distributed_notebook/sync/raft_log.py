import io
import os
import pickle
import asyncio
import logging
import time
import sys
import traceback
import ctypes
from typing import Tuple, List

from ..smr.smr import NewLogNode, NewConfig, NewBytes, Bytes, WriteCloser, ReadCloser
from ..smr.go import Slice_string
from .log import SyncValue, KEY_SYNC_END
from .checkpoint import Checkpoint
from .future import Future
from .errors import SyncError, GoError, GoNilError
from .reader import readCloser

KEY_LEAD = "_lead_"

def print_trace():
  _, _, exc_traceback = sys.exc_info()
  traceback.print_tb(exc_traceback, limit=5, file=sys.stdout)

class writeCloser:
  def __init__(self, wc: WriteCloser):
    self.wc = wc

  def write(self, b):
    self.wc.Write(NewBytes(b))

  def close(self):
    self.wc.Close()

# class buffer:
#   def __init__(self, raw: bytes):
#     self.handle = raw
#     self.goHandle = NewBytes(bytes)

#   def __call__(self):
#     return self.goHandle

# class rawReader(io.RawIOBase):
#   def __init__(self, rc: ReadCloser):
#     self.rc = rc
#     self.buffer = None

#   def readinto(self, b):
#     if self.buffer is None or b is not self.buffer.raw:
#       self.buffer = buffer(b)
    
#     ret = self.rc.Read(self.buffer())
#     return ret.N

#   def readable(self):
#     return True

#   def close(self):
#     self.rc.Close()

# class readCloser(io.BufferedReader):
#   def __init__(self, rc: ReadCloser, size = io.DEFAULT_BUFFER_SIZE):
#     self.rc = rc
#     # The size is passed to prevent allocating a large buffer for fixed sized data.
#     # No bigger than DEFAULT_BUFFER_SIZE buffer is allowed.
#     if size > io.DEFAULT_BUFFER_SIZE:
#       size = io.DEFAULT_BUFFER_SIZE
#     super().__init__(rawReader(rc), size)

#   def close(self):
#     self.rc.Close()

class RaftLog:
  def __init__(self, base_path: str, id: int, peers: List[str], join: bool = False):
    self._store = base_path
    self._id = id
    self._term = 0   # Term will only change if we got value update from the term. Leading status will not change it.
    self.ensure_path(self._store)
    self._node = NewLogNode(self._store, id, Slice_string(peers), join)
    self._handler = None
    self._changeCallback = self._changeHandler  # No idea why this walkaround works
    self._restoreCallback = self._restore       # No idea why this walkaround works
    self._shouldSnapshotCallback = None
    self._snapshotCallback = None
    self._ignore_changes = 0
    self._async_loop = None
    self._start_loop = None
    self._log = logging.getLogger(__class__.__name__ + str(id))
    self._log.setLevel(logging.DEBUG)
    self._closed = None

  @property
  def num_changes(self) -> int:
    """The number of incremental changes since first term or the latest checkpoint."""
    return self._node.NumChanges() - self._ignore_changes

  def start(self, handler):
    """Register change handler, restore internel states, and start monitoring changes, """
    self._handler = handler

    config = NewConfig()
    config.ElectionTick = 10
    config.HeartbeatTick = 1
    config = config.WithChangeCallback(self._changeCallback).WithRestoreCallback(self._restoreCallback)
    if self._shouldSnapshotCallback is not None:
      config = config.WithShouldSnapshotCallback(self._shouldSnapshotCallback)
    if self._snapshotCallback is not None:
      config = config.WithSnapshotCallback(self._snapshotCallback)

    self._async_loop = asyncio.get_running_loop()
    self._start_loop = self._async_loop
    # self._node.StartWait(config)
    self._node.Start(config)
    self._log.debug("Started")
    # self._closed = Future(self._start_loop)
    # return self._closed.future

  def _changeHandler(self, rc, sz, id) -> bytes:
    if id != "":
      self._log.debug("Confirm committed: {}".format(id))
      return GoNilError()
    else:
      self._log.debug("Get remote update")
    
  #   future = asyncio.run_coroutine_threadsafe(self._changeImpl(buff, id), self._start_loop)
  #   return future.result()

  # async def _changeImpl(self, buff, id) -> str:
    reader = readCloser(ReadCloser(handle=rc), sz)
    try:
      syncval = pickle.load(reader)
      if syncval.key == KEY_LEAD:
        self._ignore_changes = self._ignore_changes + 1
        return GoNilError()

      self._term = syncval.term
      # For values synchronized from other replicas or replayed, count _ignore_changes
      if syncval.op is None or syncval.op == "":
        self._ignore_changes = self._ignore_changes + 1
      self._handler(syncval)

      return GoNilError()
    except Exception as e:
      self._log.error("Failed to handle change: {}".format(e))
      print_trace()
      return GoError(e)
    finally:
      reader.close()

  def _restore(self, rc, sz) -> bytes:
  #   future = asyncio.run_coroutine_threadsafe(self._restoreImpl(buff), self._start_loop)
  #   return future.result()

  # async def _restoreImpl(self, buff) -> bytes:
  #   """Restore history"""
    self._log.debug("Restoring...")

    reader = readCloser(ReadCloser(handle=rc), sz)
    unpickler = pickle.Unpickler(reader)

    try:
      syncval = None
      syncval = unpickler.load()
    except Exception:
      pass
    finally:
      reader.close()
    
    # Recount _ignore_changes
    self._ignore_changes = 0
    restored = 0
    while syncval is not None:
      try:
        self._handler(syncval)
        restored = restored + 1

        syncval = None
        syncval = unpickler.load()
      except SyncError as se:
        self._log.error("Error on restoreing snapshot: {}".format(se))
        return GoError(se)
      except Exception:
        pass

    self._log.debug("Restored {}".format(restored))
    return GoNilError()

  def set_should_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
      callback will be in the form callback(SyncLog) bool"""
    if callback is None:
      self._shouldSnapshotCallback = None
      return
    
    def shouldSnapshotCallback(logNode):
      # Initialize object using LogNode(handle=logNode) if neccessary.
      # print("in direct shouldSnapshotCallback")
      return callback(self)
    
    self._shouldSnapshotCallback = shouldSnapshotCallback

  def set_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides to checkpoint.
      callback will be in the form callback(Checkpointer)."""
    if callback is None:
      self._snapshotCallback = None
      return

    def snapshotCallback(wc) -> str:
      try:
        checkpointer = Checkpoint(writeCloser(WriteCloser(handle=wc)))
        # asyncio.run_coroutine_threadsafe(callback(checkpointer), self._async_loop)
        callback(checkpointer)
        # checkpointer.close()
        # Reset _ignore_changes
        self._ignore_changes = 0
        return GoNilError()
      except Exception as e:
        self._log.error("Error on snapshoting: {}".format(e))
        return GoError(e)

    self._snapshotCallback = snapshotCallback

  async def lead(self, term) -> bool:
    """Request to lead the update of a term. A following append call 
       without leading status will fail."""
    if term > 0 and term <= self._term:
      return False
    
    # Append is blocking and ensure to gaining leading status if terms match.
    await self.append(SyncValue(None, self._id, term=term, key=KEY_LEAD))
    # term 0 is for single node, or we need to check the term
    if term == 0 or self._term < term:
      return True

    return False

  async def append(self, val: SyncValue):
    """Append the difference of the value of specified key to the synchronization queue"""
    if val.key != KEY_LEAD:
      self._term = val.term

    if val.op is None or val.op == "":
      # Count _ignore_changes
      self._ignore_changes = self._ignore_changes + 1

    # Ensure key is specified.
    if val.key is not None:
      # Serialize the value. 
      # start_time = time.time()
      dumped = pickle.dumps(val)
      # self._log.debug("Time elapsed in dump syncValue: {}".format(time.time() - start_time))

      # Prepare callback settings. 
      # Callback can be called from a different thread. Schedule the resolve
      # of the future object to the await thread.
      loop = asyncio.get_running_loop()
      future = Future(loop=loop)
      self._async_loop = loop
      def resolve(key, err):
        asyncio.run_coroutine_threadsafe(future.resolve(key, err), loop) # must use local variable

      # Propose and wait the future.
      
      # self._log.debug("Time elapsed in python before appending: {}, {}".format(time.time() - start_time, len(dumped)))
      # converted = Slice_byte(handle=id(dumped))
      # self._log.debug("Time elapsed in after converted: {}, {}".format(time.time() - start_time, len(converted)))
      self._node.Propose(NewBytes(dumped), resolve, val.key)
      await future.result()

  def sync(self, term):
    """Synchronization changes since specified execution counter."""
    pass

  def reset(self, term, logs: Tuple[SyncValue]):
    """Clear logs equal and before specified term and replaced with specified logs"""
    pass

  def close(self):
    """Ensure all async coroutines end and clean up."""
    self._node.Close()
    if self._closed is not None:
      asyncio.run_coroutine_threadsafe(self._closed.resolve(None, None), self._start_loop)
      self._closed = None

  def ensure_path(self, base_path):
    if not os.path.exists(base_path):
      os.makedirs(base_path, 0o750)