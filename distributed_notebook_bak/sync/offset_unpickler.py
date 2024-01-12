from typing import IO
from pickle import _Unpickler as Unpickler
import logging

from .protocols import SyncPRID
# from .profiler.profiler import show_memo

class OffsetUnpickler(Unpickler):
  """Unpickling steam containing multiple objects by offset."""

  _interrupt_fields = ("_unframer", "read", "readinto", "readline", "metastack", "stack", "append", "proto")

  def __init__(self, buffer: IO[bytes], *args, **kwargs):
    super().__init__(buffer, *args, **kwargs)

    self._buffer: IO[bytes] = buffer

    self.prmap: tuple[SyncPRID] = ()
    """A tuple of PRID that contains offsets of objects in the buffer. If omitted, the unpickler will behavior like a normal unpickler."""

    self._unframer_stack = []

    # OffsetUnpickler.dispatch[pickle.TUPLE3[0]] = load_tuple3

  def load(self):
    """Seek stream to the offset of the next object and load it."""
    # show_memo("unpickling", self.memo)

    pos = self._buffer.tell()
    logging.info("Starting load from {}:{}".format(pos, self._buffer.read(29)))
    self._buffer.seek(pos)

    self._interupt()
    obj = super().load()
    self._resume()

    logging.info("Loaded {}-{} ({} bytes)".format(pos, self._buffer.tell(), self._buffer.tell() - pos))
    return obj
  
  def _interupt(self):
    if not hasattr(self, "_unframer"):
      return
    
    _unframer = {}
    # logging.info("Interupting: {}".format(self.__dict__))
    for field in self._interrupt_fields:
      _unframer[field] = self.__dict__[field]
    self._unframer_stack.append(_unframer)

  def _resume(self):
    if len(self._unframer_stack) == 0:
      return
    
    _unframer = self._unframer_stack.pop()
    for field in self._interrupt_fields:
      self.__dict__[field] = _unframer[field]
  
# def load_tuple3(self):
#   logging.info("Loading tuple3: {}".format(self.stack))
#   self.stack[-3:] = [(self.stack[-3], self.stack[-2], self.stack[-1])]

