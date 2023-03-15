import io
import pickle
import hashlib
from typing import Generator, Tuple, Optional, Any
from typing_extensions import Protocol, runtime_checkable

from .log import SyncValue, OP_SYNC_PUT
from .referer import EMPTY_TUPLE, SyncReferer

@runtime_checkable
class Pickled(Protocol):
	"""Placeholder picklable protocol"""

@runtime_checkable
class SyncObject(Protocol):
  def dump(self, meta=None) -> SyncValue: # type: ignore
    """Get a view of the object for checkpoint."""

  def diff(self, raw, meta=None) -> Optional[SyncValue]: # type: ignore
    """Update the object with new raw object and get the difference view for synchronization"""

  def update(self, val: SyncValue) -> Any:
    """Apply the difference view to the object"""


@runtime_checkable
class SyncStreamObject(Protocol):
  def dump(self, meta=None) -> Generator[SyncValue, None, None]: # type: ignore
    """Get a view of the object for checkpoint in the form of a stream."""

  def diff(self, raw, meta=None) -> Generator[SyncValue, None, None]: # type: ignore
    """Update the object with new raw object and get the difference stream for synchronization"""

  def update(self, vals: Tuple[SyncValue]) -> Any:
    """Apply the difference stream to the object"""

class SyncObjectMeta:
  def __init__(self, batch=None):
    self.batch = batch

class SyncObjectWrapper:
  """A simple SyncObject implementation that simply return a view of whole object as the difference."""
  
  def __init__(self, referer:SyncReferer, raw:Any=None, tag:Any=None):
    self.raw = raw
    self._hash = tag
    self._referer = referer
    if self.raw is not None and tag is None:
      _, _, self._hash = self.get_hash(raw, None)

  def dump(self, meta=None) -> SyncValue:
    pickled, prmap, hash = self.get_hash(self.raw, self.batch_from_meta(meta))
    return SyncValue(hash, pickled, prmap=prmap)
  
  def diff(self, raw, meta=None) -> Optional[SyncValue]:
    pickled, prmap, hash = self.get_hash(raw, self.batch_from_meta(meta))
    # print("old {}:{}, new {}:{}, match:{}".format(self.raw, self._hash, raw, hash, hash == self._hash))
    op = None
    if hash != self._hash:
      if self._hash is not None:
        op = OP_SYNC_PUT
      self._hash = hash
      self.raw = raw
      return SyncValue(hash, pickled, prmap=prmap, op=op)

    return None

  def update(self, val: SyncValue) -> Any:
    if val.tag == self._hash:
      return self.raw

    if type(val.val) == bytes:
      buff = io.BytesIO(val.val)
      unpickler = pickle.Unpickler(buff)
      unpickler.persistent_load = self._referer.dereference(val.prmap)
      diff = unpickler.load()
      # Verify tags
      # _, tag = self.get_hash(diff, val.term)
      # if tag != val.tag:
      #   print("tag updated")
      #   val.tag = tag
    else:
      diff = val.val
    
    self._hash = val.tag
    self.raw = diff
    return self.raw

  def get_hash(self, raw, batch: Optional[str]) -> Tuple[Any, Optional[list[str]], Any]:
    t = type(raw)
    if raw is None or t is int or t is float or t is bool or raw is EMPTY_TUPLE:
      return raw, None, raw
    elif t is str and len(raw) <= 32:
      return raw, None, raw
    else:
      buff = io.BytesIO()
      pickler = pickle.Pickler(buff)
      pickler.persistent_id, pickle_id = self._referer.reference(batch)
      pickler.dump(raw)
      pickled = buff.getvalue()
      prmap = pickle_id.dump()
      # prmap = None
      # Should work in the case the pickled is a reference.
      return pickled, prmap, hashlib.md5(pickled).digest()

  def batch_from_meta(self, meta:Optional[SyncObjectMeta]=None) -> Optional[str]:
    if meta is None:
      return None
    
    return meta.batch

  @property
  def object(self):
    return self.raw

  @property
  def tag(self):
    return self._hash

class SyncObjectUnpickler(pickle.Unpickler):
  """Customized unpickler"""