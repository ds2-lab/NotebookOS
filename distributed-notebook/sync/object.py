import io
import pickle
import hashlib
from typing import Generator, Tuple
from typing_extensions import Protocol, runtime_checkable

from .log import SyncValue
from .referer import EMPTY_TUPLE

@runtime_checkable
class Pickled(Protocol):
	"""Placeholder picklable protocol"""

@runtime_checkable
class SyncObject(Protocol):
  def dump(self, meta=None) -> SyncValue:
    """Get pickled state and tag of the object."""

  def diff(self, raw, meta=None) -> SyncValue:
    """Update with raw object and get difference between the object and specified raw object"""

  def update(self, val: SyncValue) -> any:
    """Update difference to the object"""


@runtime_checkable
class SyncStreamObject(Protocol):
  def dump(self, meta=None) -> Generator[SyncValue, None, None]:
    """Get pickled state and tag of the object."""

  def diff(self, raw, meta=None) -> Generator[SyncValue, None, None]:
    """Update with raw object and get difference between the object and specified raw object"""

  def update(self, vals: Tuple[SyncValue]) -> any:
    """Update difference to the object"""

class SyncObjectMeta:
  def __init__(self, batch=None):
    self.batch = batch

class SyncObjectWrapper:
  def __init__(self, referer, raw=None, tag=None):
    self.raw = raw
    self._hash = tag
    self._referer = referer
    if self.raw is not None and tag is None:
      _, self._hash = self.get_tag(raw)

  def dump(self, meta=None) -> SyncValue:
    pickled, prmap, hash = self.get_hash(self.raw, self.batch_from_meta(meta))
    return SyncValue(hash, pickled, prmap=prmap)
  
  def diff(self, raw, meta=None) -> SyncValue:
    pickled, prmap, hash = self.get_hash(raw, self.batch_from_meta(meta))
    # print("old {}:{}, new {}:{}, match:{}".format(self.raw, self._hash, raw, hash, hash == self._hash))
    if hash != self._hash:
      self._hash = hash
      self.raw = raw
      return SyncValue(hash, pickled, prmap=prmap)

    return None

  def update(self, val: SyncValue) -> any:
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

  def get_hash(self, raw, batch) -> Tuple[any, tuple, any]:
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
      # Should work in the case the pickled is a reference.
      return pickled, pickle_id.dump(), hashlib.md5(pickled).digest()

  def batch_from_meta(self, meta:SyncObjectMeta):
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