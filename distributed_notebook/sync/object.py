import io
from .log_pickler import SyncLogPickler as Pickler
from .offset_unpickler import OffsetUnpickler as Unpickler
import hashlib
import logging
from typing import Generator, Tuple, Optional, Any, Callable
from typing_extensions import Protocol, runtime_checkable

from .log import SynchronizedValue, OP_SYNC_PUT, OP_SYNC_ADD
from .referer import EMPTY_TUPLE, SyncReferer, SyncRID, _T
from .profiler import PickleProfiler

@runtime_checkable
class Pickled(Protocol):
	"""Placeholder picklable protocol"""

@runtime_checkable
class SyncObject(Protocol):
  def dump(self, meta=None) -> SynchronizedValue: # type: ignore
    """Get a view of the object for checkpoint."""

  def diff(self, raw, meta=None) -> Optional[SynchronizedValue]: # type: ignore
    """Update the object with new raw object and get the difference view for synchronization"""

  def update(self, val: SynchronizedValue) -> Any:
    """Apply the difference view to the object"""


@runtime_checkable
class SyncStreamObject(Protocol):
  def dump(self, meta=None) -> Generator[SynchronizedValue, None, None]: # type: ignore
    """Get a view of the object for checkpoint in the form of a stream."""

  def diff(self, raw, meta=None) -> Generator[SynchronizedValue, None, None]: # type: ignore
    """Update the object with new raw object and get the difference stream for synchronization"""

  def update(self, vals: Tuple[SynchronizedValue]) -> Any:
    """Apply the difference stream to the object"""

class SyncObjectMeta:
  def __init__(self, batch:Optional[str]=None):
    self.batch: Optional[str] = batch

class SyncObjectWrapper:
  """A simple object wrapper that implemented SyncObject, which returns a view of whole object as the difference."""
  
  def __init__(self, referer:SyncReferer, raw:Any=None, tag:Any=None):
    self.raw: Any = raw
    """The raw object"""

    self._hash: Any = tag
    """The hash of the raw object. For simple objects, it can be the raw object itself, otherwise it is of type bytes."""

    self._referer: SyncReferer = referer
    """The referer to resolve persistent references"""

    self._pickler = Pickler
    self._unpickler = Unpickler
    self._profiling = False

    if self.raw is not None and tag is None:
      _, _, self._hash = self.get_hash(raw, None)

  def config_pickler(self, pickler = None, unpickler = None, profiling = False):
    self._profiling = profiling
    if pickler is not None:
      self._pickler = pickler
    if unpickler is not None:
      self._unpickler = unpickler

  def dump(self, meta=None) -> SynchronizedValue:
    """Get a view of the object for checkpoint."""
    pickled, prmap, hash = self.get_hash(self.raw, self.batch_from_meta(meta))
    return SynchronizedValue(hash, pickled, prmap=prmap)
  
  def diff(self, raw, meta=None) -> Optional[SynchronizedValue]:
    """Update the object with new raw object and get the difference view for synchronization"""
    pickled, prmap, hash = self.get_hash(raw, self.batch_from_meta(meta))
    # print("old {}:{}, new {}:{}, match:{}".format(self.raw, self._hash, raw, hash, hash == self._hash))
    op = OP_SYNC_ADD
    if hash != self._hash:
      if self._hash is not None:
        op = OP_SYNC_PUT
      self._hash = hash
      self.raw = raw
      return SynchronizedValue(hash, pickled, prmap=prmap, operation=op)

    return None

  def update(self, val: SynchronizedValue) -> Any:
    """Apply the difference view to the object"""
    if val.tag == self._hash:
      return self.raw

    if type(val.data) == bytes:
      buff = io.BytesIO(val.data)
      unpickler = self._unpickler(buff)
      unpickler.persistent_load = self._referer.dereference(buff, val.prmap, unpickler=unpickler)
      diff = unpickler.load()
      logging.info("Unpickled: {}".format(diff))
      # Verify tags
      # _, tag = self.get_hash(diff, val.term)
      # if tag != val.tag:
      #   print("tag updated")
      #   val.tag = tag
    else:
      diff = val.data
    
    self._hash = val.tag
    self.raw = diff
    return self.raw

  def get_hash(self, raw, batch: Optional[str], profiling: bool = False) -> Tuple[Any, Optional[list[str]], Any]:
    t = type(raw)
    if raw is None or t is int or t is float or t is bool or raw is EMPTY_TUPLE:
      return raw, None, raw
    elif t is str and len(raw) <= 32:
      return raw, None, raw
    else:
      buff = io.BytesIO()

      # Instaniate pickler
      reference_id_provider: Optional[Callable[[Any, int], SyncRID]] = None
      pickler = self._pickler(buff)
      if self._profiling:
        pickler = PickleProfiler(buff, pickler=pickler)
        reference_id_provider = pickler.get_reference_id

      # Pickle
      pickler.persistent_id, pickle_id = self._referer.reference(batch, rid_provider = reference_id_provider, pickler=pickler)
      pickler.dump(raw)
      pickled = buff.getvalue()
      if hasattr(pickler, "get_polifiller"):
        logging.info("Polyfilling")
        pickle_id.polyfill(pickler.get_polifiller(self._referer.id_from_prid))
      else:
        logging.info("No polyfilling")
      prmap = pickle_id.dump()
      if self._profiling:
        assert isinstance(pickler, PickleProfiler)
        logging.info("Total bytes {}".format(len(pickled)))
        pickler.print_profile(self._referer.id_from_prid, pickle_id.load(prmap))

        logging.info(pickled)
        
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

class SyncObjectUnpickler(Unpickler):
  """Customized unpickler"""