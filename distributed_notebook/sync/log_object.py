import io
from pickle import Pickler as Pickler
from pickle import Unpickler as Unpickler
import hashlib
import logging
from typing import Generator, Tuple, Optional, Any, Callable
from typing_extensions import Protocol, runtime_checkable

from .log import SyncValue, OP_SYNC_PUT, OP_SYNC_ADD
from .referer import EMPTY_TUPLE, SyncReferer, SyncRID
from .profiler import PickleProfile, PickleProfiler
from .object import SyncObjectMeta

@runtime_checkable
class SyncLogObject:
  """A object wrapper that implemented SyncObject and support incremental synchronization."""

  def __init__(self, referer:SyncReferer, raw:Any=None, tag:Any=None):
    self.raw: Any = raw
    """The raw object"""

    self._hash: Any = tag
    """The hash of the raw object. For simple objects, it can be the raw object itself, otherwise it is of type bytes."""

    self._obj_tree: Any = tag
    """The object hierarchy tree. For simple objects, it can be the raw object itself."""

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

  def dump(self, meta=None) -> SyncValue:
    """Get a view of the object for checkpoint."""
    pickled, prmap, hash = self.get_hash(self.raw, self.batch_from_meta(meta))
    return SyncValue(hash, pickled, prmap=prmap)
  
  def diff(self, raw, meta=None) -> Optional[SyncValue]:
    """Update the object with new raw object and get the difference view for synchronization"""
    pickled, prmap, hash = self.get_hash(raw, self.batch_from_meta(meta))
    # print("old {}:{}, new {}:{}, match:{}".format(self.raw, self._hash, raw, hash, hash == self._hash))
    op = OP_SYNC_ADD
    if hash != self._hash:
      if self._hash is not None:
        op = OP_SYNC_PUT
      self._hash = hash
      self.raw = raw
      return SyncValue(hash, pickled, prmap=prmap, op=op)

    return None

  def update(self, val: SyncValue) -> Any:
    """Apply the difference view to the object"""
    if val.tag == self._hash:
      return self.raw

    if type(val.val) == bytes:
      buff = io.BytesIO(val.val)
      unpickler = Unpickler(buff)
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
      if self._profiling:
        pickler = PickleProfiler(buff)
        reference_id_provider = pickler.get_reference_id
      else:
        pickler = self._pickler(buff)
      
      # Pickle
      pickler.persistent_id, pickle_id = self._referer.reference(batch, rid_provider = reference_id_provider)
      pickler.dump(raw)
      pickled = buff.getvalue()
      prmap = pickle_id.dump()
      if self._profiling:
        assert isinstance(pickler, PickleProfiler)
        logging.info("Total bytes {}".format(len(pickled)))
        pickler.print_profile(lambda x: id(self._referer._dereference(x)), prmap)

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

  def _rebuild_obj_tree(self, raw: Any, batch: Optional[str], profiling: bool = False):
    """Compare raw object with the current object and incorporated into the object tree"""
