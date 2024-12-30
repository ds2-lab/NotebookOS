from typing import IO, Any, Optional, Union
from types import BuiltinFunctionType, FunctionType
import inspect
import logging

from .protocols import SyncRID, SyncRIDProvider, SyncPRID, SyncPRIDProvider
from .referer import EMPTY_TUPLE, _T, SKIP_SYNC, SyncReferer, SyncReference, SyncPickleId

class SyncLogPRID:
  """SyncLog's implementation of SyncPRID.
    This class conforms to SyncPRID protocol.
    This class provides a method that conforms SyncPRIDProvider definition.
  """

  @staticmethod
  def wrap(raw: any) -> SyncPRID:
    if isinstance(raw, str):
      return SyncLogPRID(raw)
    elif isinstance(raw, tuple) and len(raw) >= 2:
      return SyncLogPRID(raw[0], raw[1])
    else:
      raise TypeError("raw must be a string or a tuple of (string, int).")
  
  def __init__(self, prid: str, offset: int = -1):
    self._prid: str = prid
    """The PRID string."""

    self._offset: int = offset
    """The start position of the pickled object in the stream."""

  def __prid__(self) -> str:
    return self._prid
  
  def raw(self) -> any:
    return (self._prid, self._offset)
  
  def __str__(self) -> str:
    return "({},{})".format(self._prid, self._offset)
    
class SyncLogReferer(SyncReferer):
  """Used to deduplicate multiple references of the same object for 
  serializing/deserializing. Example use cases:
  1. Serializing, referenced object has never serialized.
  2. Serializing, referenced object has been serialized locally.
  3. Serializing, referenced object has been serialized as a part of other objects.
  4. Serializing, referenced object has been serialized remotely and restored locally.
  5. Deserializing, reference id and object has specified.
  6. Deserializing, reference id has specified."""
  
  def __init__(self):
    # Call super constructor
    super().__init__()
    self._prid_provider = SyncLogPRID.wrap

  def dereference(self, buff: IO[bytes], prmap: Optional[tuple], protocol:Optional[int]=None, unpickler=None):
    _normalized_prmap: Optional[list[SyncPRID]] = prmap

    # Seek to the start position of the first object
    if prmap is not None and len(prmap) > 0:
       # Normalize prmap if necessary
      if not isinstance(prmap[0], SyncPRID):
        _normalized_prmap = list(map(lambda raw: self._prid_provider(raw), prmap))
      buff.seek(_normalized_prmap[0]._offset)

    return super().dereference(buff, _normalized_prmap, protocol, unpickler)

  def _reference(self, obj, pickle_id:SyncPickleId) -> tuple[Optional[Union[str, int, _T]], Optional[str]]:
    """persistent_id implementation for Pickler. Return pickle compatible persistent ID and PRID."""
    if isinstance(obj, _T) and len(obj) > 0:
      logging.warning("Find _T:{}".format(str(obj[0])))

    if isinstance(obj, SyncRID):
      # For SyncRID, we must return the int as its pickle compatible persistent ID.
      rid = obj.__rid__()
      # self.pickler.dump(self.referers[pickle_id._prmap[rid].__prid__()].obj)
      return rid, None
    elif isinstance(obj, BuiltinFunctionType) or isinstance(obj, FunctionType) or inspect.isclass(obj) or isinstance(obj, tuple):
    # elif isinstance(obj, FunctionType) or inspect.isclass(obj) or isinstance(obj, tuple):  
      # Excluding tuple is essential for pickling SyncRID not falling into recursive error.
      return None, None

    t = type(obj)
    # Exclude constant variables
    if obj is None or t is int or t is float or t is bool or t is dict or obj is EMPTY_TUPLE or t == type:
      return None, None
    elif t is str and len(obj) < 100: 
      return None, None

    identity = id(obj)
    print("referencing {}:{}\n".format(t, obj))
    if identity not in self.referers:
      first = pickle_id.len() == 0
      # New objects (case 1)
      prid, rid = pickle_id.next_reference(obj)
      self.referers[identity] = SyncReference(prid, obj, batch_id = pickle_id.batch_id, pickle_id = pickle_id)
      self.referers[prid.__prid__()] = self.referers[identity]  # Register PRID for later remote referencing.
      # No need to expand first object.
      if first:
        rid.__rid__() # Dummy call to be compatible with Profiler.  
        return None, prid
      return self._persistent_id_impl(rid, obj, prid=prid.__prid__()), prid
    elif id(self.referers[identity]) == id(SKIP_SYNC):
      # Referencing is disabled.
      return self._persistent_id_impl(None, obj), None
    elif self.referers[identity].batch_id != pickle_id.batch_id:
      # First touch in a batch
      self.referers[identity].batch_id = pickle_id.batch_id
      self.referers[identity].pickle_id = pickle_id
      prid, rid = pickle_id.next_reference(obj, prid=self.referers[identity].id)
      # print("Updated {}:{}, local {}, permanent id: {},{}".format(type(obj), obj, identity, prid, pickle_id.pcnt))
      return self._persistent_id_impl(rid, obj, prid=prid.__prid__()), prid
    elif self.referers[identity].pickle_id == pickle_id:
       # Within a pickle.dump call, leave pickle to deduplicate.
      return None, self.referers[identity].id
    else:
      # Cross pickle deduplication
      # if pickle_id is not None:
      #   print("Detected duplicated obj, type: {}, local id: {},{}, permanent id: {},{}".format(type(obj), identity, pickle_id, self.referers[identity].id, self.referers[identity].pickle_id))
        # print(obj)
      return self.referers[identity].permanent_reference(), self.referers[identity].id

  def _dereference(self, pickle_ref_id:Union[_T, str], buff: IO[bytes], prmap: Optional[list[SyncPRID]]=None):
    """persistent_load implementation for Unpickler. Register permanent refReturn obj on case 6."""
    # print("dereferencing {}".format(ref_id))

    if isinstance(pickle_ref_id, int):
      if prmap is None:
        raise ValueError("prmap is required for ref_id = tuple(rid, __class__, value).")
      
      obj, rid = self._persistent_load_impl((pickle_ref_id,), buff, prmap)
      prid = prmap[rid]
      self.register(prid, obj)
      self.referers[id(obj)] = self.referers[prid.__prid__()] # Register ID for later local referencing.
      # identity = id(obj)
      # print("Restore {}:{}, local {}, permanent id: {}".format(type(obj), obj, identity, self.referers[identity].id))
      return obj
    
    # reference
    if pickle_ref_id in self.referers:
      return self.referers[pickle_ref_id].obj
    
    # SyncRID is pickled as a persistent_id, so return directly.
    return pickle_ref_id
  
  def _persistent_id_impl(self, rid, obj, prid=None) -> Optional[Union[_T, str, int]]:
    """persistent_id heler."""
    ret = self._persistent_id_v3(rid, obj, prid=prid)
    # self._disable(ret)
    # logging.info("persistent_id {}:{} -> {}\n".format(type(obj), obj, ret))
    return ret
  
  def _persistent_load_impl(self, pickle_ref_id, buff: IO[bytes], prmap:list[SyncPRID]):
    """persistent_load helper."""
    return self._persistent_load_v3(pickle_ref_id, buff, prmap)

  def _persistent_id_v3(self, rid, obj, prid=None) -> Optional[Union[_T, str, int]]:
    """V2 persistent_id encoder enables references to __dict__ be kept if __dict__ is pickled.
    rid is put as the last element of the tuple as a guard to flag the end of the object pickle."""
    if rid is None:
      return None

    assert self.pickler is not None
    assert prid is not None

    # Transform stacked child objects to flat structure.
    self.pickler.dump(obj)

    # Return single element as persistent_id to avoid introducing new object in the internal stack of the pickle.
    # On return single element persistent_id, return the object directly.
    return rid.__rid__()

  def _persistent_load_v3(self, pickle_ref_id: _T, buff: IO[bytes], prmap:list[SyncPRID]):
    """V2 persistent_id decoder supports the reuse of the state decoded from ref_id."""
    # Preserve the current position.
    pos = buff.tell()
    rid = pickle_ref_id[0]

    # Seek to the position of the object and load.
    logging.info("Unpickling interrupted at {} for {}".format(pos, rid))
    buff.seek(prmap[rid]._offset)
    obj = self.unpickler.load()
    logging.info("Unpickled {}:{}".format(id(obj), obj))

    # Restore the position.
    buff.seek(pos)

    return obj, rid
