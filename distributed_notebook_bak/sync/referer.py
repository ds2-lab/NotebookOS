from typing import Any, Optional, Union, Callable, IO
from types import BuiltinFunctionType, FunctionType
import pickle
import inspect
import logging

from .protocols import SyncRID, SyncRIDProvider, SyncPRID, SyncPRIDProvider

EMPTY_TUPLE = ()

class DefaultSyncRID(int):
  """Default implementation of SyncRID, only used for testing."""
  @staticmethod
  def wrap(obj: Any, rid: int) -> SyncRID:
    return DefaultSyncRID(rid)

  def __rid__(self) -> int:
    return int(self)
  
class DefaultSyncPRID(str):
  """Default implementation of SyncPRID."""
  @staticmethod
  def wrap(prid: str) -> SyncPRID:
    return DefaultSyncPRID(prid)

  def __prid__(self) -> str:
    return str(self)
  
  def raw(self) -> any:
    return str(self)
  
class _T(tuple):
  """Tuple used to represent an object internally."""

  def obj(self) -> any:
    if len(self) == 0:
      return None
    else:
      return self[0]
  
class SyncPickleId:
  """Instance of this class are used to uniquely identify a pickle dump process. 
  While the instance can be called to register or generate permanent reference id (PRID) for objects."""

  def __init__(self, batch_id: Optional[str], pickle_count: int, 
               prid_provider: SyncPRIDProvider,
               rid_provider: Optional[SyncRIDProvider]=DefaultSyncRID.wrap):
    self.batch_id: Optional[str] = batch_id
    """Id of the batch this pickle id belongs to. A batch is a set of objects that are serialized together."""

    self.pcnt: int = pickle_count
    """Short for pickle count, which is unique within a batch."""

    self._prmap: list[SyncPRID] = [] # A map to permanent reference id (PRID)
    """Map from reference id (RID, local unique) to permanent reference id (PRID, global unique)"""

    self._prid_provider: SyncPRIDProvider = prid_provider
    """A callback function that returns an object represents the PRID."""

    self._rid_provider: Optional[Callable[[Any, int], SyncRID]] = rid_provider
    """A callback function that takes a RID and returns an object represents the RID."""

  def len(self) -> int:
    """Return the number of objects that have been serialized."""
    return len(self._prmap)

  def next_reference(self, obj:Any, prid:SyncPRID=None) -> tuple[SyncPRID, Union[int, SyncRID]]:
    """Register PRID, generate one if prid is not available, which is usualy because we haven't seen the object during the batch"""
    rid = len(self._prmap)
    if self._rid_provider is not None:
      rid = self._rid_provider(obj, rid)
    # Generate PRID if not specified.
    if prid is None:
      prid = self._prid_provider("{}-{}-{}".format(self.batch_id, self.pcnt, rid))
    self._prmap.append(prid) # By appending, the rid that is used to query prid is the same as the index of prmap.
    return prid, rid

  def dump(self) -> tuple:
    """Dump the prmap to a tuple."""
    return tuple(map(lambda prid: prid.raw(), self._prmap))
  
  def load(self, prmap: tuple) -> list[SyncPRID]:
    """Load the prmap from a tuple."""
    return list(map(lambda prid: self._prid_provider(prid), prmap))
  
  def polyfill(self, polyfiller: Callable[[SyncPRID], None]):
    """Polyfill the prmap using the polyfiller function."""
    for i in range(len(self._prmap)):
      polyfiller(self._prmap[i])

class SyncReference:
  """Metadata for identifying an object."""

  def __init__(self, ref_id: SyncPRID, obj: Any, batch_id: Optional[str]=None, pickle_id: Optional[SyncPickleId]=None):
    self.id: SyncPRID = ref_id
    """Permanent reference id (PRID)"""

    self.obj: Any = obj
    """The object itself."""

    self.batch_id: Optional[str] = batch_id
    """Batch id that first serialized this object."""

    self.pickle_id: Optional[SyncPickleId] = pickle_id 
    """Pickle id that first serialized this object."""

    self.pickled: bool = False
    """Whether the object has been serialized."""

  def permanent_reference(self) -> str:
    """Return the permanent reference id (PRID) of the object."""
    return self.id.__prid__()

SKIP_SYNC = SyncReference("SKIP", None)
"""A special SyncReference that indicates the object should be skipped during serialization."""
    
class SyncReferer:
  """Used to deduplicate multiple references of the same object for 
  serializing/deserializing. Example use cases:
  1. Serializing, referenced object has never serialized.
  2. Serializing, referenced object has been serialized locally.
  3. Serializing, referenced object has been serialized as a part of other objects.
  4. Serializing, referenced object has been serialized remotely and restored locally.
  5. Deserializing, reference id and object has specified.
  6. Deserializing, reference id has specified."""
  
  def __init__(self):
    self.last_pickle: int = 0
    """Local id sequence for reference id (RID)"""

    self.referers: dict[Union[str,int], SyncReference] = {}
    """Registry for objects. Each object has two entry: 
    1. Permanent Reference ID (RFID) for dereference remote alias.
    2. Local id() for alias detection.

    If the value is SKIP_SYNC, the corresponding object will be skipped."""

    self.protocol: Optional[int] = None
    """Protocol version of the pickle. Used to determine whether the pickle is compatible."""

    self.pickler = None
    self.unpickler = None

    self._prid_provider: SyncPRIDProvider = DefaultSyncPRID.wrap

  def register(self, ref_id: SyncPRID, obj):
    """Register deserialized obj with permanent reference id. Called on case 5, required for case 6."""
    self.referers[ref_id.__prid__()] = SyncReference(ref_id, obj)

  # def prid(self, obj):
  #   """Query permanent reference id for first time serialized object. Called after case 1 for sychronizing."""
  #   _, prid = self._reference(obj)
  #   return prid

  def reference(self, batch_id:Optional[str], protocol:Optional[int]=None, rid_provider: Optional[SyncRIDProvider]=None, pickler=None):
    """Get pickle compatible persistent_id function. Corresponding pickle_id is returned,
     which should be stored together with pickled object."""

    # Generate pickle id
    pickle_id = SyncPickleId(batch_id, self.last_pickle + 1, self._prid_provider, rid_provider=rid_provider)
    # print("start pickling {}...".format(pickle_id))
    self.last_pickle = pickle_id.pcnt # Update pickle count
    if protocol is None:
      protocol = pickle.DEFAULT_PROTOCOL
    if protocol < 0:
      protocol = pickle.HIGHEST_PROTOCOL
    _reference = self._reference # Store a reference in function closure.
    if pickler is not None:
      self.pickler = pickler

    def persistent_id(obj):
      # print("pickling {}:{}".format(obj, type(obj)))
      self.protocol = protocol
      ret, _ = _reference(obj, pickle_id=pickle_id)
      # if ret is None:
      #   print("pickle as original")
      # elif len(ret) == 2:
      #   print("pickle as {}:{}".format(ret[1], type(ret[1])))
      # else:
      #   print("pickle as {}".format(ret))
      return ret

    return persistent_id, pickle_id

  def dereference(self, buff: IO[bytes], prmap:Optional[tuple], protocol:Optional[int]=None, unpickler=None):
    """Get pickle compatible persistent_load function. prmap must be deserialized and provided."""

    if protocol is None:
      protocol = pickle.DEFAULT_PROTOCOL
    if protocol < 0:
      protocol = pickle.HIGHEST_PROTOCOL

    _dereference = self._dereference # Store a reference in function closure.
    _normalized_prmap: Optional[list[SyncPRID]] = prmap
    # Normalize prmap if necessary
    if prmap is not None and len(prmap) > 0 and not isinstance(prmap[0], SyncPRID):
      _normalized_prmap = list(map(lambda raw: self._prid_provider(raw), prmap))
    # Override unpickler if necessary
    if unpickler is not None:
      self.unpickler = unpickler

    def persistent_load(pid):
      self.protocol = protocol
      return _dereference(pid, buff, _normalized_prmap)

    return persistent_load
  
  def object_from_prid(self, prid: SyncPRID):
    obj = self._dereference(prid.__prid__(), None, None)
    # _dereference returns prid if the object is not found.
    if isinstance(obj, str) and obj == prid.__prid__(): # test type first to avoid unnecessary string cast.
      obj = None
    return obj
  
  def id_from_prid(self, prid: SyncPRID):
    return id(self.object_from_prid(prid))

  def _reference(self, obj, pickle_id:SyncPickleId) -> tuple[Optional[Union[str, int, _T]], Optional[SyncPRID]]:
    """persistent_id implementation for Pickler. Return pickle compatible persistent ID and PRID."""
    if isinstance(obj, SyncRID):
      # For SyncRID, we must return the int as its pickle compatible persistent ID.
      return obj.__rid__(), None
    elif isinstance(obj, BuiltinFunctionType) or isinstance(obj, FunctionType) or inspect.isclass(obj) or isinstance(obj, tuple): 
      # Excluding tuple is essential for pickling SyncRID not falling into recursive error.
      return None, None

    t = type(obj)
    # Exclude constant variables
    if obj is None or t is int or t is float or t is bool or t is dict or t == type:
      return None, None
    elif t is str and len(obj) < 100: 
      return None, None

    identity = id(obj)
    # print("referencing {}:{}\n".format(t, obj))
    if identity not in self.referers:
      # New objects (case 1)
      prid, rid = pickle_id.next_reference(obj)
      self.referers[identity] = SyncReference(prid, obj, batch_id = pickle_id.batch_id, pickle_id = pickle_id)
      self.referers[prid.__prid__()] = self.referers[identity]  # Register PRID for later remote referencing.
      return self._persistent_id_impl(rid, obj), prid
    elif id(self.referers[identity]) == id(SKIP_SYNC):
      # Referencing is disabled.
      return self._persistent_id_impl(None, obj), None
    elif self.referers[identity].batch_id != pickle_id.batch_id:
      # First touch in a batch
      self.referers[identity].batch_id = pickle_id.batch_id
      self.referers[identity].pickle_id = pickle_id
      prid, rid = pickle_id.next_reference(obj, prid=self.referers[identity].id)
      # print("Updated {}:{}, local {}, permanent id: {},{}".format(type(obj), obj, identity, prid, pickle_id.pcnt))
      return self._persistent_id_impl(rid, obj), prid
    elif self.referers[identity].pickle_id == pickle_id:
      # Within a pickle.dump call, leave pickle to deduplicate.
      return None, self.referers[identity].id
    else:
      # Cross pickle deduplication
      # if pickle_id is not None:
      #   print("Detected duplicated obj, type: {}, local id: {},{}, permanent id: {},{}".format(type(obj), identity, pickle_id, self.referers[identity].id, self.referers[identity].pickle_id))
        # print(obj)
      return self.referers[identity].id, self.referers[identity].id

  def _dereference(self, pickle_ref_id:Union[_T, str], buff: IO[bytes], prmap: Optional[list[SyncPRID]]=None):
    """persistent_load implementation for Unpickler. Register permanent refReturn obj on case 6."""
    # print("dereferencing {}".format(ref_id))
    if isinstance(pickle_ref_id, _T):
      obj, rid = self._persistent_load_impl(pickle_ref_id)
      if prmap is None:
        raise ValueError("prmap is required for ref_id = tuple(rid, __class__, value).")
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
  
  def _persistent_id_impl(self, rid, obj) -> Optional[_T]:
    """persistent_id heler."""
    ret = self._persistent_id_v2(rid, obj)
    # self._disable(ret)
    # logging.info("persistent_id {}:{} -> {}\n".format(type(obj), obj, ret))
    return ret
  
  def _persistent_load_impl(self, pickle_ref_id):
    """persistent_load helper."""
    return self._persistent_load_v2(pickle_ref_id)

  def _persistent_id(self, rid, obj) -> Optional[_T]:
    """Default persistent_id encoder. Return object combined with in-pickle (RID) version of PRID"""
    if rid is None:
      return None
    return _T((obj, rid))

  def _persistent_load(self, pickle_ref_id):
    """Default persistent_id decoder."""
    return pickle_ref_id[0], pickle_ref_id[1]

  def _persistent_id_v2(self, rid, obj) -> Optional[_T]:
    """V2 persistent_id encoder enables references to __dict__ be kept if __dict__ is pickled.
    rid is put as the last element of the tuple as a guard to flag the end of the object pickle."""
    if rid is None:
      return None

    # Customized reducer invalidates the reuse of the state.
    # if not hasattr(obj, "__dict__") or obj.__class__.__reduce__ is not object.__reduce__ or obj.__class__.__reduce_ex__ is not object.__reduce_ex__:
    if not hasattr(obj, "__dict__") or getattr(obj, "__reduce_ex__", None) is not None or getattr(obj, "__reduce__", None) is not None:
      return _T((obj, rid))
    
    # If getstate is definded, use it.
    val = obj.__dict__
    getstate = getattr(obj, "__getstate__", None)
    if getstate is not None:
      val = getstate()
      # print("persisting state of {}:{}".format(rid, id(val)))
      # print("state:{}".format(val))
    else:
      # print("persisting dict of {}:{}".format(rid, id(val)))
      pass

    # setstate provides customized state recovery implementation and the reuse of state is not guranteed to work.
    setstate = getattr(obj, "__setstate__", None)
    if setstate is not None:
      self._disable(val)

    # While pickle will extract __dict__ or state of obj again, 
    # little overhead is incurred due to pickle's obj deduplication.
    return _T((obj.__class__, val, rid))

  def _persistent_load_v2(self, pickle_ref_id):
    """V2 persistent_id decoder supports the reuse of the state decoded from ref_id."""
    # print("before instanciation of {}".format(ref_id[1]))

    if len(pickle_ref_id) < 3:
      return pickle_ref_id[0], pickle_ref_id[1]

    inst = pickle_ref_id[0].__new__(pickle_ref_id[0])
    setstate = getattr(inst, "__setstate__", None)
    if setstate is not None:
      # Use unpickle dict instead of local dict.
      setstate(pickle_ref_id[1])
      # print("restore state of {},{}".format(ref_id[0], id(ref_id[2])))
    else:
      inst.__dict__ = pickle_ref_id[1]
      # print("restore dict of {},{}".format(ref_id[0], id(ref_id[2])))

    return inst, pickle_ref_id[2]

  def _disable(self, obj):
    """Disable referencing for a temporary object."""
    self.referers[id(obj)] = SKIP_SYNC

  def _is_disabled(self, obj):
    """Check if referencing is disabled for an object."""
    return id(obj) in self.referers and id(self.referers[id(obj)]) == id(SKIP_SYNC)
