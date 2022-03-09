import io
import pickle
import hashlib
import sys
import types

EMPTY_TUPLE = ()

class SyncReference:
  def __init__(self, ref_id, obj, batch_id=None, pickle_id=None):
    self.id = ref_id
    self.obj = obj
    self.batch_id = batch_id    # Id of batch last touched.
    self.pickle_id = pickle_id  # Distinguish pickle barrier.

class SyncPickleId:
  def __init__(self, batch_id, pickle_count):
    self.batch_id = batch_id
    self.pcnt = pickle_count
    self.prmap = [] # A map to permanent reference id (PRID)

  def __call__(self, prid=None):
    """Generate PRID and reference id (RID)"""
    rid = len(self.prmap)
    if prid is None:
      prid = "{}-{}-{}".format(self.batch_id, self.pcnt, rid)
    self.prmap.append(prid)
    return prid, rid

  def dump(self):
    return self.prmap
    
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
    # Registry for objects. Each object has two entry: 
    # 1. Permanent Reference ID (RFID) for dereference remote alias.
    # 2. Local id() for alias detection.
    self.referers = {}

    self.last_pickle = 0

  def register(self, ref_id, obj):
    """Register deserialized obj with permanent reference id. Called on case 5, required for case 6."""
    self.referers[ref_id] = SyncReference(ref_id, obj)

  def prid(self, obj):
    """Query permanent reference id for first time serialized object. Called after case 1 for sychronizing."""
    _, prid = self._reference(obj)
    return prid

  def reference(self, batch_id):
    pickle_id = SyncPickleId(batch_id, self.last_pickle + 1)
    # print("start pickling {}...".format(pickle_id))
    self.last_pickle = pickle_id.pcnt
    _reference = self._reference

    def persistent_id(obj):
      ret, _ = _reference(obj, pickle_id=pickle_id)
      return ret

    return persistent_id, pickle_id

  def dereference(self, prmap):
    _dereference = self._dereference
    def persistent_load(ref_id):
      return _dereference(ref_id, prmap)

    return persistent_load

  def _reference(self, obj, pickle_id=None):
    """persistent_id implementation for Pickler. Return permanent reference id on case 2 and 4. Return None on case 1."""
    t = type(obj)
    # Exclude constant variables
    if obj is None or t is int or t is float or t is bool or obj is EMPTY_TUPLE or t is type or t is types.FunctionType:
      return None, None
    elif t is str and len(obj) < 100:
      return None, None

    identity = id(obj)
    _persistent_id = self._persistent_id_v2
    if identity not in self.referers:
      # New objects
      prid, rid = pickle_id()
      self.referers[identity] = SyncReference(prid, obj, batch_id = pickle_id.batch_id, pickle_id = pickle_id)
      self.referers[prid] = self.referers[identity]  # Register PRID for later remote referencing.
      # print("Registered {}:{}, local {}, permanent id: {},{}".format(type(obj), obj, identity, prid, pickle_id.pcnt))
      return _persistent_id(rid, obj), prid
    elif self.referers[identity] is None:
      # Referencing is disabled.
      return _persistent_id(None, obj), None
    elif self.referers[identity].batch_id != pickle_id.batch_id:
      # First touch in a batch
      self.referers[identity].batch_id = pickle_id.batch_id
      self.referers[identity].pickle_id = pickle_id
      prid, rid = pickle_id(prid=self.referers[identity].id)
      # print("Updated {}:{}, local {}, permanent id: {},{}".format(type(obj), obj, identity, prid, pickle_id.pcnt))
      return _persistent_id(rid, obj), prid
    elif self.referers[identity].pickle_id == pickle_id:
       # Within a pickle.dump call, leave pickle to deduplicate.
      return None, self.referers[identity].id
    else:
      # Cross pickle deduplication
      # if pickle_id is not None:
      #   print("Detected duplicated obj, type: {}, local id: {},{}, permanent id: {},{}".format(type(obj), identity, pickle_id, self.referers[identity].id, self.referers[identity].pickle_id))
        # print(obj)
      return self.referers[identity].id, self.referers[identity].id

  def _dereference(self, ref_id, prmap):
    """persistent_load implementation for Unpickler. Register permanent refReturn obj on case 6."""
    if isinstance(ref_id, tuple):
      rid, obj = self._persistent_load_v2(ref_id)
      prid = prmap[rid]
      self.register(prid, obj)
      self.referers[id(obj)] = self.referers[prid] # Register ID for later local referencing.
      identity = id(obj)
      # print("Restore {}:{}, local {}, permanent id: {}".format(type(obj), obj, identity, self.referers[identity].id))
      return obj
    
    # reference
    return self.referers[ref_id].obj

  def _persistent_id(self, rid, obj):
    """Default persistent_id encoder. Return object combined with in-pickle (RID) version of PRID"""
    if rid is None:
      return None
    return (rid, obj)

  def _persistent_load(self, ref_id):
    """Default persistent_id decoder."""
    return ref_id[0], ref_id[1]

  def _persistent_id_v2(self, rid, obj):
    """V2 persistent_id encoder enables references to __dict__ be kept if __dict__ is pickled."""
    if rid is None:
      # try:
      #   if hasattr(obj, "__dict__"):
      #     for o in obj.__dict__:
      #       self._disable(o)
      #   else:
      #     for o in obj:
      #       self._disable(o)
      # except TypeError:
      #   pass
      return None

    # Customized reducer invalidates the reuse of the state.
    if not hasattr(obj, "__dict__") or obj.__class__.__reduce__ is not object.__reduce__ or obj.__class__.__reduce_ex__ is not object.__reduce_ex__:
      return (rid, obj)
    
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
    return (rid, obj.__class__, val)

  def _persistent_load_v2(self, ref_id):
    """V2 persistent_id decoder supports the reuse of the state decoded from ref_id."""
    if len(ref_id) < 3:
      return ref_id[0], ref_id[1]

    inst = ref_id[1].__new__(ref_id[1])
    setstate = getattr(inst, "__setstate__", None)
    if setstate is not None:
      # Use unpickle dict instead of local dict.
      setstate(ref_id[2])
      # print("restore state of {},{}".format(ref_id[0], id(ref_id[2])))
    else:
      inst.__dict__ = ref_id[2]
      # print("restore dict of {},{}".format(ref_id[0], id(ref_id[2])))

    return ref_id[0], inst

  def _disable(self, obj):
    """Disable referencing for a temporary object."""
    self.referers[id(obj)] = None

  def _is_disabled(self, obj):
    """Check if referencing is disabled for an object."""
    return id(obj) in self.referers and self.referers[id(obj)] is None
