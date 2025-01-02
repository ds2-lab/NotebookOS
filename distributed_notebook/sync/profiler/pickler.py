from typing import Any, IO, Callable, Optional
from pickle import _Pickler as Pickler
import logging

from .profiler import Profiler
from .framer import _Framer
from ..protocols import SyncPRID


class PickleProfile:
    def __init__(self, depth: int, frame_id: int, next_id: int, anchor: int = 0):
        self.offset: int = 0
        """The offset of the object in the root stream."""

        self.anchor: int = anchor
        """The anchor position of the object in the parent stream."""

        self.size: int = 0
        """The size of the object in bytes."""

        self.depth: int = depth
        """The depth of the object in the object graph."""

        self.frame_id: int = frame_id
        """The id of the frame that contains the object."""

        self.next_id: int = next_id
        """The id of the next next frame."""

        self.type: Any = None
        """Object type."""

    def __str__(self) -> str:
        return "{}({}) {} bytes, anchor: {}, {}".format(
            self.depth, self.frame_id, self.size, self.anchor, self.type
        )

    def __repr__(self) -> str:
        return str(self)

    def polyfill(self, prid: SyncPRID):
        if hasattr(prid, "_offset"):
            prid._offset = self.offset


class PickleFramer(_Framer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def tell(self, profile: Optional[PickleProfile] = None) -> int:
        if self.current_frame is None:
            return 0

        if profile is None:
            return self.current_frame.getbuffer().nbytes
        elif profile.frame_id != self.current_id:
            # profile is specified, try to locate the frame
            if profile.next_id == self.current_id:
                return self.current_frame.getbuffer().nbytes
            else:
                logging.warning(
                    "Unexpected frame: frame id {}, next id {}, current {}".format(
                        profile.frame_id, profile.next_id, self.current_id
                    )
                )
        else:
            return self.current_frame.getbuffer().nbytes - profile.anchor

        return 0

    def id(self) -> int:
        return self.current_id


class PickleProfileRefId:
    def __init__(self, pickler: Profiler, obj: Any, rid: int):
        self._profiler: Profiler = pickler
        self.obj = obj
        self.rid = rid
        self._profiler.mark_start_object(self.rid, self.obj)

    def __rid__(self):
        self._profiler.mark_end_object(self.rid, self.obj)
        return self.rid

    def __getstate__(self):
        return self.rid

    def __str__(self) -> str:
        return str(self.rid)


class PickleProfiler(Pickler):
    def __init__(
        self, buffer: IO[bytes], pickler: Optional[Pickler] = None, *args, **kwargs
    ):
        if pickler is not None:
            super().__setattr__("_parent", pickler)
        else:
            super().__setattr__("_parent", self)
            super().__init__(buffer, *args, **kwargs)

        logging.info("Init PickleProfiler")

        # Hacking properties
        self._parent.framer = PickleFramer(buffer.write)
        self._parent.write = self._parent.framer.write
        self._parent._write_large_bytes = self._parent.framer.write_large_bytes
        if hasattr(self._parent, "get_polyfiller"):
            super().__setattr__("get_polyfiller", self._parent.get_polyfiller)
        elif hasattr(self._parent, "_get_polifiller"):
            super().__setattr__("get_polyfiller", self._parent._get_polifiller)

        # Private profiler properties
        self._buffer: IO[bytes] = buffer
        """The buffer used to write the pickled bytes."""

        self._profile: dict[int, PickleProfile] = {}
        """A dictionary mapping the id of the object to the size of pickled bytes."""

        self._stack = []
        """A stack of the objects used to track object depth."""

    def get_reference_id(self, obj: Any, rid: int) -> PickleProfileRefId:
        """Get the reference id of the object."""
        return PickleProfileRefId(self, obj, rid)

    def mark_start_object(self, key, obj):
        """Mark the start of profiling an object."""
        self._stack.append(obj)
        profile = PickleProfile(
            len(self._stack),
            self._parent.framer.id(),
            next_id=self._parent.framer.sequence + 1,
            anchor=self._parent.framer.tell(),
        )
        profile.type = type(obj)
        self._profile[id(obj)] = profile
        logging.info("Profiled object start: {}".format(id(obj)))

    def mark_end_object(self, key, obj):
        """Mark the end of profiling an object."""
        self._stack.pop()
        profile = self._profile[id(obj)]
        if hasattr(self._parent, "_pickle_profile"):
            profile.size = self._parent._pickle_profile[id(obj)].size
        else:
            profile.size = self._parent.framer.tell(profile)
        logging.info("Profiled object end: {}".format(id(obj)))

    def __setattr__(self, __name: str, __value: Any) -> None:
        if self._parent != self and hasattr(self._parent, __name):
            self._parent.__setattr__(__name, __value)
        else:
            super().__setattr__(__name, __value)

    def dump(self, obj):
        self._parent.dump(obj)

    # def dump(self, obj_or_T):
    #   """Write a pickled representation of obj to the open file. This version overrides the default dump method to embed the PROTO opcode in the frame."""

    #   obj_id = id(obj_or_T)
    #   if isinstance(obj_or_T, SyncGrainWrapper):
    #     obj = obj_or_T.obj()
    #     if obj is not None:
    #       obj_id = id(obj)
    #   logging.info("Started object: {}, written {}: {}".format(obj_id, self._buffer.getbuffer().nbytes, obj_or_T))

    #   # Check whether Pickler was initialized correctly. This is
    #   # only needed to mimic the behavior of _pickle.Pickler.dump().
    #   if not hasattr(self, "_file_write"):
    #       raise PicklingError("Pickler.__init__() was not called by "
    #                           "%s.__init__()" % (self.__class__.__name__,))

    #   if self.proto < 4:
    #       raise PicklingError("pickle protocol must be >= 4")

    #   self._parent.framer.start_framing()
    #   self.write(PROTO + pack("<B", self.proto))
    #   self.save(obj_or_T)
    #   self.write(STOP)
    #   logging.info("Dumping object: {} at {}".format(obj_id, self._buffer.getbuffer().nbytes))
    #   # logging.info("memo: {}".format(self.memo))
    #   if obj_id in self._profile:
    #     self._profile[obj_id].offset = self._buffer.getbuffer().nbytes # Record the offset of the object in root stream just before flushing.
    #   self._parent.framer.end_framing()

    #   logging.info("Dumped object: {}".format(obj_id))

    def _get_polifiller(
        self, cb: Callable[[SyncPRID], int]
    ) -> Callable[[SyncPRID], None]:
        return lambda prid: self._profile[cb(prid)].polyfill(prid)

    def print_profile(self, cb: Callable[[SyncPRID], int], prmap: list[SyncPRID]):
        """Get the profile of the pickled objects.

        Args:
          cb: A callback function that takes a PRID and returns the id of the object.
          prmap: A list of PRID that represent objects.

        Returns:
          A dictionary mapping the PRID to the profile of the pickled object.
        """
        # profile: dict[str, PickleProfile] = {}
        logging.info("Pickled profile: ")
        depth = 1
        indent = ""
        for i, pr in enumerate(prmap):
            pf = self._profile[cb(pr)]
            if pf.depth > depth:
                indent += "  "
            elif pf.depth < depth:
                indent = indent[:-2]
            depth = pf.depth
            logging.info("{}{}: {}".format(indent, pr, pf))
            # profile[pr] = self._profile[cb(pr)]
        # return profile
