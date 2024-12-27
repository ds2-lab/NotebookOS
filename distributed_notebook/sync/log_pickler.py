from typing import Any, IO, Callable
from pickle import _Pickler as Pickler, PicklingError, PROTO, STOP
import logging
from struct import pack

from .profiler.framer import _Framer
from .protocols import SyncPRID


class SyncLogPickleProfile:
    def __init__(self, offset: int, type: Any):
        self.offset: int = offset
        """The offset of the object in the root stream."""

        self.size: int = 0
        """The size of the object in bytes."""

        self.type: Any = type
        """Object type."""

    def __str__(self) -> str:
        return f"SyncLogPickleProfile[offset={self.offset}, size={self.size}, type={self.type}]"
        # return "{}({}) {} bytes, anchor: {}, {}".format(
        #     self.depth, self.frame_id, self.size, self.anchor, self.type
        # )

    def __repr__(self) -> str:
        return str(self)

    def polyfill(self, prid: SyncPRID):
        if hasattr(prid, "_offset"):
            prid._offset = self.offset


class SyncLogPickler(Pickler):
    def __init__(self, buffer: IO[bytes], *args, **kwargs):
        super().__init__(buffer, *args, **kwargs)
        logging.info("Init SyncLogPickler")

        # Hacking properties
        self.framer = _Framer(buffer.write)
        self.write = self.framer.write
        self._write_large_bytes = self.framer.write_large_bytes

        # Private profiler properties
        self._buffer: IO[bytes] = buffer
        """The buffer used to write the pickled bytes."""

        self._pickle_profile: dict[int, SyncLogPickleProfile] = {}
        """A dictionary mapping the id of the object to the size of pickled bytes."""

    def dump(self, obj):
        """Write a pickled representation of obj to the open file. This version overrides the default dump method to embed the PROTO opcode in the frame."""

        obj_id = id(obj)
        logging.info(
            "Started object: {}, written {}: {}".format(
                obj_id, self._buffer.getbuffer().nbytes, obj
            )
        )

        profile = SyncLogPickleProfile(self._buffer.getbuffer().nbytes, type(obj))
        print(
            f"Storing profile for {type(obj).__name__} object {obj} in _pickle_profile at key {id(obj)}. Profile: {profile}"
        )
        self._pickle_profile[id(obj)] = profile

        # Check whether Pickler was initialized correctly. This is
        # only needed to mimic the behavior of _pickle.Pickler.dump().
        if not hasattr(self, "_file_write"):
            raise PicklingError(
                "Pickler.__init__() was not called by "
                "%s.__init__()" % (self.__class__.__name__,)
            )

        if self.proto < 4:
            raise PicklingError("pickle protocol must be >= 4")

        self.framer.start_framing()
        self.write(PROTO + pack("<B", self.proto))
        self.save(obj)
        self.write(STOP)
        logging.info(
            "Dumping object: {} at {}".format(obj_id, self._buffer.getbuffer().nbytes)
        )
        # logging.info("memo: {}".format(self.memo))
        self._pickle_profile[id(obj)].offset = self._buffer.getbuffer().nbytes
        self.framer.end_framing()

        # update size
        profile.size = self._buffer.getbuffer().nbytes - profile.offset

        logging.info("Dumped object: {}".format(obj_id))

    def get_polyfiller(
        self, cb: Callable[[SyncPRID], int]
    ) -> Callable[[SyncPRID], None]:
        def polyfiller(prid: SyncPRID):
            print(f'Converting prid "{prid}" to key')

            _key = cb(prid)

            print(f'Converted prid "{prid}" to key "{_key}" using cb {cb}')

            if _key not in self._pickle_profile:
                raise KeyError(
                    f'invalid key "{_key}". valid "pickle profile" keys: {self._pickle_profile.keys()}'
                )

            profile: SyncLogPickleProfile = self._pickle_profile[_key]

            print(f'SyncLogPickleProfile for SyncPRID "{prid}": {profile}')

            profile.polyfill(prid)

        return polyfiller
