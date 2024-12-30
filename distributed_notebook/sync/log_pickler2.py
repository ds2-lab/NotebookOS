from typing import Any, IO, Callable, Optional
from pickle import _Pickler as Pickler
import logging

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
        return "{}({}) {} bytes, anchor: {}, {}".format(
            self.depth, self.frame_id, self.size, self.anchor, self.type
        )

    def __repr__(self) -> str:
        return str(self)

    def polyfill(self, prid: SyncPRID):
        if hasattr(prid, "_offset"):
            prid._offset = self.offset


class SyncLogSearchStack(list):
    """A queue of takes BFS input and output DFS traversal.
    Example: A1 -> (A1B1, A1B2) -> (A1B1C1, A1B1C2), (A1B2C1, A1B2C2)
    Step 1: A1(1)
    Step 2: A1B2, A1B1(2)
    Step 3: A1B2, A1B1C2, A1B1C1(3)
    Step 4: A1B2, A1B1C2(4)
    Step 5: A1B2(5)
    Step 6: A1B2C2, A1B2C1(6)
    Step 7: A1B2C2(7)
    Turns to: A1 -> A1B1 -> A1B1C1, A1B1C2 -> A1B2 -> A1B2C1, A1B2C2
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._current_branch: int = 0

    def pop(self) -> any:
        """Pop the next node to visit. The position will be marked and any node appended
        by BFS during visiting should be reversed after visited."""
        self._current_branch = len(self) - 1
        return super().pop()

    def done_pop(self):
        """Mark the current node as visited and reversed any node appended."""
        outstanding = len(self) - self._current_branch
        if outstanding > 1:  # At least 2 items in the stack.
            for i in range(0, outstanding // 2):  # Truncates
                self[self._current_branch + i], self[-i - 1] = (
                    self[-i - 1],
                    self[self._current_branch + i],
                )

    def inspect(self):
        logging.info("Inspecting stack: {}".format(list(map(lambda x: id(x), self))))


class SyncLogPickler(Pickler):
    def __init__(
        self, buffer: IO[bytes], protocol: Optional[int] = None, *args, **kwargs
    ):
        super().__init__(buffer, 5, *args, **kwargs)
        # logging.info("Init SyncLogPickler")

        self._buffer: IO[bytes] = buffer

        self._pickle_profile: dict[int, SyncLogPickleProfile] = {}
        """A dictionary mapping the id of the object to the size of pickled bytes."""

        self._queue: SyncLogSearchStack = SyncLogSearchStack([])
        """A queue of the objects used to track next object need to pickle."""

        self._dumping: any = None

    def dump(self, obj):
        """Dump an object and its sub-objects as flat frames for different isolation and different update."""

        # Enqueue whatsoever.
        self._queue.append(obj)
        # logging.info("Queued object: {}".format(id(obj)))

        # Abandon dumping and allow the dump() of root object scheduling the dumping.
        if len(self._queue) > 1 or self._dumping is not None:
            # logging.info("Delayed object: {}".format(id(obj)))
            return

        # Root object only, dump any sub-object added to the queue during dumping.
        # As long as queue is not empty, dump the next object: first in the queue.
        while len(self._queue) > 0:
            # We do not dequeue before dump completion to delay new object waiting until the current object is fully dumped.
            self._dumping = self._queue.pop()
            self._dump(self._dumping)
            self._dumping = None
            # Make done.
            self._queue.inspect()
            self._queue.done_pop()
            self._queue.inspect()

    def _dump(self, obj):
        """Write a pickled representation of obj to the open file. This version overrides the default dump method to embed the PROTO opcode in the frame."""

        obj_id = id(obj)
        # logging.info("Started object: {}, written {}: {}".format(obj_id, self._buffer.getbuffer().nbytes, obj))

        # create profile
        profile = SyncLogPickleProfile(self._buffer.getbuffer().nbytes, type(obj))
        print(
            f"Storing profile for {type(obj).__name__} object {obj} in _pickle_profile at key {id(obj)}. Profile: {profile}"
        )
        self._pickle_profile[id(obj)] = profile

        super().dump(obj)

        # update size
        profile.size = self._buffer.getbuffer().nbytes - profile.offset

        # logging.info("Dumped object: {}".format(obj_id))

    def get_polyfiller(
        self, cb: Callable[[SyncPRID], int]
    ) -> Callable[[SyncPRID], None]:
        def polyfiller(prid: SyncPRID):
            print("Polyfilling")

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

        # return lambda prid: self._pickle_profile[cb(prid)].polyfill(prid)
