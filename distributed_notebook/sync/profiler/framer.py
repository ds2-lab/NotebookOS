import io
from struct import pack, unpack
from typing import Optional
import logging

FRAME            = b'\x95'  # indicate the beginning of a new frame

class _Framer:

    _FRAME_SIZE_MIN = 4
    _FRAME_SIZE_TARGET = 64 * 1024

    def __init__(self, file_write):
        self.file_write = file_write
        self.current_frame = None

        self.sequence = 0
        """The sequence to track frames created."""

        self.metaframes: list[tuple] = []
        """A stack of frames to support flattened frame tree in (id, frame)."""

        self.last_metaframe: Optional[tuple] = None
        """The last metaframe that was ended."""

        self.current_id = 0
        """The id of the current frame."""

    def start_framing(self):
        self.metaframes.append((self.current_id, self.current_frame)) # Add whatever regardless of None
        self.sequence += 1
        self.current_id = self.sequence
        self.current_frame = io.BytesIO()
        logging.info("Started frame {}".format(self.current_id))

    def end_framing(self):
        if self.current_frame and self.current_frame.tell() > 0:
            self.commit_frame(force=True)

        logging.info("Ended frame {}".format(self.current_id))
        self.last_metaframe = (self.current_id, self.current_frame) # Save the last metaframe
        self.current_id, self.current_frame = self.metaframes.pop() # current_frame can be None after this.

    def commit_frame(self, force=False):
        if self.current_frame:
            f = self.current_frame
            if f.tell() >= self._FRAME_SIZE_TARGET or force:
                data = f.getbuffer()
                write = self.file_write
                if len(data) >= self._FRAME_SIZE_MIN:
                    # Issue a single call to the write method of the underlying
                    # file object for the frame opcode with the size of the
                    # frame. The concatenation is expected to be less expensive
                    # than issuing an additional call to write.
                    write(FRAME + pack("<Q", len(data)))

                # Issue a separate call to write to append the frame
                # contents without concatenation to the above to avoid a
                # memory copy.
                write(data)
                logging.info("{} bytes: {}".format(len(bytearray(data)), bytearray(data)))

                # Start the new frame with a new io.BytesIO instance so that
                # the file object can have delayed access to the previous frame
                # contents via an unreleased memoryview of the previous
                # io.BytesIO instance.
                self.current_frame = io.BytesIO()

    def write(self, data):
        if self.current_frame:
            return self.current_frame.write(data)
        else:
            return self.file_write(data)

    def write_large_bytes(self, header, payload):
        write = self.file_write
        if self.current_frame:
            # Terminate the current frame and flush it to the file.
            self.commit_frame(force=True)

        # Perform direct write of the header and payload of the large binary
        # object. Be careful not to concatenate the header and the payload
        # prior to calling 'write' as we do not want to allocate a large
        # temporary bytes object.
        # We intentionally do not insert a protocol 4 frame opcode to make
        # it possible to optimize file.read calls in the loader.
        write(header)
        write(payload)