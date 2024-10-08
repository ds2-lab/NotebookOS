import io
import logging
import uuid

from ..logging import ColoredLogFormatter
from ..smr.smr import NewBytes, ReadCloser

# TODO: Debug why, when reading from a read closer and we get to the end, it automatically loops back to the beginning.
class readCloser(io.IOBase):
  def __init__(self, rc: ReadCloser, size:int = io.DEFAULT_BUFFER_SIZE):
    self.rd = rc
    # The size is passed to prevent allocating a large buffer for fixed sized data.
    # No bigger than DEFAULT_BUFFER_SIZE buffer is allowed.
    if size > io.DEFAULT_BUFFER_SIZE:
      size = io.DEFAULT_BUFFER_SIZE
    self.buf = memoryview(bytearray(size))

    self.r = 0
    self.w = 0
    
    self.data_size:int = size 

    self.logger: logging.Logger = logging.getLogger(__class__.__name__ + "-" + str(uuid.uuid4())[0:5])

    self.logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(ColoredLogFormatter())
    self.logger.addHandler(ch)

    # buffered returns the number of buffered bytes
  def buffered(self) -> int:
    return self.w - self.r

  def peek(self, size=0) -> bytes:
    if size == 0:
      size = len(self.buf)
    
    # self.logger.debug(f"Peeking with size={size}, self.r={self.r}, self.w={self.w}, and len(self.buf)={len(self.buf)}")

    self.require(size)
    return bytes(self.buf[self.r:])

  def readinto(self, b):
    # self.logger.debug(f"Reading into with arg b={str(b)[0:200]}, self.r={self.r}, self.w={self.w}, and len(self.buf)={len(self.buf)}")
    ret = self.rd.Read(NewBytes(b))
    return ret.N

  def readline(self, size=-1) -> bytes:
    return super().readline(size)

  # peekline returns the next line until CRLF without reading it
  def read(self, size=-1) -> bytes:
    if size is None:
      size = -1
    
    self.logger.debug(f"reading data from readCloser now. size={size}")
    
    ret = bytearray()
    while size < 0 or len(ret) < size:
      read = self.read1(len(self.buf) if size < 0 else size - len(ret))
      if len(read) == 0:
        self.logger.debug(f"read 0 bytes from readCloser.")
        break

      ret.extend(read)
  
    self.logger.debug(f"returning {len(ret)} byte(s) from readCloser: {bytes(ret)}")
    return bytes(ret)

  def readall(self) -> bytes:
    return self.read()

  def read1(self, size=-1) -> bytes:
    if size is None:
      size = -1
    if size < 0:
      size = len(self.buf)
    
    self.logger.debug(f"Preparing to read buffer with size={size}, self.r={self.r}, self.w={self.w}, self.w - self.r = {self.w-self.r}, and len(self.buf)={len(self.buf)}")
    
    n = self.require(size)
    
    self.logger.debug(f"Reading buffer[{self.r}: {self.r + n}]; r = {self.r}, n = {n}.")

    if n == 0:
      return b''
    
    ret = bytes(self.buf[self.r:self.r+n])
    self.r += n
    return ret

  # require ensures that sz bytes are buffered
  def require(self, sz) -> int:
    self.logger.debug(f"require called with sz={sz}, self.r={self.r}, self.w={self.w}, self.w - self.r = {self.w-self.r}, len(self.buf)={len(self.buf)}, self.buffered = {self.buffered()}, and extra = {sz - self.buffered()}")
    extra = sz - self.buffered()
    if extra < 1:
      return sz
    #elif self.r == self.w == self.data_size:
    #  return 0 # Will cause read1() to return b''.

    # compact first
    self.compact()
    
    self.logger.debug(f"Buffering {sz} bytes now self.w={self.w}, self.r={self.r}.")
 
    # read data into buffer
    # this will modify self.buf 
    buf = self.buf[self.w:]
    ret = self.rd.Read(NewBytes(buf))
    self.w += ret.N
    
    self.logger.debug(f"Added {ret.N} to self.w; self.w is now = {self.w}, self.buffered()={self.buffered()}, sz={sz}")

    # print("before release sub buffer")
    # buf.release()
    return min(self.buffered(), sz)

  # compact moves the unread chunk to the beginning of the buffer
  def compact(self):
    if self.r > 0:
      self.logger.debug(f"Compacting buffer. Moving self.buf[{self.r}:{self.w}] to self.buf[:{self.buffered()}].")
      self.buf[:self.buffered()] = self.buf[self.r:self.w]
      self.w -= self.r
      self.r = 0
      self.logger.debug(f"After compaction, self.w={self.w}, and self.r={self.r}")

  def close(self):
    self.buf.release()
    self.rd.Close()