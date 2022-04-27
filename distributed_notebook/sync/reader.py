import io
import ctypes

from ..smr.smr import NewBytes, Bytes, ReadCloser

class readCloser(io.IOBase):
  def __init__(self, rc: ReadCloser, size = io.DEFAULT_BUFFER_SIZE):
    self.rd = rc
    # The size is passed to prevent allocating a large buffer for fixed sized data.
    # No bigger than DEFAULT_BUFFER_SIZE buffer is allowed.
    if size > io.DEFAULT_BUFFER_SIZE:
      size = io.DEFAULT_BUFFER_SIZE
    self.buf = memoryview(bytearray(size))

    self.r = 0
    self.w = 0

    # buffered returns the number of buffered bytes
  def buffered(self) -> int:
    return self.w - self.r

  def peek(self, size=0) -> bytes:
    if size == 0:
      size = len(self.buf)

    self.require(size)
    return bytes(self.buf[self.r:])

  def readinto(self, b):
    buf = memoryview(b)
    ret = self.rd.Read(NewBytes(buf))
    buf.release()
    return ret.N

  def readline(self, size=-1) -> bytes:
    return super().readline(size)

  # peekline returns the next line until CRLF without reading it
  def read(self, size=-1) -> bytes:
    if size is None:
      size = -1
    
    ret = bytearray()
    while size < 0 or len(ret) < size:
      read = self.read1(len(self.buf) if size < 0 else size - len(ret))
      if len(read) == 0:
        break

      ret.extend(read)
  
    return bytes(ret)

  def readall(self) -> bytes:
    return self.read()

  def read1(self, size=-1) -> bytes:
    if size is None:
      size = -1
    if size < 0:
      size = len(self.buf)
    
    n = self.require(size)
    if n == 0:
      return b''

    ret = bytes(self.buf[self.r:self.r+n])
    self.r += n
    return ret

  # require ensures that sz bytes are buffered
  def require(self, sz) -> int:
    extra = sz - self.buffered()
    if extra < 1:
      return sz

    # compact first
    self.compact()

    # no grow allowed
 
    # read data into buffer
    buf = self.buf[self.w:]
    ret = self.rd.Read(NewBytes(buf))
    self.w += ret.N
    return min(self.buffered(), sz)

  # compact moves the unread chunk to the beginning of the buffer
  def compact(self):
    if self.r > 0:
      self.buf[:self.buffered()] = self.buf[self.r:self.w]
      self.w -= self.r
      self.r = 0

  def close(self):
    self.buf.release()
    self.rd.Close()