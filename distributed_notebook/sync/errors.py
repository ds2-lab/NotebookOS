import sys
import traceback

def print_trace():
  _, _, exc_traceback = sys.exc_info()
  traceback.print_tb(exc_traceback, limit=5, file=sys.stdout)

class SyncError(Exception):
  """Base exception class for errors generated in sync module."""
  def __init__(self, *args: object) -> None:
      super().__init__(*args)

def GoError(err):
  msg = ""
  if err is not None:
    msg = str(err)

  return str.encode(msg, "utf-8")

def GoNilError():
  return GoError(None)

def FromGoError(err):
  if err == "":
    return None
  else:
    return SyncError(err)