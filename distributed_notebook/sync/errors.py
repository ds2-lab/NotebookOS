import sys
import traceback

from distributed_notebook.sync.election import Election
from distributed_notebook.sync.log import SynchronizedValue
from typing import Optional

def print_trace(limit = 5):
  _, _, exc_traceback = sys.exc_info()
  traceback.print_tb(exc_traceback, limit=limit, file=sys.stdout)

class InconsistentTermNumberError(ValueError):
  """
  An InconsistentTermNumberError is raised when handling a committed value associated with a future election
  relative to the most recent location Election on this Node.

  Encountering an InconsistentTermNumberError suggests that this kernel may not be receiving some messages from
  its Local Daemon, or just that the messages are delayed and may arrive late (which is, in general, fine).
  """
  def __init__(self, message, election: Optional[Election] = None, value: Optional[SynchronizedValue] = None):
    # Call the base class constructor with the parameters it needs
    super().__init__(message)

    self.election: Optional[Election] = election
    self.value: Optional[SynchronizedValue] = value

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