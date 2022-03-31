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