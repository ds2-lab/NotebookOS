import sys

from ipykernel.iostream import OutStream as OutStreamBase

class OutStream(OutStreamBase):
  def __init__(self, *args, **kwargs):
    print("In OutStream.__init__")
    super().__init__(*args, **kwargs)
    self.disable = False

  def write(self, string: str) -> int:
    if not self.disable:
      return super().write(string)

    if not isinstance(string, str):
      raise TypeError(
        f"write() argument must be str, not {type(string)}"
      )

    if self.echo is not None:
      try:
        return self.echo.write(string)
      except OSError as e:
        if self.echo is not sys.__stderr__:
          print(f"Write failed: {e}",
                file=sys.__stderr__)
        return 0