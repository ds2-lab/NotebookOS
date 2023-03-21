import sys
import re
from functools import reduce
from typing import Optional

from ipykernel.iostream import OutStream as OutStreamBase

no_output_patterns = [
  r'^\{"level":', # SMR Logs
  r'^raft[0-9]{4}/[0-9]{2}/[0-9]{2} ' # Raft Logs
]

class OutStream(OutStreamBase):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.disable = False

  def write(self, string: str) -> Optional[int]:
    if not isinstance(string, str):
      raise TypeError(
        f"write() argument must be str, not {type(string)}"
      )
    
    if not self.disable and reduce(lambda ret, pattern: ret and not re.match(pattern, string), no_output_patterns, True):
      return super().write(string)
    
    # Simply echo rest messages.
    if self.echo is not None:
      try:
        return self.echo.write(string)
      except OSError as e:
        if self.echo is not sys.__stderr__:
          print(f"Write failed: {e}",
                file=sys.__stderr__)