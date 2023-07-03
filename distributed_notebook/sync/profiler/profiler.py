from typing_extensions import Protocol, runtime_checkable

@runtime_checkable
class Profiler(Protocol):
  def mark_start_object(self, key, obj):
    """Mark the start of profiling an object."""

  def mark_end_object(self, key, obj):
    """Mark the end of profiling an object."""

def show_memo(msg: str, memo: dict):
  """Show the memo table."""
  # print(f"{msg} Memo:")
  for k, v in memo.items():
    if isinstance(v, str) or isinstance(v, int) or isinstance(v, tuple):
      print(f"  {k}: {v}")
    else:
      print(f"  {k}: {type(v)}")
