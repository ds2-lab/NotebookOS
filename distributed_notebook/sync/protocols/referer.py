from typing import Callable
from typing_extensions import Protocol, runtime_checkable

@runtime_checkable
class SyncRID(Protocol):
  """Interface for reference id (RID) that is used to identify an object."""

  def __rid__(self) -> int:
    """Get the raw local reference id """

class SyncRIDProvider(Callable[[any, int], SyncRID]):
  """Interface for RID provider that is used to generate RID from raw int presentation."""

@runtime_checkable
class SyncPRID(Protocol):
  """Interface for permanent reference id (PRID) that is used to identify an object."""
  
  def __prid__(self) -> str:
    """Get the PRID."""

  def raw(self) -> any:
    """Get the underlying object for persistence."""

class SyncPRIDProvider(Callable[[any], SyncPRID]):
  """Interface for PRID provider that is used to:
     1. Generate PRID from raw string presentation.
     2. Restore PRID returned from SyncPRID.raw()."""
  
@runtime_checkable
class SyncGrainWrapper(Protocol):
  """Interface for grain wrapper that is used to wrap fine-grained object."""

  def obj(self) -> any:
    """Get the wrapped obj."""

