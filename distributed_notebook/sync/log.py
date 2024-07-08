from typing import Tuple, Callable, Optional, Any, Iterable, Dict, List
from typing_extensions import Protocol, runtime_checkable
import time 
import datetime
import uuid 

KEY_SYNC_END = "_end_"
OP_SYNC_ADD = "add"
OP_SYNC_PUT = "put"
OP_SYNC_DEL = "del"

class ProposedValue(object):
  """
  A value for log proposal.

  This is an updated/rewritten version of the SyncValue class. 
  """

  def __init__(
      self,
      data: Any, # The value/data attached to the proposal.
      proposer_id: int = -1, # The SMR node ID of the node proposing this value.
      attempt_number: int = -1, # Serves as a sort of "sub-term", as elections can be re-tried if they fail (i.e., if everyone proposes "YIELD")
      election_term: int = -1, # The election term on which this value is intended to be proposed.
  ):
    self._proposer_id = proposer_id
    self._election_term: int = election_term
    self._attempt_number: int = attempt_number
    self._data:Any = data 
    self._id:str = str(uuid.uuid4())
    self._timestamp = time.time() 

  @property 
  def proposer_id(self)->int:
    """
    # The SMR node ID of the node proposing this value.
    """
    return self._proposer_id 

  @property
  def timestamp(self)->float:
    """
    The timestamp at which this proposal was created.
    """
    return self._timestamp

  @property 
  def ts(self)->float:
    """
    Alias of `self.timestamp`

    The timestamp at which this proposal was created.
    """
    return self.timestamp  

  @property 
  def id(self)->str:
      """
      Return the UUID v4 uniquely identifying this proposed value.
      """
      return self._id 
    
  @property 
  def data(self)->Any:
    return self._data 
  
  @property 
  def election_term(self)->int:
    return self._election_term 
  
  @property 
  def attempt_number(self)->int:
    return self._attempt_number

class LeaderElectionVote(ProposedValue):
  """
  A special type of ProposedValue encapsulating a vote for a leader during a leader election.

  These elections occur when code is submitted for execution by the user.
  """
  def __init__(
      self,
      data: Any, # The value/data attached to the proposal.
      election_term: int = -1, # The election term on which this value is intended to be proposed.
      proposed_node_id: int = -1, # The SMR node ID of the node being voted for.
  ):
    super().__init__(data, election_term = election_term)

    self._proposed_node_id:int = proposed_node_id
  
  @property 
  def proposed_node_id(self)->int:
    return self._proposed_node_id 

class SyncValue:
  """A value for log proposal."""

  def __init__(self, tag, val: Any, term:int=0, proposed_node:Optional[int] = -1, timestamp:Optional[float] = time.time(), key:Optional[str]=None, op:Optional[str]=None, prmap:Optional[list[str]]=None, end:bool=False, attempt_number:int = -1):
    self.term:int = term
    self.key:str = key
    self.prmap = prmap
    self.tag = tag
    self.val = val
    self.attempt_number = attempt_number
    self.proposed_node = proposed_node # Only used by 'SYNC' proposals to specify the node that should serve as the leader.
    self.end:bool = end
    self.op:str = op
    self.timestamp:float = timestamp # The time at which the proposal/value was issued.

    self._reset: bool = False
  
  def __str__(self)->str:
    ts:str = datetime.datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
    return "SyncValue[Key='%s',Term=%d,Timestamp='%s',Tag='%s',Val='%s']" % (self.key, self.term, ts, str(self.tag), str(self.val)[0:25])
  
  @property
  def reset(self):
    return self._reset

@runtime_checkable
class SyncLog(Protocol):
  @property
  def num_changes(self) -> int: # type: ignore
    """The number of incremental changes since first set or the latest checkpoint."""

  @property
  def term(self) -> int: # type: ignore
    """Current term."""

  def start(self, handler):
    """Register change handler, restore internel states, and start monitoring changes. 
      handler will be in the form listerner(key, val: SyncValue)"""

  def set_should_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
      callback will be in the form callback(SyncLog) bool"""

  def set_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides to checkpoint.
      callback will be in the form callback(Checkpointer)."""

  async def yield_execution(self, term) -> bool:
    """Request yield the update of a term to another replica."""

  async def lead(self, term) -> bool:
    """Request to lead the update of a term. A following append call 
       without leading status will fail."""
    return False

  async def append(self, val: SyncValue):
    """Append the difference of the value of specified key to the synchronization queue."""

  def sync(self, term):
    """Manually trigger the synchronization of changes since specified term."""

  def reset(self, term, logs: Tuple[SyncValue]):
    """Clear logs equal and before specified term and replaced with specified logs"""

  def close(self):
    """Ensure all async coroutines end and clean up."""

@runtime_checkable
class Checkpointer(Protocol):
  @property
  def num_changes(self) -> int:
    """The number of values checkpointed."""
    return 0

  def lead(self, term) -> bool:
    """Set the term to checkpoint. False if any error."""
    return False
       
  async def append(self, val: SyncValue):
    """Append the value of specified key to the writer."""

  def close(self):
    """Ensure all async coroutines end and clean up."""

