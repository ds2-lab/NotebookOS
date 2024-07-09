from typing import Tuple, Callable, Optional, Any, Iterable, Dict, List
from typing_extensions import Protocol, runtime_checkable
from enum import Enum 

import time 
import datetime
import uuid

KEY_FAILURE = "_failure_" # We cannot execute this request... 
KEY_NONE:str = ""
KEY_SYNC_END:str = "_end_"
OP_SYNC_ADD:str = "add"
OP_SYNC_PUT:str = "put"
OP_SYNC_DEL:str = "del"
OP_NONE:str = "" 

class ElectionProposalKey(Enum):
  YIELD = "_yield_" # Propose to yield the execution to another replica.
  LEAD = "_lead_"   # Propose to lead the execution term (i.e., execute the user's code).
  SYNC = "_sync_"   # Synchronize to confirm decision about who is executing the code.

class SynchronizedValue(object):
  """
  Base class for a value for log proposal.

  This is an updated/rewritten version of the SyncValue class. 
  """

  def __init__(
      self,
      tag: Any,
      data: Any, # The value/data attached to the proposal.
      proposer_id: int = -1, # The SMR node ID of the node proposing this value.
      attempt_number: int = -1, # Serves as a sort of "sub-term", as elections can be re-tried if they fail (i.e., if everyone proposes "YIELD")
      election_term: int = -1, # The election term on which this value is intended to be proposed.
      prmap: Optional[list[str]] = None,
      should_end_execution: bool = False,
      key: str = KEY_NONE,
      operation:str = OP_NONE, 
  ):
    self._tag: Any = tag
    self._proposer_id: int = proposer_id
    self._election_term: int = election_term
    self._attempt_number: int = attempt_number
    self._data: Any = data 
    self._id: str = str(uuid.uuid4())
    self._timestamp: float = time.time() 
    self._operation: str = operation
    
    self._should_end_execution: bool = should_end_execution
    self._prmap: Optional[list[str]] = prmap
    self._key: str = key 

  @property 
  def tag(self)->Any:
    return self._tag

  @property
  def key(self)->str:
    return self._key 
  
  def set_key(self, key:str)->None: 
    self._key = key 

  @property 
  def should_end_execution(self)->bool:
    return self._should_end_execution 
  
  def set_should_end_execution(self, should_end_execution:bool)->None:
    self._should_end_execution = should_end_execution

  @property 
  def prmap(self)->Optional[list[str]]:
    return self._prmap 
  
  def set_prmap(self, prmap: Optional[list[str]])->None:
    self._prmap = prmap 

  @property 
  def has_operation(self)->bool:
    """
    True if the SynchronizedValue has an explicitly-defined operation (i.e., the `self._operation` field is not the empty string).
    """
    return len(self.operation) > 0 

  @property 
  def op(self)->str:
    """
    Alias of `self.operation` 

    The named operation of the synchronized value, which may be the empty string.
    """
    return self.operation 

  @property 
  def operation(self)->str:
    """
    The named operation of the synchronized value, which may be the empty string.
    """
    return self._operation 

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
  
  def set_data(self, data:Any)->None:
    self._data = data 
  
  @property 
  def election_term(self)->int:
    return self._election_term 
  
  def set_election_term(self, term)->None:
    self._election_term = term
  
  @property 
  def attempt_number(self)->int:
    return self._attempt_number

class LeaderElectionProposal(SynchronizedValue):
  """
  A special type of SynchronizedValue encapsulating a "LEAD" or "YIELD" proposal during a leader election.
  """
  def __init__(self, **kwargs):
    if "key" not in kwargs:
      raise ValueError("Must specify a \"key\" keyword argument when creating an instance of `LeaderElectionProposal`")
    
    # LeaderElectionProposals cannot have data. 
    super().__init__(None, None, **kwargs)

  @property 
  def is_lead(self)->bool:
    return self._key == str(ElectionProposalKey.LEAD)

  @property 
  def is_yield(self)->bool:
    return self._key == str(ElectionProposalKey.YIELD)

  @property 
  def election_proposal_key(self)->str:
    """
    Alias for `self.key`.
    
    The "key" of this proposal, which indicates whether it is a LEAD or a YIELD proposal.
    """
    return self._key 

class LeaderElectionVote(SynchronizedValue):
  """
  A special type of SynchronizedValue encapsulating a vote for a leader during a leader election.

  These elections occur when code is submitted for execution by the user.

  This used to be 'SYNC' in the original RaftLog implementation.
  """
  def __init__(self, proposed_node_id: int, **kwargs):
    if "key" in kwargs:
      _key = kwargs.pop("key")

      if _key != str(ElectionProposalKey.SYNC):
        raise ValueError(f"the \"key\" keyword argument must be equal to \"{ElectionProposalKey.SYNC}\" for `LeaderElectionVote`. This is handled automatically; no need to pass a \"key\" keyword argument.")
    else:
      kwargs["key"] = str(ElectionProposalKey.SYNC)
      
    super().__init__(None, None, **kwargs)

    # The SMR node ID of the node being voted for
    self._proposed_node_id:int = proposed_node_id
  
  @property 
  def proposed_node_id(self)->int:
    """
    The SMR node ID of the node being voted for
    """
    return self._proposed_node_id 

  @property 
  def election_succeeded(self)->bool:
    """
    True if a particular node was selected/voted for (rather than all nodes proposing 'YIELD', which would constitute a failure/failed election.)
    Basically, True if at least one node proposed 'LEAD', and False if all nodes proposed 'YIELD'.
    """
    return self._proposed_node_id != -1 

  @property 
  def election_failed(self)->bool:
    """
    True if all nodes proposed 'YIELD'.
    False if at least one node proposed 'LEAD'.
    """
    return self._proposed_node_id == -1 

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

  async def try_yield_execution(self, term) -> bool:
    """Request yield the update of a term to another replica."""

  async def try_lead_execution(self, term) -> bool:
    """Request to lead the update of a term. A following append call 
       without leading status will fail."""
    return False

  async def append(self, val: SynchronizedValue):
    """Append the difference of the value of specified key to the synchronization queue."""

  def sync(self, term):
    """Manually trigger the synchronization of changes since specified term."""

  def reset(self, term, logs: Tuple[SynchronizedValue]):
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

