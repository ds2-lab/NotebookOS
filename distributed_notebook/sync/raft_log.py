import io
import os
import base64
import pickle
import asyncio
import json
import logging
import time
from typing import Tuple, Callable, Optional, Any, Iterable, Dict, List
import datetime

from ..smr.smr import NewLogNode, NewConfig, NewBytes, WriteCloser, ReadCloser, PrintTestMessage
from ..smr.go import Slice_string, Slice_int, Slice_byte
from .log import SyncLog, SyncValue
from .checkpoint import Checkpoint
from .future import Future
from .errors import print_trace, SyncError, GoError, GoNilError
from .reader import readCloser
from .file_log import FileLog

KEY_LEAD = "_lead_" # Propose to lead the execution term (i.e., execute the user's code).
KEY_YIELD = "_yield_" # Propose to yield the execution to another replica.
KEY_SYNC = "_sync_" # Synchronize to confirm decision about who is executing the code.
KEY_FAILURE = "_failure_" # We cannot execute this request... 

MAX_MEMORY_OBJECT = 1024 * 1024

class writeCloser:
  def __init__(self, wc: WriteCloser):
    self.wc = wc

  def write(self, b):
    self.wc.Write(NewBytes(b))

  def close(self):
    self.wc.Close()

class OffloadPath:
  def __init__(self, path: str):
    self.path = path

  def __str__(self):
    return self.path

class RaftLog:
  """A log that stores the changes of a python object."""
  _leader_term: int = 0   # Mar 2023: Term will updated after a lead() call. For now, we don't care if the jupyter's execution_count, which is equal to the term, is continuous or not.
  _leader_id: int = 0     # The id of the leader.
  _expected_term: int = 0 # The term that the leader is expecting.
  _future_loop: asyncio.AbstractEventLoop = None
  _leading: Optional[asyncio.Future[bool]] = None # A future that is set when _leader_id is decided.
  _decisionProposalFuture: Optional[asyncio.Future[SyncValue]] = None # A future that is set when we have a decision to propose.
  _start_loop: asyncio.AbstractEventLoop # The loop that start() is called.
  _async_loop: asyncio.AbstractEventLoop # The loop that the async functions are running on.
  _handler: Callable[[SyncValue], None] # The handler to be called when a change is committed.
  _shouldSnapshotCallback: Optional[Callable[[SyncLog], bool]] = None # The callback to be called when a snapshot is needed.
  _snapshotCallback: Optional[Callable[[Any], bytes]] = None # The callback to be called when a snapshot is needed.
  _catchingUpAfterMigration: bool = False # If true, then the "remote updates" that we're receiving are us catching up to where we were before a migration/eviction was triggered.

  def __init__(self, base_path: str, id: int, hdfs_hostname:str, data_directory:str, peer_addrs: Iterable[str], peer_ids: Iterable[int], join: bool = False, debug_port:int = 8464):
    self._log: logging.Logger = logging.getLogger(__class__.__name__ + str(id))
    self._log.info("Creating RaftNode %d now." % id)

    self._store: str = base_path
    self._id: int = id
    self.ensure_path(self._store)
    self._offloader: FileLog = FileLog(self._store)
    
    if len(hdfs_hostname) == 0:
      raise ValueError("HDFS hostname is empty.")

    self._log.info("_store: %s" % self._store)
    self._log.info("hdfs_hostname: \"%s\"" % hdfs_hostname)
    self._log.info("data_directory: \"%s\"" % data_directory)
    self._log.info("peer_addrs: %s" % peer_addrs)
    self._log.info("peer_ids: %s" % peer_ids)
    self._log.info("join: %s" % join)
    self._log.info("debug_port: %d" % debug_port)

    self._log.info("Creating LogNode %d now." % id)

    self._log.info("Actually creating LogNode %d now." % id)

    self._node = NewLogNode(self._store, id, hdfs_hostname, data_directory, Slice_string(peer_addrs), Slice_int(peer_ids), join, debug_port)
    if self._node == None:
      raise RuntimeError("Failed to create LogNode.")

    self.my_current_attempt_number : int = 1 # Attached to proposals. Sort of an ID within an election term. 
    self.winners_per_term: Dict[int, int] = {} # Mapping from term number -> SMR node ID of the winner of that term.
    self.my_proposals: Dict[int, Dict[int, SyncValue]] = {} # Mapping from term number -> Dict. The inner map is attempt number -> proposal.
    self.my_current_attempt_number:int = 1 # The current attempt number for the current term. 
    self.largest_peer_attempt_number:Dict[int, int] = {0:0} # The largest attempt number received from a peer's proposal.
    self.proposals_per_term: Dict[int, Dict[int, SyncValue]] = {} # Mapping from term number -> dict. Inner dict is map from SMR node ID -> proposal.
    self.own_proposal_times: Dict[int, float] = {} # Mapping from term number -> the time at which we proposed LEAD/YIELD in that term.
    self.first_lead_proposal_received_per_term: Dict[int, SyncValue] = {} # Mapping from term number -> the first 'LEAD' proposal received in that term.
    self.first_proposal_received_per_term: Dict[int, SyncValue] = {} # Mapping from term number -> the first proposal received in that term.
    self.timeout_durations: Dict[int, float] = {} # Mapping from term number -> the timeout (in seconds) for that term.
    self.discard_after: Dict[int, float] = {} # Mapping from term number -> the time after which received proposals will be discarded.
    self.num_proposals_discarded: Dict[int, int] = {} # Mapping from term number -> the number of proposals that were discarded in that term.
    self.sync_proposals_per_term: Dict[int, SyncValue] = {} # Mapping from term number -> the first SYNC proposal committed during that term.
    self.decisions_proposed: Dict[int, bool] = {} # Mapping from term number -> boolean flag indicating whether we've proposed (but not necessarily committed) a decision for the given term yet.

    if self._node == None:
      self._log.error("Failed to create the LogNode.")
      raise ValueError("Could not create LogNode. See logs for details.")
    elif not self._node.ConnectedToHDFS():
      self._log.error("The LogNode failed to connect to HDFS.")
      raise RuntimeError("The LogNode failed to connect to HDFS")

    self._log.info("Successfully created LogNode %d." % id)

    self._changeCallback = self._changeHandler  # No idea why this walkaround works
    self._restoreCallback = self._restore       # No idea why this walkaround works
    self._ignore_changes = 0
    self._closed = None
    
    self.__load_and_apply_serialized_state()

  def __get_serialized_state(self) -> bytes:
    """
    Serialize important state so that it can be written to HDFS (for recovery purposes).
    
    This return value of this function should be passed to the `self._node.WriteDataDirectoryToHDFS` function.
    """        
    data_dict:dict = {
      "winners_per_term": self.winners_per_term,
      "my_proposals": self.my_proposals,
      "my_current_attempt_number": self.my_current_attempt_number,
      "largest_peer_attempt_number": self.largest_peer_attempt_number,
      "proposals_per_term": self.proposals_per_term,
      "own_proposal_times": self.own_proposal_times,
      "first_lead_proposal_received_per_term": self.first_lead_proposal_received_per_term,
      "first_proposal_received_per_term": self.first_proposal_received_per_term,
      "timeout_durations": self.timeout_durations,
      "discard_after": self.discard_after,
      "num_proposals_discarded": self.num_proposals_discarded,
      "sync_proposals_per_term": self.sync_proposals_per_term,
      "decisions_proposed": self.decisions_proposed,
      "_leader_term": self._leader_term,
      "_leader_id": self._leader_id,
      "_expected_term": self._expected_term,
    }
    
    return pickle.dumps(data_dict)

  def __load_and_apply_serialized_state(self) -> None:
    """
    Retrieve the serialized state read by the Go-level LogNode. 
    This state is read from HDFS during migration/error recovery.
    Update our local state with the state retrieved from HDFS.
    """
    serialized_state_bytes:bytes = bytes(self._node.GetSerializedState()) # Convert the Go bytes (Slice_byte) to Python bytes.
    
    if len(serialized_state_bytes) == 0:
      self._log.debug("No serialized state found. Nothing to load and apply.")
      return 
    
    data_dict:dict = pickle.loads(serialized_state_bytes) # json.loads(serialized_state_json)
    if len(data_dict) == 0:
      self._log.debug("No serialized state found. Nothing to apply.")
      return 
    
    for key, entry in data_dict.items():
      self._log.debug(f"Retrived state \"{key}\": {str(entry)}")
      
    # TODO: 
    # There may be some bugs that arrise from these values being somewhat old or outdated, potentially.
    self.winners_per_term = data_dict["winners_per_term"]
    self.my_proposals = data_dict["my_proposals"]
    self.my_current_attempt_number = data_dict["my_current_attempt_number"]
    self.largest_peer_attempt_number = data_dict["largest_peer_attempt_number"]
    self.proposals_per_term = data_dict["proposals_per_term"]
    self.own_proposal_times = data_dict["own_proposal_times"]
    self.first_lead_proposal_received_per_term = data_dict["first_lead_proposal_received_per_term"]
    self.first_proposal_received_per_term = data_dict["first_proposal_received_per_term"]
    self.timeout_durations = data_dict["timeout_durations"]
    self.discard_after = data_dict["discard_after"]
    self.num_proposals_discarded = data_dict["num_proposals_discarded"]
    self.sync_proposals_per_term = data_dict["sync_proposals_per_term"]
    self.decisions_proposed = data_dict["decisions_proposed"]
    
    # If true, then the "remote updates" that we're receiving are us catching up to where we were before a migration/eviction was triggered.
    self._catchingUpAfterMigration = True 

    # The value of _leader_term before a migration/eviction was triggered.
    self.leader_term_before_migration: int = data_dict["_leader_term"]

    # Commenting these out for now; it's not clear if we should set these in this way yet.
    # self._leader_term = data_dict["_leader_term"]
    # self._leader_id = data_dict["_leader_id"]
    # self._expected_term = data_dict["_expected_term"]

  @property
  def num_changes(self) -> int:
    """The number of incremental changes since first term or the latest checkpoint."""
    return self._node.NumChanges() - self._ignore_changes

  @property
  def term(self) -> int:
    """Current term."""
    return self._leader_term

  def start(self, handler:Callable[[SyncValue], None]):
    """Register change handler, restore internel states, and start monitoring changes, """
    self._handler = handler

    config = NewConfig()
    config.ElectionTick = 10
    config.HeartbeatTick = 1
    config = config.WithChangeCallback(self._changeCallback).WithRestoreCallback(self._restoreCallback)
    if self._shouldSnapshotCallback is not None:
      config = config.WithShouldSnapshotCallback(self._shouldSnapshotCallback)
    if self._snapshotCallback is not None:
      config = config.WithSnapshotCallback(self._snapshotCallback)

    self._log.debug("Starting LogNode now.")

    self._async_loop = asyncio.get_running_loop()
    self._start_loop = self._async_loop

    startSuccessful: bool = self._node.Start(config)
    if not startSuccessful:
      self._log.error("Failed to start LogNode.")
      raise RuntimeError("failed to start the Golang-level LogNode component")
    self._log.info("Started LogNode.")
    self._log.info("Started RaftLog.")

  def _printIOLoopInformationDebug(self):
    """Print some debug information about the IO loops."""
    self._log.debug("self._start_loop.is_closed: %s, self._start_loop.is_running: %s" % (str(self._start_loop.is_closed()), str(self._start_loop.is_running())))
    self._log.debug("self._async_loop.is_closed: %s, self._async_loop.is_running: %s" % (str(self._async_loop.is_closed()), str(self._async_loop.is_running())))
    self._log.debug("self._start_loop == self._async_loop: %s" % str(self._start_loop == self._async_loop))
    
    # Calling `asyncio.get_running_loop()` will throw a RuntimeError if there is no running event loop.
    # We call this function from non-async contexts sometimes, so this may occur.
    try:
      self._log.debug("self._start_loop == asyncio.get_running_loop(): %s" % str(self._start_loop == asyncio.get_running_loop()))
      self._log.debug("self._async_loop == asyncio.get_running_loop(): %s" % str(self._async_loop == asyncio.get_running_loop()))
    except RuntimeError:
      pass 
  
  def _isProposal(self, syncval:SyncValue):
    return syncval.key == KEY_LEAD or syncval.key == KEY_YIELD or syncval.key == KEY_SYNC

  def _handleProposal(self, proposal: SyncValue, received_at: float = time.time()) -> bytes:
    """Handle a committed LEAD/YIELD proposal.

    Args:
        proposal (SyncValue): the committed proposal.
        received_at (float): the time at which we received this proposal.
    """
    discarded:bool = False
    
    # 'SYNC' proposals are handled a little differently.
    # We don't want them to be counted with the other proposals.
    if proposal.key == KEY_SYNC:
      return self._handleSyncProposal(proposal)

    time_str = datetime.datetime.fromtimestamp(proposal.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
    term:int = proposal.term 
    self._log.debug("Received {} from node {}. Term {}, attempt {}, timestamp {} ({}), match {}...".format(proposal.key, proposal.val, term, proposal.attempt_number, proposal.timestamp, time_str, self._id == proposal.val))

    if proposal.attempt_number > self.largest_peer_attempt_number.get(term, 0):
      self._log.debug("Received proposal from node %d for term %d with new largest attempt number of %d. Previous largest: %d.", proposal.val, term, proposal.attempt_number, self.largest_peer_attempt_number.get(term, -1))
      self.largest_peer_attempt_number[term] = proposal.attempt_number
      
      # Now we need to purge any proposals we've received this term with lower attempt numbers.
      proposals = self.proposals_per_term.get(term, {})
      toRemove:List = [] 
      
      for node_id, p in proposals.items():
        if p.attempt_number < proposal.attempt_number:
          self._log.debug("Purging proposal from node %d (term=%d) with attempt number %d." % (p.val, p.term, p.attempt_number))
          toRemove.append(node_id)
      
      for node_id in toRemove:
        del proposals[node_id]
      
      self._log.debug("Purged a total of %d existing proposal(s) for term %d." % (len(toRemove), term))
      
      self.proposals_per_term[term] = proposals
    elif proposal.attempt_number < self.largest_peer_attempt_number.get(term, 0):
      self._log.warn("Proposal received from Node %d has attempt number %d, which is lower than the largest attempt number we've seen for this term (%d for term %d). Discarding proposal.", proposal.val, proposal.attempt_number, self.largest_peer_attempt_number.get(term, -1), term)
      discarded = True 
    
    # If we've already discarded the proposal due to its attempt number being low, then we won't bother storing it.
    if not discarded:
      if term in self.proposals_per_term:
        proposals = self.proposals_per_term.get(term, {})

        if proposal.val in proposals:
          prev_proposal:SyncValue = proposals[proposal.val]
          
          # If the proposal that we just received has an attempt number that is less than the last proposal -- or an attempt number that is equal to the last proposal's attempt number,
          # then this is bad. It's possible that the same node will propose a new value for a future execution attempt, such as if the first execution failed due to all replicas proposing
          # 'YIELD'. In this case, the new proposal should have a larger atttempt number.
          if proposal.attempt_number <= prev_proposal.attempt_number:
            self._log.error("Received multiple proposals from Node %d in term %d. New proposal has attempt number (%d) <= previous proposal (%d)." % (proposal.val, term, proposal.attempt_number, prev_proposal.attempt_number))
            # For now, we'll just replace the previous proposal with the same attempt number with this new one.
            # TODO (Ben): Fix this. This happens during migrations.
            proposals[proposal.val] = proposal
            # raise ValueError("Received multiple proposals from Node %d in term %d. New proposal has attempt number (%d) <= previous proposal (%d)." % (proposal.val, term, proposal.attempt_number, prev_proposal.attempt_number))
          else:
            self._log.debug("Received multiple proposals from Node %d in term %d. New proposal's attempt number (%d) > previous proposal's attempt number (%d)." % (proposal.val, term, proposal.attempt_number, prev_proposal.attempt_number))
            proposals[proposal.val] = proposal
        else:
          proposals[proposal.val] = proposal

        self.proposals_per_term[term] = proposals
      else:
        self.proposals_per_term[term] = {
          # 'proposal.val' is the SMR node ID of whoever proposed this.
          proposal.val: proposal
        }

    numProposalsReceived:int = len(self.proposals_per_term.get(term, {}))
    self._log.debug("Received %d proposal(s) in term %d so far." % (numProposalsReceived, term))

    first_proposal: SyncValue = self.first_proposal_received_per_term.get(term, None)

    # If this is the first 'LEAD' proposal we're receiving in this term, then take note of the time.
    # Alternatively, if we receive a new proposal with a higher attempt number, then that must mean another round of proposals has begun.
    # In this case, we'll reset the 'discard' timer and treat this newly-received proposal as the "first" proposal. 
    if first_proposal is None or first_proposal.attempt_number < proposal.attempt_number: # if numProposalsReceived == 1:
      timeout: float = min(5 * (time.time() - proposal.timestamp), 10) # Timeout of 10 seconds at-most.
      self.timeout_durations[term] = timeout
      self.discard_after[term] = proposal.timestamp + timeout
      self.first_proposal_received_per_term[term] = proposal
      first_proposal = proposal 

      self._log.debug("Timeout for term %d will be %.6f seconds. Will discard any proposals received after time %s." % (term, timeout, datetime.datetime.fromtimestamp(self.discard_after[term]).strftime('%Y-%m-%d %H:%M:%S.%f')))
    elif first_proposal is not None and received_at > self.discard_after[term]:
      # If we've received at least one 'LEAD' proposal, then the timeout has been set, so we check if we need to discard this proposal.
      self._log.debug("Term %d proposal from node %d was received after timeout. Will be discarding." % (term, proposal.val))

      # Increment the 'num-discarded' counter.
      num_discarded: int = self.num_proposals_discarded.get(term, 0)
      self.num_proposals_discarded[term] = num_discarded + 1
      discarded = True

    # Check if this is the first 'LEAD' proposal we're receiving this term.
    # If so, and if we're not discarding the proposal, then record that it is the first 'LEAD' proposal.
    if not discarded and proposal.key == KEY_LEAD and self.first_lead_proposal_received_per_term.get(term, None) == None:
      self._log.debug("'%s' proposal from Node %d is first LEAD proposal in term %d." % (proposal.key, proposal.val, term))
      self.first_lead_proposal_received_per_term[term] = proposal

    self._ignore_changes = self._ignore_changes + 1
    return self._makeDecision(term)

    # Default to '_handleOtherProposal' in case of an erroneous key field.
    # return self.proposal_handlers.get(proposal.key, self._handleOtherProposal)(proposal)

  def _makeDecision(self, term: int) -> bytes:
    """Make a decision on who should execute the code for the given term.

    This should be called after all 3 proposals are collected, or after we've collected N proposals and discarded the others due to timeouts.
    """
    if self.decisions_proposed.get(term, False):
      self._log.debug("We've already proposed a decision for term %d." % term)
      return GoNilError()

    self._log.debug("Preparing to make decision proposal for term %d." % term)
    num_proposals:int = len(self.proposals_per_term[term])
    num_discarded:int = self.num_proposals_discarded.get(term, 0)
    winningProposal: SyncValue | None = None

    if num_proposals == 0:
      self._log.error("Erroneously found that we've received 0 proposals so far in term %d." % term)
      raise ValueError("Erroneously found that we've received 0 proposals so far in term %d." % term)

    # If we've not yet received enough proposals to make a decision -- including discarded proposals -- then we will stop.
    # TODO(Ben): We should also probably verify that we're not outside the "discard window", meaning enough time has passed that we'll 
    # just discard any future proposals, in which case we should make a decision anyway. 
    # TODO(Ben): We need a way to make a decision if we receive 2 proposals and then don't receive anymore (before the discard window has elapsed), as we'll just stall forever in this case.
    if (num_proposals + num_discarded) < 3:
      self._log.debug("We've not received enough proposals yet to propose a decision (received: %d, discarded: %d)." % (num_proposals, num_discarded))
      return GoNilError()

    self._log.debug("Received enough proposals to propose a decision (received: %d, discarded: %d)." % (num_proposals, num_discarded))
    # First, check if the winner of the last term issued a LEAD proposal this term.
    # If they did, then we'll propose a decision that they get to lead again.
    # Otherwise, accept the first LEAD proposal of the term.
    last_winner_id:int | None = self.winners_per_term.get(term - 1, None)
    if last_winner_id is not None:
      last_term_proposals: Dict[int, SyncValue] = self.proposals_per_term.get(term, None)
      last_winner_proposal:SyncValue = last_term_proposals.get(last_winner_id, None)

      if last_winner_proposal != None:
        self._log.debug("last_winner_proposal: %s" % str(last_winner_proposal))
        self._log.debug("Last term (%d) winner, node %d, proposed '%s'." % (term - 1, last_winner_id, last_winner_proposal.key))

        if last_winner_proposal.key == KEY_LEAD:
          self._log.debug("Proposing decision that last term (%d) winner, node %d, leads this term (%d) as well." % (term-1, last_winner_id, term))

          winningProposal = last_winner_proposal
        else:
          self._log.debug("Will propose first 'LEAD' proposal of this term (%d) as leader." % term)
      else:
        self._log.warn("No proposal received from previous term (%d) winner, node %d." % (term - 1, last_winner_id))
    else:
      self._log.warn("I don't know who won the last term...")
      
    # If the winning proposal is still done, then we aren't defaulting to the previous term's leader.
    # Get the first 'LEAD' proposal that we received this term.
    if winningProposal == None:
      self._log.debug("Proposing first 'LEAD' proposal that we received as the winner...")
      winningProposal = self.first_lead_proposal_received_per_term.get(term, None)

    # If the winning proposal is still None, then we just didn't receive any 'LEAD' proposals this term.
    # Depending on the scheduling policy, we'll either dynamically create a new replica, or we'll migrate existing replicas.
    if winningProposal == None:
      self._log.warn("We didn't receive any 'LEAD' proposals during term %d. Proposing failure." % term)
    else:
      self._log.debug("Will propose that Node %d lead execution for term %d." % (winningProposal.val, winningProposal.term))
    # self._printIOLoopInformationDebug()
    
    # Propose the second-round confirmation.
    # self._async_loop.call_soon_threadsafe(self.append_decision, SyncValue(None, self._id, proposed_node = winningProposal.val, timestamp = time.time(), term=winningProposal.term, key=KEY_SYNC))
    if self._decisionProposalFuture is not None:
      self._scheduleDecision(term, winningProposal) # winningProposal will be None if no replicas proposed 'LEAD'.
    else:
      self._log.debug("Cannot schedule the setting of result on _decisionProposalFuture now; it is None.")
    
    return GoNilError()

  def _scheduleDecision(self, term:int, winningProposal: SyncValue): 
    """
    Called by _makeDecision. This schedules the setting of the result on the _decisionProposalFuture.
    """
    self._log.debug("Scheduling the setting of result on _decisionProposalFuture now.")
    if winningProposal is not None:
      assert winningProposal.term == term 
      self._future_loop.call_soon_threadsafe(self._decisionProposalFuture.set_result, SyncValue(None, self._id, proposed_node = winningProposal.val, timestamp = time.time(), term=term, key=KEY_SYNC, attempt_number=self.my_current_attempt_number))
    else:
      self._future_loop.call_soon_threadsafe(self._decisionProposalFuture.set_result, SyncValue(None, self._id, proposed_node = -1, timestamp = time.time(), term=term, key=KEY_FAILURE, attempt_number=self.my_current_attempt_number))
    self._decisionProposalFuture = None # Make sure it is only set once.
    self._log.debug("Scheduled the setting of result on _decisionProposalFuture now.")
    
  def _handleSyncProposal(self, proposal: SyncValue) -> bytes:
    """Handle a committed 'SYNC' (KEY_SYNC) proposal.

    Args:
        proposal (SyncValue): The SyncVal that was commtited as part of the proposal. 
    """
    self._log.debug("Received 'SYNC' proposal from Node %d in term %s proposing that Node %d wins." % (proposal.val, proposal.term, proposal.proposed_node))

    if self.sync_proposals_per_term.get(proposal.term, None) != None:
      self._log.debug("We've already received a 'SYNC' proposal during term %d. Ignoring." % proposal.term)
      return GoNilError()

    self.sync_proposals_per_term[proposal.term] = proposal

    if self._leader_term < proposal.term:
      self._log.debug("Our 'leader_term' (%d) < 'leader_term' of latest committed 'SYNC' (%d). Setting our 'leader_term' to %d and the 'leader_id' to %d (from newly-committed value)." % (self._leader_term, proposal.term, proposal.term, proposal.proposed_node))
      self._leader_term = proposal.term
      self._leader_id = proposal.proposed_node

      self.winners_per_term[proposal.term] = proposal.proposed_node
      self._log.debug("Node %d has won in term %d as proposed by node %d." % (proposal.proposed_node, proposal.term, proposal.val))
    else:
      self._log.debug("Our leader_term (%d) >= 'leader_term' of latest committed 'SYNC' message (%d)..." % (self._leader_term, proposal.term))
    
    # Set the future if the term is expected.
    _leading = self._leading
    if _leading is not None and self._leader_term >= self._expected_term:
      self._log.debug("leader_term=%d, expected_term=%d. Scheduling the setting of result on '_leading' future to %d." % (self._leader_term, self._expected_term, self._leader_term))
      self._future_loop.call_later(0, _leading.set_result, self._leader_term)
      self._leading = None # Ensure the future is set only once.
      self._log.debug("Scheduled setting of result on '_leading' future.")
    else:
      self._log.debug("Skipping setting result on _leading. _leading is None: %s. self._leader_term (%d) >= self._expected_term (%d): %s." % (self._leading == None, self._leader_term, self._expected_term, self._leader_term >= self._expected_term))

    self._ignore_changes = self._ignore_changes + 1

    return GoNilError()

  def _handleOtherProposal(self, proposal: SyncValue) -> bytes:
    """
    Called if we receive a proposal whose key is unsupported. This is basically an error-handler.
    """
    self._log.error("Received proposal with unknown/unsupported key: '%s'" % proposal.key)
    raise ValueError("received proposal with unsupported key '%s'" % proposal.key)

  def _changeHandler(self, rc, sz: int, id: str) -> bytes:
    received_at:float = time.time()

    if id != "":
      self._log.debug("Our proposal {} of size {} bytes was committed.".format(id, sz))
    else:
      self._log.debug("Received remote update of size {} bytes".format(sz))

    reader = readCloser(ReadCloser(handle=rc), sz)
    try:
      syncval:SyncValue = pickle.load(reader)

      if self._isProposal(syncval):
        return self._handleProposal(syncval, received_at = received_at)
      elif id != "":
        # Skip state updates from current node.
        return GoNilError()

      self._log.debug("Setting _leader_term (currently %d) to %d." % (self._leader_term, syncval.term))
      old_leader_term:int = self._leader_term
      self._leader_term = syncval.term

      if self._catchingUpAfterMigration:
        # Check if we've caught-up now.
        if self._leader_term >= self.leader_term_before_migration:
          self._catchingUpAfterMigration = False 
          self._log.debug(f"We're done catching up. Current and previous leader term are both equal to {self._leader_term}.")
        else:
          self._log.debug(f"We're not done catching-up yet. Previous leader term: {self.leader_term_before_migration}, current leader term: {self._leader_term}.")
      
      # For values synchronized from other replicas or replayed, count _ignore_changes
      if syncval.op is None or syncval.op == "":
        self._ignore_changes = self._ignore_changes + 1
      self._handler(self._load(syncval))

      return GoNilError()
    except Exception as ex:
      self._log.error("Failed to handle change: {}".format(ex))
      print_trace(limit = 10)
      raise ex
      # return GoError(ex)
    # pickle will close the reader
    # finally:
    #   reader.close()

  def _restore(self, rc, sz) -> bytes:
    self._log.debug("Restoring...")

    reader = readCloser(ReadCloser(handle=rc), sz)
    unpickler = pickle.Unpickler(reader)

    syncval = None
    try:
      syncval = unpickler.load()
    except Exception:
      pass

    # Recount _ignore_changes
    self._ignore_changes = 0
    restored = 0
    while syncval is not None:
      try:
        self._handler(self._load(syncval))
        restored = restored + 1

        syncval = None
        syncval = unpickler.load()
      except SyncError as se:
        self._log.error("Error on restoreing snapshot: {}".format(se))
        return GoError(se)
      except Exception:
        pass

    self._log.debug("Restored {}".format(restored))
    return GoNilError()

  def set_should_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
      callback will be in the form callback(SyncLog) bool"""
    if callback is None:
      self._shouldSnapshotCallback = None
      return

    def shouldSnapshotCallback(logNode):
      # Initialize object using LogNode(handle=logNode) if neccessary.
      # print("in direct shouldSnapshotCallback")
      return callback(self)

    self._shouldSnapshotCallback = shouldSnapshotCallback

  def set_checkpoint_callback(self, callback):
    """Set the callback that will be called when the SyncLog decides to checkpoint.
      callback will be in the form callback(Checkpointer)."""
    if callback is None:
      self._snapshotCallback = None
      return

    def snapshotCallback(wc) -> bytes:
      try:
        checkpointer = Checkpoint(writeCloser(WriteCloser(handle=wc)))
        callback(checkpointer)
        # Reset _ignore_changes
        self._ignore_changes = 0
        return GoNilError()
      except Exception as e:
        self._log.error("Error on snapshoting: {}".format(e))
        return GoError(e)

    self._snapshotCallback = snapshotCallback

  async def lead(self, term) -> bool:
    """
    Request to lead the update of a term. A following append call without leading status will fail.
    """
    self._log.debug("RaftLog %d is proposing to lead term %d. Current leader term: %d." % (self._id, term, self._leader_term))
    is_leading:bool = await self.handle_election(term, SyncValue(None, self._id, timestamp = time.time(), term=term, key=KEY_LEAD, attempt_number=self.my_current_attempt_number))
    self._log.debug("RaftLog %d: returning from lead for term %d after waiting, is_leading=%s" % (self._id, term, str(is_leading)))

    # TODO(Ben):
    # Is there one singular place I could have this happen?
    self.my_current_attempt_number = 1 # Reset the current attempt number. 
    self.largest_peer_attempt_number[self._leader_term] = 0 # Reset the current "largest peer" attempt number. 

    return is_leading

  async def yield_execution(self, term) -> bool:
    """
    Request to yield the update of a term. A following append call without leading status will fail.
    """
    self._log.debug("RaftLog %d: proposing to yield term %d. Current leader term: %d." % (self._id, term, self._leader_term))
    is_leading:bool = await self.handle_election(term, SyncValue(-1, self._id, timestamp = time.time(), term=term, key=KEY_YIELD, attempt_number=self.my_current_attempt_number))
    assert is_leading == False
    self._log.debug("RaftLog %d: returning from yield_execution for term %d." % (self._id, term))

    # TODO(Ben):
    # Is there one singular place I could have this happen?
    self.my_current_attempt_number = 1 # Reset the current attempt number. 
    self.largest_peer_attempt_number[self._leader_term] = 0 # Reset the current "largest peer" attempt number. 

    return False 

  async def handle_election(self, term, proposal: SyncValue):
    """ The procedure for "handling an election" is roughly the same when proposing 'YIELD' and 'LEAD'.
    
    The primary difference is what value we propose. We also necessarily expect NOT to lead when proposing 'YIELD'.
    
    Other than that, the steps are identical.

    Args:
        term (_type_): _description_
        proposal (SyncValue): _description_
    """
    self._log.debug("RaftLog %d: Handling election for term %d now. Will be proposing '%s'.", self._id, term, proposal.key)
    if term == 0:
      term = self._leader_term + 1
    elif term <= self._leader_term:
      self._log.warn("Trying to lead term %d, but _leader_term is %d...", term, self._leader_term)
      return False

    # Define the _leading future
    self._expected_term = term
    self._future_loop = asyncio.get_running_loop()
    self._decisionProposalFuture = self._future_loop.create_future() 
    _decisionProposalFuture = self._decisionProposalFuture
    self._leading = self._future_loop.create_future()
    _leading = self._leading
    self.own_proposal_times[term] = time.time()
    self._log.debug("RaftLog %d: appending %s proposal for term %d. Attempt number: (proposal=%d,local_var=%d)." % (self._id, proposal.key, term, proposal.attempt_number, self.my_current_attempt_number))
    
    # Append is blocking. We are guaranteed to gain leading status if terms match.
    await self.append(proposal)
    self._log.debug("RaftLog %d: appended %s proposal for term %d. Attempt number: (proposal=%d,local_var=%d)" % (self._id, proposal.key, term, proposal.attempt_number, self.my_current_attempt_number))
    
    decisionProposal: SyncValue = await _decisionProposalFuture
    self._decisionProposalFuture = None 
    
    my_term_proposals: Dict[int, SyncValue] = self.my_proposals.get(term, {})
    my_term_proposals[decisionProposal.attempt_number] = decisionProposal
    self.my_current_attempt_number += 1 # Now that we have our proposal, we'll increment the attempt number in case we propose something else again soon.
    self.my_proposals[term] = my_term_proposals

    if decisionProposal.key == KEY_SYNC:
      self._log.debug("RaftLog %d: Got decision to propose: I think Node %d should win in term %d." % (self._id, decisionProposal.proposed_node, decisionProposal.term))
    elif decisionProposal.key == KEY_FAILURE:
      self._log.debug("RaftLog %d: Got decision to propose: election failed. No replicas proposed 'LEAD'." % self._id)
      
      # None of the replicas proposed 'LEAD'
      # It is likely that a migration of some sort will be triggered as a result, leading to another election round for this term.

      return False 
    else:
      raise ValueError("Unexpected key on decision proposal: \"%s\"", decisionProposal.key)
    
    if decisionProposal.term != term:
      raise ValueError("Received decision proposal with different term: %d. Expected: %d." % (decisionProposal.term, term))
    
    self._log.debug("RaftLog %d: Appending decision proposal for term %s now." % (self._id, decisionProposal.term))
    await self.append_decision(decisionProposal)
    self._log.debug("RaftLog %d: Successfully appended decision proposal for term %s now." % (self._id, decisionProposal.term))
    self.decisions_proposed[term] = True

    # Validate the term
    wait, is_leading = self._is_leading(term)
    if not wait:
      self._log.debug("RaftLog %d: returning for term %d without waiting, is_leading=%s" % (self._id, term, str(is_leading)))
      return is_leading
    
    # Wait for the future to be set.
    self._log.debug("waiting on _leading Future to be resolved.")
    await _leading
    self._log.debug("Successfully waited for self._leading.")
    self._leading = None

    # Validate the term
    wait, is_leading = self._is_leading(term)
    assert wait == False
    return is_leading

  def _is_leading(self, term) -> Tuple[bool, bool]:
    """Check if the current node is leading, return (wait, is_leading)"""
    if self._leader_term > term:
      return False, False
    elif self._leader_term == term:
      return False, self._leader_id == self._id
    else:
      return True, False

  async def append_decision(self, val: SyncValue):
    """Append the difference of the value of specified key to the synchronization queue

    This method is specifically used for proposing/appending 'SYNC' proposals.
    """
    # Ensure key is specified and is 'SYNC'.
    if val.key != KEY_SYNC:
      raise ValueError("Cannot append value with key '%s' using `append_decision` method." % val.key)

    self._log.debug("Proposing/appending 'SYNC' proposal for term %d, node %d.", val.term, val.val)

    if val.op is None or val.op == "":
      # Count _ignore_changes
      self._ignore_changes = self._ignore_changes + 1

    if val.val is not None and type(val.val) is bytes and len(val.val) > MAX_MEMORY_OBJECT:
      val = await self._offload(val)

    # Serialize the value.
    dumped = pickle.dumps(val)

    # Propose and wait the future.
    future, resolve = self._get_callback()
    self._node.Propose(NewBytes(dumped), resolve, val.key)
    await future.result()

  async def append(self, val: SyncValue):
    """Append the difference of the value of specified key to the synchronization queue"""
    if val.key != KEY_LEAD and val.key != KEY_YIELD:
      self._log.debug("[Append] setting _leader_term (currently %d) to %d." % (self._leader_term, val.term))
      self._leader_term = val.term
      self._log.debug("[Append] Resetting attempt number and largest peer attempt number.")
      self.my_current_attempt_number = 1 # Reset the current attempt number. 
      self.largest_peer_attempt_number[self._leader_term] = 0 # Reset the current "largest peer" attempt number. 
    
    if val.op is None or val.op == "":
      # Count _ignore_changes
      self._ignore_changes = self._ignore_changes + 1

    # Ensure key is specified.
    if val.key is not None:
      if val.val is not None and type(val.val) is bytes and len(val.val) > MAX_MEMORY_OBJECT:
        val = await self._offload(val)

      # Serialize the value.
      dumped = pickle.dumps(val)

      # Propose and wait the future.
      future, resolve = self._get_callback()
      self._node.Propose(NewBytes(dumped), resolve, val.key)
      await future.result()

  def sync(self, term):
    """Synchronization changes since specified execution counter."""
    pass

  def reset(self, term, logs: Tuple[SyncValue]):
    """Clear logs equal and before specified term and replaced with specified logs"""
    pass

  async def add_node(self, node_id, address):
    """Add a node to the cluster."""
    self._log.info("Adding node %d at addr %s to the SMR cluster." % (node_id, address))
    future, resolve = self._get_callback()
    self._node.AddNode(node_id, address, resolve)
    res = await future.result()
    self._log.info("Result of AddNode: %s" % str(res))

  async def update_node(self, node_id, address):
    """Add a node to the cluster."""
    self._log.info("Updating node %d with new addr %s." % (node_id, address))
    future, resolve = self._get_callback()
    self._node.UpdateNode(node_id, address, resolve)
    res = await future.result()
    self._log.info("Result of UpdateNode: %s" % str(res))
    
    self.proposals_per_term[self._leader_term]

  async def remove_node(self, node_id):
    """Remove a node from the cluster."""
    self._log.info("Removing node %d from the SMR cluster." % node_id)
    future, resolve = self._get_callback()

    try:
      self._node.RemoveNode(node_id, resolve)
    except Exception as ex:
      self._log.error("Error in LogNode while removing replica %d: %s" % (node_id, str(ex)))

    res = await future.result()
    self._log.info("Result of RemoveNode: %s" % str(res))

  async def write_data_dir_to_hdfs(self):
    """
    Write the contents of the etcd-Raft data directory to HDFS.
    """
    self._log.info("Writing etcd-Raft data directory to HDFS.")
    
    serialized_state:bytes = self.__get_serialized_state()
    self._log.info("Serialized important state to be written along with etcd-Raft data. Size: %d bytes." % len(serialized_state))
    
    future, resolve = self._get_callback()
    
    # Convert the Python bytes (bytes) to Go bytes (Slice_byte).
    self._node.WriteDataDirectoryToHDFS(Slice_byte(serialized_state), resolve)
    data_dir_path = await future.result()
    return data_dir_path

  def close(self):
    """Ensure all async coroutines end and clean up."""
    self._node.Close()
    if self._closed is not None:
      asyncio.run_coroutine_threadsafe(self._closed.resolve(None, None), self._start_loop)
      self._closed = None
    self._log.debug("RaftLog %d has closed." % self._id)

  def ensure_path(self, base_path):
    if base_path != "" and not os.path.exists(base_path):
      self._log.debug("Creating persistent store directory: \"%s\"", base_path)
      os.makedirs(base_path, 0o750, exist_ok = True) # It's OK if it already exists.
      self._log.debug("Created persistent store directory \"%s\" (or it already existed)", base_path)

  async def _offload(self, val: SyncValue) -> SyncValue:
    """Offload the buffer to the storage server."""
    # Ensure path exists.
    valEnd = val.end
    val.end = False
    val.val = OffloadPath(await self._offloader.append(val))
    val.prmap = None
    val.end = valEnd
    return val

  def _load(self, val: SyncValue) -> SyncValue:
    """Onload the buffer from the storage server."""
    if type(val.val) is not OffloadPath:
      return val

    valEnd = val.end
    val = self._offloader._load(val.val.path)
    val.end = valEnd
    return val

  def _get_callback(self):
    """Get the future object for the specified key."""
    # Prepare callback settings.
    # Callback can be called from a different thread. Schedule the result of the future object to the await thread.
    loop = asyncio.get_running_loop()

    if loop == self._async_loop:
      self._log.debug("Registering callback future on _async_loop. _async_loop.is_running: %s" % str(self._async_loop.is_running()))
    elif loop == self._start_loop:
      self._log.debug("Registering callback future on _start_loop. _start_loop.is_running: %s" % str(self._start_loop.is_running()))
    else:
      self._log.debug("Registering callback future on unknown loop. loop.is_running: %s" % str(loop.is_running()))

    # self._printIOLoopInformationDebug()

    future = Future(loop=loop)
    self._async_loop = loop
    def resolve(key, err):
      asyncio.run_coroutine_threadsafe(future.resolve(key, err), loop) # must use local variable

    return future, resolve