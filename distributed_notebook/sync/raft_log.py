import io
import os
import pickle
import asyncio
import logging
import time
from typing import Tuple, Callable, Optional, Any, Iterable, Dict
import datetime

from ..smr.smr import NewLogNode, NewConfig, NewBytes, WriteCloser, ReadCloser
from ..smr.go import Slice_string, Slice_int
from .log import SyncLog, SyncValue
from .checkpoint import Checkpoint
from .future import Future
from .errors import print_trace, SyncError, GoError, GoNilError
from .reader import readCloser
from .file_log import FileLog

KEY_LEAD = "_lead_" # Propose to lead the execution term (i.e., execute the user's code).
KEY_YIELD = "_yield_" # Propose to yield the execution to another replica.
KEY_SYNC = "_sync_" # Synchronize to confirm decision about who is executing the code.

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

  def __init__(self, base_path: str, id: int, hdfs_hostname:str, data_directory:str, peer_addrs: Iterable[str], peer_ids: Iterable[int], join: bool = False):
    self._store: str = base_path
    self._id: int = id
    self.ensure_path(self._store)
    self._offloader: FileLog = FileLog(self._store)
    self._log: logging.Logger = logging.getLogger(__class__.__name__ + str(id))

    self._log.info("Creating LogNode %d now." % id)
    self._log.info("_store: %s" % self._store)
    self._log.info("hdfs_hostname: \"%s\"" % hdfs_hostname)
    self._log.info("data_directory: \"%s\"" % data_directory)
    self._log.info("peer_addrs: %s" % peer_addrs)
    self._log.info("peer_ids: %s" % peer_ids)
    self._log.info("join: %s" % join)
    self._node = NewLogNode(self._store, id, hdfs_hostname, data_directory, Slice_string(peer_addrs), Slice_int(peer_ids), join)

    self.winners_per_term: Dict[int, int] = {} # Mapping from term number -> SMR node ID of the winner of that term.
    self.proposals_per_term: Dict[int, SyncValue] = {} # Mapping from term number -> dict. Inner dict is map from SMR node ID -> proposal.
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
      raise ValueError("The LogNode failed to connect to HDFS")

    self._log.info("Successfully created LogNode %d." % id)

    # self.proposal_handlers = {
    #   KEY_LEAD: self._handleLeadProposal,
    #   KEY_YIELD: self._handleYieldProposal,
      # KEY_SYNC: self._handleSyncProposal, # We don't include KEY_SYNC here as we call it explicitly.
    # }

    self._changeCallback = self._changeHandler  # No idea why this walkaround works
    self._restoreCallback = self._restore       # No idea why this walkaround works
    self._ignore_changes = 0
    self._closed = None

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

    self._printIOLoopInformationDebug()

    self._node.Start(config)
    self._log.info("Started LogNode.")

    self._printIOLoopInformationDebug()

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
    """Handle a LEAD/YIELD proposal.

    Args:
        proposal (SyncValue): the committed proposal.
        received_at (float): the time at which we received this proposal.
    """
    # 'SYNC' proposals are handled a little differently.
    # We don't want them to be counted with the other proposals.
    if proposal.key == KEY_SYNC:
      return self._handleSyncProposal(proposal)

    time_str = datetime.datetime.fromtimestamp(proposal.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
    self._log.debug("Received {} req: node {}, term {}, timestamp {} ({}), match {}...".format(proposal.key, proposal.val, proposal.term, proposal.timestamp, time_str, self._id == proposal.val))

    if proposal.term in self.proposals_per_term:
      proposals = self.proposals_per_term[proposal.term]

      if proposal.val in proposals:
        self._log.error("Received multiple proposals from Node %d in term %d." % (proposal.val, proposal.term))
        raise ValueError("Received multiple proposals from Node %d in term %d." % (proposal.val, proposal.term))
      else:
        proposals[proposal.val] = proposal

      self.proposals_per_term[proposal.term] = proposals
    else:
      self.proposals_per_term[proposal.term] = {
        # 'proposal.val' is the SMR node ID of whoever proposed this.
        proposal.val: proposal
      }

    numProposalsReceived:int = len(self.proposals_per_term[proposal.term])
    self._log.debug("Received %d proposal(s) in term %d so far." % (numProposalsReceived, proposal.term))

    discarded:bool = False
    # If this is the first 'LEAD' proposal we're receiving in this term, then take note of the time.
    if self.first_proposal_received_per_term.get(proposal.term, None) is None: # if numProposalsReceived == 1:
      timeout: float = min(5 * (time.time() - proposal.timestamp), 10) # Timeout of 10 seconds at-most.
      self.timeout_durations[proposal.term] = timeout
      self.discard_after[proposal.term] = proposal.timestamp + timeout
      self.first_proposal_received_per_term[proposal.term] = proposal

      self._log.debug("Timeout for term %d will be %.6f seconds. Will discard any proposals received after time %s." % (proposal.term, timeout, datetime.datetime.fromtimestamp(self.discard_after[proposal.term]).strftime('%Y-%m-%d %H:%M:%S.%f')))
    elif self.first_proposal_received_per_term.get(proposal.term, None) is not None and received_at > self.discard_after[proposal.term]:
      # If we've received at least one 'LEAD' proposal, then the timeout has been set, so we check if we need to discard this proposal.
      self._log.debug("Term %d proposal from node %d was received after timeout. Will be discarding." % (proposal.term, proposal.val))

      # Increment the 'num-discarded' counter.
      num_discarded: int = self.num_proposals_discarded.get(proposal.term, 0)
      self.num_proposals_discarded[proposal.term] = num_discarded + 1
      discarded = True

    # Check if this is the first 'LEAD' proposal we're receiving this term.
    # If so, and if we're not discarding the proposal, then record that it is the first 'LEAD' proposal.
    if not discarded and proposal.key == KEY_LEAD and self.first_lead_proposal_received_per_term.get(proposal.term, None) == None:
      self._log.debug("'LEAD' proposal from Node %d is first LEAD proposal in term %d." % (proposal.val, proposal.term))
      self.first_lead_proposal_received_per_term[proposal.term] = proposal

    self._ignore_changes = self._ignore_changes + 1
    return self._makeDecision(proposal.term)

    # Default to '_handleOtherProposal' in case of an erroneous key field.
    # return self.proposal_handlers.get(proposal.key, self._handleOtherProposal)(proposal)

  # def _handleLeadProposal(self, proposal: SyncValue) -> bytes:
  #   """Handle a LEAD proposal."""
    # if self._leader_term < proposal.term:
    #   self._log.debug("Our 'leader_term' (%d) < 'leader_term' of latest commit (%d). Setting our 'leader_term' to %d and the 'leader_id' to %d (from newly-committed value).", self._leader_term, proposal.term, proposal.term, proposal.val)
    #   self._leader_term = proposal.term
    #   self._leader_id = proposal.val

    #   self.winners_per_term[proposal.term] = proposal.val
    #   self._log.debug("Node %d has won in term %d.", proposal.val, proposal.term)

    # # Set the future if the term is expected.
    # _leading = self._leading
    # if _leading is not None and self._leader_term >= self._expected_term:
    #   self._log.debug("leader_term=%d, expected_term=%d. Setting result on '_leading' future to %d.", self._leader_term, self._expected_term, self._leader_term)
    #   self._start_loop.call_later(0, _leading.set_result, self._leader_term)
    #   self._leading = None # Ensure the future is set only once.

    # self._ignore_changes = self._ignore_changes + 1
    # return GoNilError()

  # def _handleYieldProposal(self, proposal: SyncValue) -> bytes:
    # Set the future if the term is expected.
    # _leading = self._leading
    # if _leading is not None and self._leader_term >= self._expected_term:
    #   self._log.debug("leader_term=%d, expected_term=%d. Setting result on '_leading' future to %d.", self._leader_term, self._expected_term, self._leader_term)
    #   self._start_loop.call_later(0, _leading.set_result, self._leader_term)

    # self._ignore_changes = self._ignore_changes + 1
    # return GoNilError()

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

    if (num_proposals + num_discarded) < 3:
      self._log.debug("We've not received enough proposals yet to propose a decision (received: %d, discarded: %d)." % (num_proposals, num_discarded))
      return GoNilError()

    self._log.debug("Received enough proposals to propose a decision (received: %d, discarded: %d)." % (num_proposals, num_discarded))
    # First, check if the winner of the last term issued a LEAD proposal this term.
    # If they did, then we'll propose a decision that they get to lead again.
    # Otherwise, accept the first LEAD proposal of the term.
    last_winner_id:int | None = self.winners_per_term.get(term, None)
    if last_winner_id is not None:
      last_winner_proposal:SyncValue = self.proposals_per_term.get(last_winner_id, None)

      if last_winner_proposal != None:
        self._log.debug("Last term (%d) winner, node %d, proposed '%s'." % (term - 1, last_winner_id, last_winner_proposal.key))

        if last_winner_proposal.key == KEY_LEAD:
          self._log.debug("Proposing decision that last term (%d) winner, node %d, leads this term (%d) as well." % (term-1, last_winner_id, term))

          winningProposal = last_winner_proposal
        else:
          self._log.debug("Will propose first 'LEAD' proposal of this term (%d) as leader." % term)
      else:
        self._log.warn("No proposal received from previous term (%d) winner, node %d." % (term - 1, last_winner_id))

    # If the winning proposal is still done, then we aren't defaulting to the previous term's leader.
    # Get the first 'LEAD' proposal that we received this term.
    if winningProposal == None:
      winningProposal = self.first_lead_proposal_received_per_term.get(term, None)

    # If the winning proposal is still None, then we just didn't receive any 'LEAD' proposals this term.
    # Depending on the scheduling policy, we'll either dynamically create a new replica, or we'll migrate existing replicas.
    if winningProposal == None:
      self._log.warn("We didn't receive any 'LEAD' proposals during term %d." % term)
      # TODO(Ben): The system needs to be able to observe this, which I think it can, as it can see which replicas are yielding.
      raise ValueError("we did not receive any 'LEAD' proposals during term %d, and we've not implemented the procedure(s) for handling this scenario" % term)

    self._log.debug("Will propose that Node %d lead execution for term %d." % (winningProposal.val, winningProposal.term))
    self._printIOLoopInformationDebug()
    
    # Propose the second-round confirmation.
    # self._async_loop.call_soon_threadsafe(self.append_decision, SyncValue(None, self._id, proposed_node = winningProposal.val, timestamp = time.time(), term=winningProposal.term, key=KEY_SYNC))
    if self._decisionProposalFuture is not None:
      self._log.debug("Scheduling the setting of result on _decisionProposalFuture now.")
      self._future_loop.call_soon_threadsafe(self._decisionProposalFuture.set_result, SyncValue(None, self._id, proposed_node = winningProposal.val, timestamp = time.time(), term=winningProposal.term, key=KEY_SYNC))
      self._decisionProposalFuture = None # Make sure it is only set once.
      self._log.debug("Scheduled the setting of result on _decisionProposalFuture now.")
    else:
      self._log.debug("Cannot schedule the setting of result on _decisionProposalFuture now; it is None.")
    
    return GoNilError()

  def _handleSyncProposal(self, proposal: SyncValue) -> bytes:
    """Handle a 'SYNC' (KEY_SYNC) proposal.

    Args:
        proposal (SyncValue): The proposed value.
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
      self._leader_term = syncval.term
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
  #   future = asyncio.run_coroutine_threadsafe(self._restoreImpl(buff), self._start_loop)
  #   return future.result()

  # async def _restoreImpl(self, buff) -> bytes:
  #   """Restore history"""
    self._log.debug("Restoring...")

    reader = readCloser(ReadCloser(handle=rc), sz)
    unpickler = pickle.Unpickler(reader)

    syncval = None
    try:
      syncval = unpickler.load()
    except Exception:
      pass
    # unpickler will close the reader
    # finally:
    #   reader.close()

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
    self._printIOLoopInformationDebug()

    if term == 0:
      term = self._leader_term + 1
    elif term <= self._leader_term:
      return False

    # Define the _leading future
    self._expected_term = term
    self._future_loop = asyncio.get_running_loop()
    self._decisionProposalFuture = self._future_loop.create_future() 
    _decisionProposalFuture = self._decisionProposalFuture
    self._leading = self._future_loop.create_future()
    _leading = self._leading
    self.own_proposal_times[term] = time.time()
    self._log.debug("RaftLog %d: appending LEAD proposal for term %d." % (self._id, term))
    # Append is blocking. We are guaranteed to gain leading status if terms match.
    await self.append(SyncValue(None, self._id, timestamp = time.time(), term=term, key=KEY_LEAD))
    self._log.debug("RaftLog %d: appended LEAD proposal for term %d." % (self._id, term))
    
    decisionProposal: SyncValue = await _decisionProposalFuture
    self._decisionProposalFuture = None 
    self._log.debug("RaftLog %d: Got decision to propose: I think Node %d should win in term %d." % (self._id, decisionProposal.proposed_node, decisionProposal.term))
    
    if decisionProposal.term != term:
      raise ValueError("Received decision proposal with different term: %d. Expected: %d." % (decisionProposal.term, term))
    
    self._log.debug("RaftLog %d: Appending decision proposal for term %s now." % (self._id, decisionProposal.term))
    await self.append_decision(decisionProposal)
    self._log.debug("RaftLog %d: Successfully appended decision proposal for term %s now." % (self._id, decisionProposal.term))
    self.decisions_proposed[term] = True

    # Validate the term
    wait, is_leading = self._is_leading(term)
    if not wait:
      self._log.debug("RaftLog %d: returning from lead for term %d without waiting, is_leading=%s" % (self._id, term, str(is_leading)))
      return is_leading
    # Wait for the future to be set.
    self._log.debug("waiting on _leading Future to be resolved.")
    self._printIOLoopInformationDebug()
    
    await _leading
    # if self._start_loop.is_running():
    #   # Unholy, despicable hack incoming...
    #   current_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    #   target: asyncio.Future = current_loop.create_future()
      
    #   async def wait_on_target():
    #     """
    #     This abomination is executed on the self._start_loop event loop.
    #     It returns when the self._leading Future resolves.
    #     """
    #     try:
    #       self._log.debug("Awaiting _leading in hack.")
    #       result = await self._leading
    #       self._log.debug("Successfully awaited _leading in hack.")
    #     except Exception as ex:
    #       self._log.error("Failed to await self._leading: %s" % str(ex))
    #       current_loop.call_soon_threadsafe(target.set_exception, ex)
    #       return 
    #     else:
    #       # This code gets executed if the try-block succeeds without any exceptions.
    #       current_loop.call_soon_threadsafe(target.set_result, result)
      
    #   self._log.debug("Father, forgive me.")
    #   # Unholy, despicable hack.
    #   self._start_loop.call_soon_threadsafe(wait_on_target)
    #   self._log.debug("Scheduled hack on _start_loop.")
    #   await target 
    #   self._log.debug("Successfully awaited hack.")
    # else:
    #   self._log.debug("Waiting on self._leading in non-hacky way.")
    #   self._start_loop.run_until_complete(self._leading)
    #   self._log.debug("Successfully waited for self._leading in non-hacky way.")
    self._log.debug("Successfully waited for self._leading in non-hacky way.")
    self._leading = None

    # Validate the term
    wait, is_leading = self._is_leading(term)
    assert wait == False
    self._log.debug("RaftLog %d: returning from lead for term %d after waiting, is_leading=%s" % (self._id, term, str(is_leading)))
    self._printIOLoopInformationDebug()
    return is_leading

  async def yield_execution(self, term) -> bool:
    """
    Request to lead the update of a term. A following append call without leading status will fail.
    """
    self._log.debug("RaftLog %d: proposing to yield term %d. Current leader term: %d." % (self._id, term, self._leader_term))
    if term == 0:
      term = self._leader_term + 1
    elif term <= self._leader_term:
      return False

    # Define the _leading future
    self._expected_term = term
    self._future_loop = asyncio.get_running_loop()
    self._decisionProposalFuture = self._future_loop.create_future() 
    _decisionProposalFuture = self._decisionProposalFuture
    self._leading = self._future_loop.create_future()
    _leading = self._leading
    self.own_proposal_times[term] = time.time()
    self._log.debug("RaftLog %d: appending YIELD proposal for term %d." % (self._id, term))

    # Append is blocking.
    await self.append(SyncValue(-1, self._id, timestamp = time.time(), term=term, key=KEY_YIELD))
    self._log.debug("RaftLog %d: appended YIELD proposal for term %d." % (self._id, term))

    decisionProposal: SyncValue = await _decisionProposalFuture
    self._decisionProposalFuture = None 
    self._log.debug("RaftLog %d: Got decision to propose: I think Node %d should win in term %d." % (self._id, decisionProposal.proposed_node, decisionProposal.term))

    if decisionProposal.term != term:
      raise ValueError("Received decision proposal with different term: %d. Expected: %d." % (decisionProposal.term, term))
    
    self._log.debug("RaftLog %d: Appending decision proposal now." % decisionProposal.term)
    await self.append_decision(decisionProposal)
    self._log.debug("RaftLog %d: Successfully appended decision proposal now." % decisionProposal.term)
    self.decisions_proposed[term] = True

    # Validate the term
    wait, is_leading = self._is_leading(term)
    if not wait:
      assert is_leading == False
      self._log.debug("RaftLog %d: returning from yield_execution for term %d without waiting" % (self._id, term))
    
    # Wait for the future to be set.
    # if self._start_loop.is_running():
    #   self._log.warn("_start_loop is already running... this is unexpected.")
    #   self._log.warn("waiting on _leading Future to be resolved.")
    #   self._start_loop.call_soon_threadsafe(self._leading)
    # else:
    #   self._log.warn("waiting on _leading Future to be resolved.")
    self._log.debug("waiting on _leading Future to be resolved.")
    self._printIOLoopInformationDebug()
    
    await _leading
    # if self._start_loop.is_running():
    #   # Unholy, despicable hack incoming...
    #   current_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    #   target: asyncio.Future = current_loop.create_future()
      
    #   async def wait_on_target():
    #     """
    #     This abomination is executed on the self._start_loop event loop.
    #     It returns when the self._leading Future resolves.
    #     """
    #     try:
    #       result = await self._leading
    #     except Exception as ex:
    #       self._log.error("Failed to await self._leading: %s" % str(ex))
    #       current_loop.call_soon_threadsafe(target.set_exception, ex)
    #       return 
    #     else:
    #       # This code gets executed if the try-block succeeds without any exceptions.
    #       current_loop.call_soon_threadsafe(target.set_result, result)
      
    #   self._log.debug("Father, forgive me.")
    #   # Unholy, despicable hack.
    #   self._start_loop.call_soon_threadsafe(wait_on_target)
    #   self._log.debug("Scheduled hack on _start_loop.")
    #   await target 
    #   self._log.debug("Successfully awaited hack.")
    # else:
    #   self._log.debug("Waiting on self._leading in non-hacky way.")
    #   self._start_loop.run_until_complete(self._leading)
    #   self._log.debug("Successfully waited for self._leading in non-hacky way.")
    self._log.debug("Successfully waited for self._leading in non-hacky way.")
    self._leading = None

    # Validate the term
    wait, is_leading = self._is_leading(term)
    assert wait == False
    assert is_leading == False
    self._log.debug("RaftLog %d: returning from yield_execution for term %d after waiting" % (self._id, term))
    self._printIOLoopInformationDebug()

    return False

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
    future, resolve = self._get_callback()
    self._node.WriteDataDirectoryToHDFS(resolve)
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
      os.makedirs(base_path, 0o750)

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

    self._printIOLoopInformationDebug()

    future = Future(loop=loop)
    self._async_loop = loop
    def resolve(key, err):
      asyncio.run_coroutine_threadsafe(future.resolve(key, err), loop) # must use local variable

    return future, resolve