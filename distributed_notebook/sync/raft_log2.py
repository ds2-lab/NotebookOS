import io
import os
import base64
import pickle
import asyncio
import json
import logging
import time
import datetime

from collections import OrderedDict
from typing import Tuple, Callable, Optional, Any, Iterable, Dict, List

from ..smr.smr import NewLogNode, NewConfig, NewBytes, WriteCloser, ReadCloser, PrintTestMessage
from ..smr.go import Slice_string, Slice_int, Slice_byte
from .log import SyncLog, SynchronizedValue, LeaderElectionVote, LeaderElectionProposal, ElectionProposalKey
from .checkpoint import Checkpoint
from .future import Future
from .errors import print_trace, SyncError, GoError, GoNilError
from .reader import readCloser
from .file_log import FileLog
from .election import Election

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

class offloadPath:
    def __init__(self, path: str):
        self.path = path

    def __str__(self):
        return self.path
  
class RaftLog(object):
    """
    Encapsulates a log that stores the changes of Python objects.
    """

    def __init__(
        self,
        id: int,
        base_path: str = "/store", 
        hdfs_hostname: str = "172.17.0.1:9000",
        data_directory: str = "/storage",
        peer_addrs: Iterable[str] = [], 
        peer_ids: Iterable[int] = [], 
        num_replicas: int = 3,
        join: bool = False, 
        debug_port:int = 8464,
        heartbeat_tick: int = 10, # Raft-related
        election_tick:int = 1     # Raft-related
    ):  
        if len(hdfs_hostname) == 0:
            raise ValueError("HDFS hostname is empty.")
        
        if debug_port <= 1023 or debug_port >= 65535:
           raise ValueError("Invalid debug port specified.")

        self.logger: logging.Logger = logging.getLogger(__class__.__name__ + str(id))
        self.logger.info("Creating RaftNode %d now." % id)

        # The term that the leader is expecting.
        self._expected_term: int = 0 
        # Updated after a LEAD call. This is the term of the LEADER. Used to check if received proposals are old/new. 
        self._leader_term: int = 0 
        # The id of the leader.
        self._leader_id: int = 0
        self._persistent_store_path:str = base_path
        self._node_id:int = id 
        self._offloader: FileLog = FileLog(self._persistent_store_path)
        self._num_replicas: int = num_replicas
        self._last_winner_id: int = -1 

        try:
            self._create_persistent_store_directory(base_path)
        except Exception as ex:
            self.logger.error(f"Error while creating persistent datastore directory \"{base_path}\": {ex}")

        self.logger.info("persistent store path: %s" % self._persistent_store_path)
        self.logger.info("hdfs_hostname: \"%s\"" % hdfs_hostname)
        self.logger.info("data_directory: \"%s\"" % data_directory)
        self.logger.info("peer_addrs: %s" % peer_addrs)
        self.logger.info("peer_ids: %s" % peer_ids)
        self.logger.info("join: %s" % join)
        self.logger.info("debug_port: %d" % debug_port)

        self._log_node = NewLogNode(self._persistent_store_path, id, hdfs_hostname, data_directory, Slice_string(peer_addrs), Slice_int(peer_ids), join, debug_port)
        if self._log_node == None:
            raise RuntimeError("Failed to create LogNode.")
        elif not self._log_node.ConnectedToHDFS():
            self.logger.error("The LogNode failed to connect to HDFS.")
            raise RuntimeError("The LogNode failed to connect to HDFS")
        
        self.logger.info(f"Successfully created LogNode {id}.")

        # Mapping from term number to the election associated with that term. 
        self._elections: Dict[int, Election] = {} 
        # The current/active election.
        self._current_election: Optional[Election] = None 
        # The most recent election to have been completed successfully.
        self._last_completed_election: Optional[Election] = None 
        
        # TBD
        self._change_handler: Optional[Callable[[SynchronizedValue], None]] = None 

        self.my_current_attempt_number : int = 1 # Attached to proposals. Sort of an ID within an election term. 
        self.winners_per_term: Dict[int, int] = {} # Mapping from term number -> SMR node ID of the winner of that term.
        self._proposed_values: OrderedDict[int, OrderedDict[int, LeaderElectionProposal]] = OrderedDict() # Mapping from term number -> Dict. The inner map is attempt number -> proposal.
        self.my_current_attempt_number:int = 1 # The current attempt number for the current term. 
        self.largest_peer_attempt_number:Dict[int, int] = {0:0} # The largest attempt number received from a peer's proposal.
        self.proposals_per_term: Dict[int, Dict[int, LeaderElectionProposal]] = {} # Mapping from term number -> dict. Inner dict is map from SMR node ID -> proposal.
        self.own_proposal_times: Dict[int, float] = {} # Mapping from term number -> the time at which we proposed LEAD/YIELD in that term.
        self.first_lead_proposal_received_per_term: Dict[int, LeaderElectionProposal] = {} # Mapping from term number -> the first 'LEAD' proposal received in that term.
        self.first_proposal_received_per_term: Dict[int, LeaderElectionProposal] = {} # Mapping from term number -> the first proposal received in that term.
        self.timeout_durations: Dict[int, float] = {} # Mapping from term number -> the timeout (in seconds) for that term.
        self.discard_after: Dict[int, float] = {} # Mapping from term number -> the time after which received proposals will be discarded.
        self.num_proposals_discarded: Dict[int, int] = {} # Mapping from term number -> the number of proposals that were discarded in that term.
        self.sync_proposals_per_term: Dict[int, LeaderElectionProposal] = {} # Mapping from term number -> the first SYNC proposal committed during that term.
        self.decisions_proposed: Dict[int, bool] = {} # Mapping from term number -> boolean flag indicating whether we've proposed (but not necessarily committed) a decision for the given term yet.

        self._ignore_changes: int = 0

        # This can be set such that it will be resolved when close() is called.
        self._closed: Optional[Callable[[str, Exception]]] = None 

        self._heartbeat_tick:int = heartbeat_tick
        self._election_tick:int = election_tick

        self._valueCommittedCallback: Callable[[Any, int, str], Any] = self._valueCommitted
        self._valueRestoredCallback: Callable[[Any, int], Any] = self._valueRestored 

        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._start_loop: Optional[asyncio.AbstractEventLoop] = None

        # This will just do nothing if there's no serialized state to be loaded.
        self._load_and_apply_serialized_state()

    def _create_persistent_store_directory(self, path: str):
        """
        Create a directory at the specified path if it does not already exist.
        """
        if path != "" and not os.path.exists(path):
            self.logger.debug(f"Creating persistent store directory: \"{path}\"")
            os.makedirs(path, 0o750, exist_ok = True) # It's OK if it already exists.
            self.logger.debug(f"Created persistent store directory \"{path}\" (or it already exists).")

    def _handleVote(self, vote: LeaderElectionVote, received_at = time.time()) -> bytes:
        assert self._current_election != None 

        # The first 'VOTE' proposal received during the term automatically wins.
        was_first_vote_proposal:bool = self._current_election.add_vote_proposal(vote, overwrite = True, received_at = received_at)
        if not was_first_vote_proposal:
            self.logger.debug(f"We've already received at least 1 other 'VOTE' proposal during term {self._current_election.term_number}. Ignoring 'VOTE' proposal from node {vote.proposer_id}.")
            return GoNilError() 
        
        if self._leader_term < vote.election_term:
            self.logger.debug("Our 'leader_term' (%d) < 'leader_term' of latest committed 'SYNC' (%d). Setting our 'leader_term' to %d and the 'leader_id' to %d (from newly-committed value)." % (self._leader_term, vote.election_term, vote.election_term, vote.proposed_node_id))
            self._leader_term = vote.election_term
            self._leader_id = vote.proposed_node_id
            self.logger.debug("Node %d has won in term %d as proposed by node %d." % (vote.proposed_node_id, vote.election_term, vote.proposer_id))
            self._current_election.complete_election(vote.proposed_node_id)
            self._last_winner_id = vote.proposed_node_id
            self._last_completed_election = self._current_election
        else:
            self.logger.debug("Our leader_term (%d) >= 'leader_term' of latest committed 'SYNC' message (%d)..." % (self._leader_term, vote.election_term))
        
        # Set the future if the term is expected.
        _leading = self._leading
        if _leading is not None and self._leader_term >= self._expected_term:
            self.logger.debug("Scheduling the setting of result on '_leading' future to %d." % (self._leader_term, self._leader_term))
            self._future_io_loop.call_later(0, _leading.set_result, self._leader_term)
            self._leading = None # Ensure the future is set only once.
            self.logger.debug("Scheduled setting of result on '_leading' future.")
        else:
            self.logger.debug("Skipping setting result on _leading. _leading is None: %s. self._leader_term (%d) >= self._expected_term (%d): %s." % (self._leading == None, self._leader_term, self._expected_term, self._leader_term >= self._expected_term))

        self._ignore_changes = self._ignore_changes + 1

        return GoNilError()
    
    def _handleProposal(self, proposal: LeaderElectionProposal, received_at: float = 0) -> bytes:
        """Handle a committed LEAD/YIELD proposal.

        Args:
            proposal (LeaderElectionProposal): the committed proposal.
            received_at (float): the time at which we received this proposal.
        """
        assert self._current_election != None 
        assert self._current_election.term_number == proposal.election_term
        
        time_str = datetime.datetime.fromtimestamp(proposal.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
        term:int = proposal.election_term 
        self.logger.debug("Received {} from node {}. Term {}, attempt {}, timestamp {} ({}), match {}...".format(proposal.key, proposal.proposer_id, term, proposal.attempt_number, proposal.timestamp, time_str, self._node_id == proposal.proposer_id))

        val: Optional[tuple[asyncio.Future[Any], float]] = self._current_election.add_proposal(proposal, self._future_io_loop, received_at = received_at)
        if val != None:
            # Future to decide the result of the election by a certain time limit. 
            _pick_and_propose_winner_future, _discard_after = val

            async def resolve():
                assert self._current_election != None 
                sleep_duration: float = _discard_after - time.time() 
                current_term: int = self._current_election.term_number
                assert sleep_duration > 0 
                await asyncio.sleep(sleep_duration)

                if _pick_and_propose_winner_future.done():
                    return 

                if self._current_election.term_number != current_term:
                    self.logger.warn(f"Election term has changed in resolve(). Was {current_term}, is now {self._current_election.term_number}.")
                    return 

                try:
                    selected_winner:bool = self._tryPickWinnerToPropose() 

                    if not selected_winner:
                        assert not self._current_election.is_active

                    _pick_and_propose_winner_future.set_result(1)
                except asyncio.InvalidStateError:
                    self.logger.error("Future for picking and proposing a winner of election term {} has already been resolved...")
            
            # Schedule `resolve` to be called.
            # It will sleep until the discardAt time expires, at which point a decision needs to be made.
            # If a decision was already made for that election, then the `resolve` function will simply return.
            self._future_io_loop.call_soon_threadsafe(resolve)
        
        self.logger.debug(f"Received {self._current_election.num_proposals_received} proposal(s) so far during term {self._current_election.term_number}.")

        self._tryPickWinnerToPropose() 

        self._ignore_changes = self._ignore_changes + 1
        return GoNilError() 
    
    def _tryPickWinnerToPropose(self, election_override: Optional[Election] = None)->bool:
        """
        Try to select a winner to propose for the current election.

        Return True if a winner was selected for proposal (including just proposing 'FAILURE' due to all nodes proposing 'YIELD); otherwise, return False. 
        """
        assert self._current_election != None 

        try:
            id: int = self._current_election.pick_winner_to_propose(last_winner_id = self._last_winner_id)
            if id > 0:
                assert self._election_decision_future != None 
                self.logger.debug(f"Will propose that node {id} win the election in term {self._current_election.term_number}.")
                self._future_io_loop.call_soon_threadsafe(self._election_decision_future.set_result, LeaderElectionVote(proposed_node_id = id, proposer_id = self._node_id, election_term = self._current_election.term_number, attempt_number = self._current_election.current_attempt_number))
                return True
            else:
                assert self._election_decision_future != None 
                self.logger.debug(f"Will propose 'FAILURE' for election in term {self._current_election.term_number}.")
                self._future_io_loop.call_soon_threadsafe(self._election_decision_future.set_result, LeaderElectionVote(proposed_node_id = -1, proposer_id = self._node_id, election_term = self._current_election.term_number, attempt_number = self._current_election.current_attempt_number))
                return True
        except ValueError as ex:
            self.logger.debug(f"No winner to propose yet for election in term {self._current_election.term_number}.")
        
        return False 
    
    def _valueCommitted(self, goObject, value_size: int, value_id: str) -> bytes:
        received_at:float = time.time()

        if id != "":
            self.logger.debug(f"Our proposal {self._node_id} of size {value_size} bytes was committed.")
        else:
            self.logger.debug(f"Received remote update of size {value_size} bytes")
        
        reader = readCloser(ReadCloser(handle=goObject), value_size)
        
        try:
            committedValue: SynchronizedValue = pickle.load(reader)
        except Exception as ex:
            self.logger.error(f"Failed to unpickle committed value because: {ex}")
            raise ex 
        
        if isinstance(committedValue, LeaderElectionVote):
            return self._handleVote(committedValue, received_at = received_at)
        elif isinstance(committedValue, LeaderElectionProposal):
            return self._handleProposal(committedValue, received_at = received_at)

        # Skip state updates from current node.
        if value_id != "":
            return GoNilError()
        
        self._leader_term = committedValue.election_term

        # For values synchronized from other replicas or replayed, count _ignore_changes
        if not committedValue.has_operation:
            self._ignore_changes = self._ignore_changes + 1
        
        assert self._change_handler != None 
        try:
            self._change_handler(self._load_value(committedValue))
        except Exception as ex:
            self.logger.error(f"Failed to handle changed value because: {ex}")
            print_trace(limit = 10)
            raise ex 

        return GoNilError()

    def _valueRestored(self, goObject, value_size: int) -> bytes:
        self.logger.debug(f"Restoring value of size {value_size} now...")

        reader = readCloser(ReadCloser(handle=goObject), value_size)
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
                assert self._change_handler != None 
                self._change_handler(self._load_value(syncval))
                restored = restored + 1

                syncval = None
                syncval = unpickler.load()
            except SyncError as se:
                self.logger.error("Error on restoreing snapshot: {}".format(se))
                return GoError(se)
            except Exception:
                pass

        self.logger.debug(f"Restored value of size {value_size} bytes: {restored}")
        return GoNilError()

    def _load_value(self, val: SynchronizedValue) -> SynchronizedValue:
        """Onload the buffer from the storage server."""
        if type(val.data) is not offloadPath:
            return val

        should_end_execution = val.should_end_execution
        val = self._offloader._load(val.data.path) # type: ignore
        val.set_should_end_execution(should_end_execution)
        return val

    def _get_serialized_state(self) -> bytes:
        """
        Serialize important state so that it can be written to HDFS (for recovery purposes).
        
        This return value of this function should be passed to the `self._log_node.WriteDataDirectoryToHDFS` function.
        """        
        data_dict:dict = {
            "winners_per_term": self.winners_per_term,
            "proposed_values": self._proposed_values,
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
            "leader_term": self._leader_term,
            "leader_id": self._leader_id,
            "expected_term": self.expected_term,
            "elections": self._elections,
            "current_election": self._current_election,
            "last_completed_election": self._last_completed_election,
        }
        
        return pickle.dumps(data_dict)

    def _load_and_apply_serialized_state(self) -> None:
        """
        Retrieve the serialized state read by the Go-level LogNode. 
        This state is read from HDFS during migration/error recovery.
        Update our local state with the state retrieved from HDFS.
        """
        serialized_state_bytes:bytes = bytes(self._log_node.GetSerializedState()) # Convert the Go bytes (Slice_byte) to Python bytes.
        
        if len(serialized_state_bytes) == 0:
            self.logger.debug("No serialized state found. Nothing to load and apply.")
            return 
        
        data_dict:dict = pickle.loads(serialized_state_bytes) # json.loads(serialized_state_json)
        if len(data_dict) == 0:
            self.logger.debug("No serialized state found. Nothing to apply.")
            return 
        
        for key, entry in data_dict.items():
            self.logger.debug(f"Retrived state \"{key}\": {str(entry)}")
        
        # TODO: 
        # There may be some bugs that arrise from these values being somewhat old or outdated, potentially.
        self.winners_per_term = data_dict["winners_per_term"]
        self._proposed_values = data_dict["proposed_values"]
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
        self._elections = data_dict["elections"]
        self._current_election = data_dict["current_election"]
        self._last_completed_election = data_dict["last_completed_election"]
        
        # If true, then the "remote updates" that we're receiving are us catching up to where we were before a migration/eviction was triggered.
        self._catchingUpAfterMigration = True 

        # The value of _leader_term before a migration/eviction was triggered.
        self.leader_term_before_migration: int = data_dict["_leader_term"]

        # Commenting these out for now; it's not clear if we should set these in this way yet.
        # self._leader_term = data_dict["leader_term"]
        # self._leader_id = data_dict["leader_id"]
        self._expected_term = data_dict["expected_term"]

    def _get_callback(self)-> Tuple[asyncio.Future, Callable[[str, Exception], Any]]:
        """Get the future object for the specified key."""
        # Prepare callback settings.
        # Callback can be called from a different thread. Schedule the result of the future object to the await thread.
        loop = asyncio.get_running_loop()

        if loop == self._async_loop:
            self.logger.debug("Registering callback future on _async_loop. _async_loop.is_running: %s" % str(self._async_loop.is_running())) # type: ignore
        elif loop == self._start_loop:
            self.logger.debug("Registering callback future on _start_loop. _start_loop.is_running: %s" % str(self._start_loop.is_running())) # type: ignore
        else:
            self.logger.debug("Registering callback future on unknown loop. loop.is_running: %s" % str(loop.is_running()))

        future: asyncio.Future = Future(loop=loop) # type: ignore
        self._async_loop = loop
        def resolve(key, err):
            # must use local variable
            asyncio.run_coroutine_threadsafe(future.resolve(key, err), loop) # type: ignore 

        return future, resolve

    def _is_leading(self, term) -> Tuple[bool, bool]:
        """Check if the current node is leading, return (wait, is_leading)"""
        if self._leader_term > term:
            return False, False
        elif self._leader_term == term:
            return False, self._leader_id == self._node_id
        else:
            return True, False

    def _create_new_election(self, term_number:int = -1)->Election:
        """
        Create and register a new election with the given term number.

        This modifies the following fields:
            - self._elections
            - self._election 
            - self._last_completed_election
        
        Raises a ValueError if any of the following conditions are met:
            - term_number < 0
            - we already have an active election 
            - term_number < the previous election's term number  
            - the current election is not in the state ElectionState.COMPLETE (i.e., the current election needs to have completed successfully)
        """
        if term_number < 0:
            raise ValueError(f"illegal term number specified for new election: {term_number}")
        
        # TODO: We may want to "relax" these conditions, or rather the consequences of these conditions, and attempt to proceed even if there's an error.
        if self.has_active_election():
            assert self._current_election != None 
            self.logger.error(f"Creating new election with term number {term_number} despite already having an active election with term number {self._current_election.term_number}")
            raise ValueError(f"attempted to create new election while already having an active election")
        elif self._current_election != None and self._current_election.term_number > term_number:
            self.logger.error(f"Creating new election with term number {term_number} despite already previous election having a larger term number of {self._current_election.term_number}") 
            raise ValueError(f"attempted to create new election with term number smaller than previous election's term number ({term_number} < {self._current_election.term_number})") 
        elif self._current_election != None and not self._current_election.completed_successfully:
            self.logger.error(f"Current election with term number {self._current_election.term_number} is in state {self._current_election._election_state}; it has not yet completed successfully.")
            raise ValueError(f"current election (term number: {self._current_election.term_number}) has not yet completed successfully (current state: {self._current_election._election_state})")

        new_election: Election = Election(term_number, self._num_replicas)
        self._elections[term_number] = new_election
        self._last_completed_election = self._current_election
        self._current_election = new_election

        if self._last_completed_election != None:
            assert self._last_completed_election.completed_successfully

        self.logger.info(f"Created new election with term number {term_number}")

        return new_election

    async def _offload_value(self, val: SynchronizedValue) -> SynchronizedValue:
        """Offload the buffer to the storage server."""
        # Ensure path exists.
        should_end_execution = val.should_end_execution
        val.set_should_end_execution(False)
        val.set_data(offloadPath(await self._offloader.append(val)))
        val.set_prmap(None)
        val.set_should_end_execution(should_end_execution)
        return val

    async def _append_value(
            self,
            value: SynchronizedValue
    ):
        """
        Append some data to the synchronized Raft log.
        """
        self._leader_term = value.election_term

        if not value.has_operation:
            # Count _ignore_changes
            self._ignore_changes += 1

        # Ensure key is specified.
        if value.key is not None:
            if value.data is not None and type(value.data) is bytes and len(value.data) > MAX_MEMORY_OBJECT:
                value = await self._offload_value(value)

        await self._serialize_and_append_value(value)

    async def _append_election_proposal(
            self,
            proposal: LeaderElectionProposal
    ):
        """
        Explicitly propose and append (to the synchronized Raft log) a proposal for the current election.
        """
        await self._serialize_and_append_value(proposal)

    async def _serialize_and_append_value(self, value: SynchronizedValue):
        """
        Serialize the SynchronizedValue (using the pickle module) and explicitly propose and append it to the synchronized etcd-raft log. 
        """
        # Serialize the value.
        dumped = pickle.dumps(value)

        # Propose and wait the future.
        future, resolve = self._get_callback()
        self._log_node.Propose(NewBytes(dumped), resolve, value.key)
        await future.result()

    async def _handle_election(
            self, 
            proposal: LeaderElectionProposal,
            target_term_number: int = -1
        ) -> bool:
        """
        Orchestrate an election. Return a boolean indicating whether or not we are now the "leader".

        The election should have been setup/created prior to calling this function.

        The `target_term_number` argument is just a safety mechanism to ensure that the current election matches the intended/target term number.
        """
        self.logger.debug(f"RaftLog {self._node_id} handling election in term {target_term_number}, attempt #{proposal.attempt_number}. Will be proposing {proposal.key}.")

        # TODO: Implement functionality of specifying term 0 to guarantee winning of election.
        if target_term_number == 0:
            raise ValueError("specifiying target term of 0 is not yet supported")

        # The current election field must be non-null.
        if self._current_election == None:
            raise ValueError("current election field is None")
        
        if self._last_completed_election != None and target_term_number <= self._leader_term:
            self.logger.error(f"Current leader term {self._leader_term} > specified target term {target_term_number}...")
            return False 
        
        self._expected_term = target_term_number
        
        # The current election field's term number must match the specified target term number.
        if self._current_election.term_number != target_term_number:
            raise ValueError(f"current election is targeting term {self._current_election.term_number}, whereas the target term number was specified to be {target_term_number}")
        
        # The proposal's term number must match the specified target term number.
        if proposal.election_term != target_term_number:
            raise ValueError(f"proposal is targeting election term {proposal.election_term}, whereas caller specified election term {target_term_number}")

        # Do some additional sanity checks:
        # The proposal must already be registered. 
        # This means that there will be at least one proposal for the specified target term number (which matches the proposal's term number; we already checked verified that above).
        assert target_term_number in self._proposed_values # At least one proposal for the specified term?
        assert proposal.attempt_number in self._proposed_values[target_term_number] # The proposal is registered under its attempt number?
        assert self._proposed_values[target_term_number][proposal.attempt_number] == proposal # Equality check for ultimate sanity check. 

        # Define the `_leading` feature.
        # Save a reference to the currently-running IO loop so that we can resolve the `_leading` future on this same IO loop later.
        self._future_io_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        # This is the future we'll use to submit a formal vote for who should lead, based on the proposals that are committed to the etcd-raft log.
        self._election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = self._future_io_loop.create_future()
        # This is the future that we'll use to inform the local kernel replica if it has been selected to "lead" the election (and therefore execute the user-submitted code).
        self._leading_future: Optional[asyncio.Future[bool]] = self._future_io_loop.create_future()

        # Create local references.
        _election_decision_future: asyncio.Future[Any] = self._election_decision_future 
        _leading_future: asyncio.Future[bool] = self._leading_future 

        await self._append_election_proposal(proposal)

        voteProposal: LeaderElectionVote = await _election_decision_future
        self._election_decision_future = None 

        # Validate that the term number matches the current election.
        if voteProposal.election_term != self._current_election.term_number:
            raise ValueError(f"received LeaderElectionVote with mis-matched term number ({voteProposal.election_term}) compared to current election term number ({self._current_election.term_number})")
        
        if voteProposal.election_failed:
            self.logger.debug("RaftLog %d: Got decision to propose: election failed. No replicas proposed 'LEAD'." % self._node_id)
            
            # None of the replicas proposed 'LEAD'
            # It is likely that a migration of some sort will be triggered as a result, leading to another election round for this term.
            return False 
        
        self.logger.debug("RaftLog %d: Appending decision proposal for term %s now." % (self._node_id, voteProposal.election_term))
        await self._append_value(voteProposal)
        self.logger.debug("RaftLog %d: Successfully appended decision proposal for term %s now." % (self._node_id, voteProposal.election_term))

        # Validate the term
        wait, is_leading = self._is_leading(target_term_number)
        if not wait:
            self.logger.debug("RaftLog %d: returning for term %d without waiting, is_leading=%s" % (self._node_id, target_term_number, str(is_leading)))
            return is_leading
        
        # Wait for the future to be set.
        self.logger.debug("waiting on _leading Future to be resolved.")
        await _leading_future
        self.logger.debug("Successfully waited for self._leading.")
        self._leading_future = None

        # Validate the term
        wait, is_leading = self._is_leading(target_term_number)
        assert wait == False
        return is_leading

    def _create_election_proposal(self, key: ElectionProposalKey, term_number: int) -> LeaderElectionProposal:
        """
        Create and register a proposal for the current term.

        This updates the `self._proposed_values` field.

        The attempt number for the new proposal is "calculated" based on whether there already exists a previous proposal for this election term.
        """
        attempt_number:int = 1
        
        # Get the existing proposals for the specified term.
        existing_proposals: Dict[int, LeaderElectionProposal] = self._proposed_values.get(term_number, OrderedDict()) 

        # If there is at least one existing proposal for the specified term, then we'll get the most-recent proposal's attempt number.
        if len(existing_proposals) > 0:
            last_attempt_number: int = next(reversed(existing_proposals)) # This is O(1), as OrderedDict uses a doubly-linked list internally.
            attempt_number = last_attempt_number # Could be on one line, but this is more readable in my opinion.
        
        # Create the new proposal.
        proposal: LeaderElectionProposal = LeaderElectionProposal(key = str(key), proposer_id = self._node_id, election_term = term_number, attempt_number = attempt_number)
        
        # Add the new proposal to the mapping of proposals for the specified term.
        existing_proposals[attempt_number] = proposal 

        # Update the mapping (of proposals for the specified term) in the `self._proposed_values` field.
        self._proposed_values[term_number] = existing_proposals

        # Return the new proposal.
        return proposal 

    def sync(self, term):
        """Synchronization changes since specified execution counter."""
        pass

    def reset(self, term, logs: Tuple[SynchronizedValue]):
        """Clear logs equal and before specified term and replaced with specified logs"""
        pass

    async def add_node(self, node_id, address):
        """Add a node to the etcd-raft cluster."""
        self.logger.info("Adding node %d at addr %s to the SMR cluster." % (node_id, address))
        future, resolve = self._get_callback()
        self._log_node.AddNode(node_id, address, resolve)
        res = await future.result()
        self.logger.info("Result of AddNode: %s" % str(res))

    async def update_node(self, node_id, address):
        """Add a node to the etcd-raft  cluster."""
        self.logger.info("Updating node %d with new addr %s." % (node_id, address))
        future, resolve = self._get_callback()
        self._log_node.UpdateNode(node_id, address, resolve)
        res = await future.result()
        self.logger.info("Result of UpdateNode: %s" % str(res))
        
        self.proposals_per_term[self._leader_term]

    async def remove_node(self, node_id):
        """Remove a node from the etcd-raft cluster."""
        self.logger.info("Removing node %d from the SMR cluster." % node_id)
        future, resolve = self._get_callback()

        try:
            self._log_node.RemoveNode(node_id, resolve)
        except Exception as ex:
            self.logger.error("Error in LogNode while removing replica %d: %s" % (node_id, str(ex)))

        res = await future.result()
        self.logger.info("Result of RemoveNode: %s" % str(res))

    def has_active_election(self)->bool:
        """
        Return true if the following two conditions are met:
            - (a): We have an election (i.e., the _current_election field is non-nil)
            - (b): The current election is in the ACTIVE state
        """
        if self.current_election == None:
            return False 
        
        return self.current_election.is_active

    @property 
    def leader_term(self)->int:
        """
        Updated after a LEAD call. This is the term of the LEADER. Used to check if received proposals are old/new. 
        """
        return self._leader_term

    @property 
    def leader_id(self)->int:
        """
        ID of the current leader. Updated after a LEAD call. Used to check if received proposals are old/new.
        """
        return self._leader_id

    @property 
    def expected_term(self)->Optional[int]:
        """
        This is the term number we're expecting for the current election.

        It's equal to the current election's term number.
        """
        if self._current_election != None:
            return self._current_election.term_number
        
        return -1 

    @property 
    def current_election(self)->Optional[Election]:
        return self._current_election

    @property 
    def last_completed_election(self)->Optional[Election]:
        return self._last_completed_election

    def start(self, handler: Callable[[SynchronizedValue], None])->None:
        """
        Register the change handler, restore internal states, and start monitoring for changes committed to the Raft log.
        """
        self._change_handler = handler 

        config = NewConfig() 
        config.ElectionTick = self._heartbeat_tick 
        config.HeartbeatTick = self._election_tick 

        config = config.WithChangeCallback(self._valueCommittedCallback).WithRestoreCallback(self._valueRestoredCallback)
        
        if self._shouldSnapshotCallback is not None:
            config = config.WithShouldSnapshotCallback(self._shouldSnapshotCallback)
        if self._snapshotCallback is not None:
            config = config.WithSnapshotCallback(self._snapshotCallback)

        self.logger.info(f"Starting LogNode {self._node_id} now.")

        self._async_loop = asyncio.get_running_loop()
        self._start_loop = self._async_loop

        startSuccessful: bool = self._log_node.Start(config)
        if not startSuccessful:
            self.logger.error("Failed to start LogNode.")
            raise RuntimeError("failed to start the Golang-level LogNode component")
        
        self.logger.info("Successfully started RaftLog and LogNode.")

    def close(self)->None:
        """
        Ensure all async coroutines have completed. Clean up resources. Stop the LogNode.
        """
        self.logger.warn(f"Closing LogNode {self._node_id} now.")
        self._log_node.Close()
        
        if self._closed is not None: 
            if self._start_loop is None:
                self.logger.error("Cannot resolve '_closed' future; start loop is None...")
            else:
                asyncio.run_coroutine_threadsafe(self._closed.resolve(None, None), self._start_loop) 
                self._closed = None
        
        self.logger.debug("RaftLog %d has closed." % self._node_id)

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
                self.logger.error("Error on snapshoting: {}".format(e))
                return GoError(e)

        self._snapshotCallback = snapshotCallback

    async def write_data_dir_to_hdfs(self):
        """
        Write the contents of the etcd-Raft data directory to HDFS.
        """
        self.logger.info("Writing etcd-Raft data directory to HDFS.")
        
        serialized_state:bytes = self._get_serialized_state()
        self.logger.info("Serialized important state to be written along with etcd-Raft data. Size: %d bytes." % len(serialized_state))
        
        future, resolve = self._get_callback()
        
        # Convert the Python bytes (bytes) to Go bytes (Slice_byte).
        self._log_node.WriteDataDirectoryToHDFS(Slice_byte(serialized_state), resolve)
        data_dir_path = await future.result()
        return data_dir_path

    async def try_lead_execution(self, term_number: int) -> bool:
        """
        Request to serve as the leader for the update of a term (and therefore to be the replica to execute user-submitted code).

        A subsequent call to append (without successfully being elected as leader) will fail.
        """
        self.logger.debug("RaftLog %d is proposing to lead term %d." % (self._node_id, term_number))

        # Create the new election.
        self._create_new_election(term_number = term_number)

        # Create a proposal.
        proposal: LeaderElectionProposal = self._create_election_proposal(ElectionProposalKey.LEAD, term_number)

        # Orchestrate/carry out the election.
        is_leading:bool = await self._handle_election(proposal, target_term_number = term_number)

        return is_leading  

    async def try_yield_execution(self, term_number: int) -> bool:
        """
        Request to explicitly yield the current term update (and therefore the execution of user-submitted code) to another replica.
        """
        self.logger.debug("RaftLog %d: proposing to yield term %d." % (self._node_id, term_number))

        # Create the new election.
        self._create_new_election(term_number = term_number)

        # Create a proposal.
        proposal: LeaderElectionProposal = self._create_election_proposal(ElectionProposalKey.YIELD, term_number)

        # Orchestrate/carry out the election.
        is_leading:bool = await self._handle_election(proposal, target_term_number = term_number)

        # If is_leading is True, then we have a problem, as we proposed YIELD.
        # We should never be elected leader if we propose YIELD.
        if is_leading:
            raise RuntimeError(f"we were elected leader of election {term_number} despite proposing 'YIELD'")

        # Return hard-coded False, as is_leading must be False.
        return False 