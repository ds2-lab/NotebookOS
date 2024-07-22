import io
import os
import base64
import pickle
import asyncio
import faulthandler
import json
import logging
import time
import datetime
import sys

from .errors import FromGoError
from collections import OrderedDict
from typing import Tuple, Callable, Optional, Any, Iterable, Dict, List

from ..smr.smr import NewLogNode, NewConfig, NewBytes, WriteCloser, ReadCloser, CreateBytes, PrintTestMessage
from ..smr.go import Slice_string, Slice_int, Slice_byte
from .log import SyncLog, SynchronizedValue, LeaderElectionVote, LeaderElectionProposal, ElectionProposalKey, KEY_CATCHUP
from .checkpoint import Checkpoint
from .future import Future
from .errors import print_trace, SyncError, GoError, GoNilError
from .reader import readCloser
from .file_log import FileLog
from .election import Election, ElectionState

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
        # data_directory: str = "/storage",
        should_read_data_from_hdfs: bool = False,
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
    
        self.logger: logging.Logger = logging.getLogger(__class__.__name__ + str(id))
        self.logger.info("Creating RaftNode %d now." % id)

        if debug_port <= 1023 or debug_port >= 65535:
            if debug_port == -1:
               self.logger.warn("Debug port specified as -1. Golang HTTP debug server will be disabled.")
            else:
                raise ValueError("Invalid debug port specified.")

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
        self.logger.info("should read data from HDFS: \"%s\"" % should_read_data_from_hdfs)
        self.logger.info("peer_addrs: %s" % peer_addrs)
        self.logger.info("peer_ids: %s" % peer_ids)
        self.logger.info("join: %s" % join)
        self.logger.info("debug_port: %d" % debug_port)

        self.logger.info(">> CALLING INTO GO CODE (NewLogNode)")
        sys.stderr.flush()
        sys.stdout.flush()

        self._log_node = NewLogNode(self._persistent_store_path, id, hdfs_hostname, should_read_data_from_hdfs, Slice_string(peer_addrs), Slice_int(peer_ids), join, debug_port)
        self.logger.info("<< RETURNED FROM GO CODE (NewLogNode)")
        sys.stderr.flush()
        sys.stdout.flush()
        
        self.logger.info(">> CALLING INTO GO CODE (_log_node.ConnectedToHDFS)")
        sys.stderr.flush()
        sys.stdout.flush()
        if self._log_node == None:
            self.logger.error("Failed to create LogNode.")
            sys.stderr.flush()
            sys.stdout.flush()
            raise RuntimeError("Failed to create LogNode.")
        elif not self._log_node.ConnectedToHDFS():
            self.logger.error("The LogNode failed to connect to HDFS.")
            sys.stderr.flush()
            sys.stdout.flush()
            raise RuntimeError("The LogNode failed to connect to HDFS")
        self.logger.info("<< RETURNED FROM GO CODE (_log_node.ConnectedToHDFS)")
        sys.stderr.flush()
        sys.stdout.flush()
        
        self.logger.info(f"Successfully created LogNode {id}.")

        # Mapping from term number to the election associated with that term. 
        self._elections: Dict[int, Election] = {} 
        # The current/active election.
        self._current_election: Optional[Election] = None 
        # The most recent election to have been completed successfully.
        self._last_completed_election: Optional[Election] = None 
        
        # TBD
        self._change_handler: Optional[Callable[[SynchronizedValue], None]] = None 

        # If we receive a proposal with a larger term number than our current election, then it is possible
        # that we simply received the proposal before receiving the associated "execute_request" or "yield_execute" message 
        # that would've prompted us to start the election locally. So, we'll just buffer the proposal for now, and when
        # we receive the "execute_request" or "yield_execute" message, we'll process any buffered proposals at that point.
        #
        # This map maintains the buffered proposals. The mapping is from term number to a list of buffered proposals for that term.
        self._buffered_proposals: dict[int, List[LeaderElectionProposal]] = {}
        # Mapping from term number -> Dict. The inner map is attempt number -> proposal.
        self._proposed_values: OrderedDict[int, OrderedDict[int, LeaderElectionProposal]] = OrderedDict() 

        # Future that is resolved when we propose that somebody win the current election.
        # This future returns the `LeaderElectionVote` that we will propose to nominate/synchronize the winner of the election with our peers.
        self._election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = None 
        # The IO loop on which the `_election_decision_future` is/was created.
        self._future_io_loop: Optional[asyncio.AbstractEventLoop] = None 
        
        # The SynchronizedValue that we propose/append and then wait to see get committed in order to know that we've caught-up with our peers after a migration/restart.
        self._catchup_value: Optional[SynchronizedValue] = None 
        # Future that is created so that we can wait for the `self._catchup_value` to be fully committed. This just returns the committed `_catchup_value`.
        self._catchup_future: Optional[asyncio.Future[SynchronizedValue]] = None 
        # The IO loop that the `self._catchup_future` is created on.
        self._catchup_io_loop: Optional[asyncio.AbstractEventLoop] = None 

        self._ignore_changes: int = 0

        # This can be set such that it will be resolved when close() is called.
        self._closed: Optional[Callable[[str, Exception]]] = None 

        self._heartbeat_tick:int = heartbeat_tick
        self._election_tick:int = election_tick

        # Called by Go (into Python) when a value is committed.
        self._valueCommittedCallback: Callable[[Any, int, str], Any] = self._valueCommitted
        # Called by Go (into Python) when a value is restored (from a checkpoint/backup).
        # Note: this must not be an awaitable/it must not run on an IO loop.
        # Because the Go LogNode::Start function is called by Python from within the asyncio IO loop,
        # the IO loop will be blocked until LogNode::Start returns. We can call Python functions from Go
        # in this scenario, but only if they are not executed on the io loop. 
        self._valueRestoredCallback: Callable[[Any, int], Any] = self._valueRestored 

        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._start_loop: Optional[asyncio.AbstractEventLoop] = None
        
        sys.stderr.flush()
        sys.stdout.flush()

        # This will just do nothing if there's no serialized state to be loaded.
        self._needs_to_catch_up:bool = self._load_and_apply_serialized_state()

        # If we do need to catch up, then we'll create the state necessary to do so now. 
        # As soon as we call `RaftLog::start`, we could begin receiving proposals, so we need this state to exist now.
        # (We compare committed values against `self._catchup_value` when `self._need_to_catch_up` is true.)
        if self._needs_to_catch_up:
            self._catchup_value = SynchronizedValue(None, None, proposer_id = self._node_id, key = KEY_CATCHUP, election_term = self._leader_term_before_migration, should_end_execution = False, operation = KEY_CATCHUP)
            self._catchup_io_loop = asyncio.get_running_loop()
            self._catchup_io_loop.set_debug(True)
            self._catchup_future = self._catchup_io_loop.create_future() 
            self.logger.debug(f"Created new 'catchup value' with ID={self._catchup_value.id}, timestamp={self._catchup_value._timestamp}, and election term={self._catchup_value.election_term}.")

        sys.stderr.flush()
        sys.stdout.flush()

    def _create_persistent_store_directory(self, path: str):
        """
        Create a directory at the specified path if it does not already exist.
        """
        if path != "" and not os.path.exists(path):
            self.logger.debug(f"Creating persistent store directory: \"{path}\"")
            os.makedirs(path, 0o750, exist_ok = True) # It's OK if it already exists.
            self.logger.debug(f"Created persistent store directory \"{path}\" (or it already exists).")

    def _handleVote(self, vote: LeaderElectionVote, received_at = time.time()) -> bytes:
        if self.needs_to_catch_up:
            # TODO: We probably need to keep track of these in case we receive any votes/proposals from the latest election while we're catching up.
            self.logger.warn(f"Discarding LeaderElectionVote, as we need to catch-up: {vote}")
            sys.stderr.flush()
            sys.stdout.flush()
            return GoNilError() 
        
        assert self._current_election != None 

        # The first 'VOTE' proposal received during the term automatically wins.
        was_first_vote_proposal:bool = self._current_election.add_vote_proposal(vote, overwrite = True, received_at = received_at)
        if not was_first_vote_proposal:
            self.logger.debug(f"We've already received at least 1 other 'VOTE' proposal during term {self._current_election.term_number}. Ignoring 'VOTE' proposal from node {vote.proposer_id}.")
            return GoNilError() 
        
        if self._leader_term < vote.election_term:
            self.logger.debug("Our 'leader_term' (%d) < 'election_term' of latest committed 'SYNC' (%d). Setting our 'leader_term' to %d and the 'leader_id' to %d (from newly-committed value)." % (self._leader_term, vote.election_term, vote.election_term, vote.proposed_node_id))
            self._leader_term = vote.election_term
            self._leader_id = vote.proposed_node_id
            self.logger.debug("Node %d has won in term %d as proposed by node %d." % (vote.proposed_node_id, vote.election_term, vote.proposer_id))
            self._current_election.complete_election(vote.proposed_node_id)
            self._last_winner_id = vote.proposed_node_id
            self._last_completed_election = self._current_election
        else:
            self.logger.warn("Our leader_term (%d) >= the 'election_term' of latest committed 'SYNC' message (%d)..." % (self._leader_term, vote.election_term))
        
        # Set the future if the term is expected.
        _leading_future = self._leading_future
        if _leading_future is not None and self._leader_term >= self._expected_term:
            self.logger.debug(f"Scheduling the setting of result on '_leading_future' future to {self._leader_term}.")
            #self._future_io_loop.call_later(0, _leading_future.set_result, self._leader_term) # type: ignore
            assert self._future_io_loop != None
            self._future_io_loop.call_soon_threadsafe(_leading_future.set_result, self._leader_term)
            # leading_future.set_result(self._leader_term)
            self._leading_future = None # Ensure the future is set only once.
            self.logger.debug("Scheduled setting of result on '_leading_future' future.")
        else:
            self.logger.debug("Skipping setting result on _leading_future. _leading_future is None: %s. self._leader_term (%d) >= self._expected_term (%d): %s." % (self._leading_future == None, self._leader_term, self._expected_term, self._leader_term >= self._expected_term))

        self._ignore_changes = self._ignore_changes + 1

        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError()
    
    def _handleProposal(self, proposal: LeaderElectionProposal, received_at: float = 0) -> bytes:
        """Handle a committed LEAD/YIELD proposal.

        Args:
            proposal (LeaderElectionProposal): the committed proposal.
            received_at (float): the time at which we received this proposal.
        """
        # assert self._current_election != None 

        # If we do not have an election upon receiving a proposal, then we buffer the proposal, as we presumably
        # haven't received the associated 'execute_request' or 'yield_execute' message, whereas one of our peer
        # replicas did.
        #
        # Likewise, if we receive a proposal with a larger term number than our current election, then it is possible
        # that we simply received the proposal before receiving the associated "execute_request" or "yield_execute" message 
        # that would've prompted us to start the election locally. So, we'll just buffer the proposal for now, and when
        # we receive the "execute_request" or "yield_execute" message, we'll process any buffered proposals at that point.
        #
        # Also, we check this first before checking if we should simply discard the proposal, in case we receive a legitimate, 
        # new execution request early for some reason. This shouldn't happen, but if it does, we can just buffer the request.
        if self._current_election == None or proposal.election_term > self._current_election.term_number:
            self.logger.warn(f"")
            # Save the proposal in the "buffered proposals" mapping.
            buffered_proposals: List[LeaderElectionProposal] = self._buffered_proposals.get(proposal.election_term, [])
            buffered_proposals.append(proposal)
            self._buffered_proposals[proposal.election_term] = buffered_proposals
            sys.stderr.flush()
            sys.stdout.flush()
            return GoNilError() 

        if self.needs_to_catch_up:
            # TODO: We probably need to keep track of these in case we receive any votes/proposals from the latest election while we're catching up.
            self.logger.warn(f"Discarding LeaderElectionProposal, as we need to catch-up: {proposal}")
            sys.stderr.flush()
            sys.stdout.flush()
            return GoNilError() 
        
        if self._future_io_loop == None:
            try:
                self._future_io_loop = asyncio.get_running_loop()
                self._future_io_loop.set_debug(True)
            except RuntimeError:
                raise ValueError("Future IO loop cannot be nil whilst handling a proposal; attempted to resolve _future_io_loop, but could not do so.")

        time_str = datetime.datetime.fromtimestamp(proposal.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
        term:int = proposal.election_term 
        self.logger.debug("Received {} from node {}. Term {}, attempt {}, timestamp {} ({}), match {}...".format(proposal.key, proposal.proposer_id, term, proposal.attempt_number, proposal.timestamp, time_str, self._node_id == proposal.proposer_id))

        val: Optional[tuple[asyncio.Future[Any], float]] = self._current_election.add_proposal(proposal, self._future_io_loop, received_at = received_at)
        if val != None:
            # Future to decide the result of the election by a certain time limit. 
            _pick_and_propose_winner_future, _discard_after = val

            async def decideElection():
                if self._current_election == None:
                    self.logger.error(f"decideElection called, but current election is None...")
                    raise ValueError("Current election is None in `decideElection` callback.")
                
                current_term: int = self._current_election.term_number
                self.logger.debug(f"decideElection called for election {current_term}.")
                    
                sleep_duration: float = _discard_after - time.time() 
                assert sleep_duration > 0 
                self.logger.debug(f"Sleeping for {sleep_duration} seconds in decideElection coroutine for election {current_term}.")
                await asyncio.sleep(sleep_duration)
                self.logger.debug(f"Done sleeping in decideElection coroutine for election {current_term}.")

                if _pick_and_propose_winner_future.done():
                    self.logger.debug(f"Election {current_term} has already been decided; returning from decideElection coroutine now.")
                    return 

                if self._current_election.term_number != current_term:
                    self.logger.warn(f"Election term has changed in resolve(). Was {current_term}, is now {self._current_election.term_number}.")
                    return 

                try:
                    selected_winner:bool = self._tryPickWinnerToPropose(current_term) 

                    if not selected_winner:
                        if self._current_election.is_active:
                            self.logger.error(f"Could not select a winner for election term {current_term} after timeout period elapsed...")
                            self.logger.error(f"Received proposals: {self._current_election.proposals}")
                            # Note: the timeout period is not set until we receive our first lead proposal, so we should necessarily be able to select a winner
                            raise ValueError(f"Could not decide election term {current_term} despite timeout period elapsing")

                    _pick_and_propose_winner_future.set_result(1) # Generic result set here
                except asyncio.InvalidStateError:
                    self.logger.error("Future for picking and proposing a winner of election term {} has already been resolved...")
            
            if self._future_io_loop == None:
                self.logger.error("Future IO loop is None. Cannot schedule `resolve()` future on loop.")
                raise ValueError("self._future_io_loop is None when it shouldn't be")

            # Schedule `decideElection` to be called.
            # It will sleep until the discardAt time expires, at which point a decision needs to be made.
            # If a decision was already made for that election, then the `decideElection` function will simply return.
            asyncio.run_coroutine_threadsafe(decideElection(), self._future_io_loop)
            # self._future_io_loop.call_soon_threadsafe(decideElection)
        
        self.logger.debug(f"Received {self._current_election.num_proposals_received} proposal(s) so far during term {self._current_election.term_number}.")

        self._tryPickWinnerToPropose(proposal.election_term) 

        self._ignore_changes = self._ignore_changes + 1
        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError() 
    
    def _tryPickWinnerToPropose(self, term_number: int)->bool:
        """
        Try to select a winner to propose for the current election.

        Return True if a winner was selected for proposal (including just proposing 'FAILURE' due to all nodes proposing 'YIELD); otherwise, return False. 
        """
        if self._current_election == None:
            raise ValueError(f"cannot try to pick winner for election {term_number}; current election field is null.")
        
        if self._future_io_loop == None:
            try:
                self._future_io_loop = asyncio.get_running_loop()
                self._future_io_loop.set_debug(True)
            except RuntimeError:
                raise ValueError(f"cannot try to pick winner for election {term_number}; 'future IO loop' field is null, and there is no running IO loop right now.")
            
        try:
            id: int = self._current_election.pick_winner_to_propose(last_winner_id = self._last_winner_id)
            if id > 0:
                assert self._election_decision_future != None 
                self.logger.debug(f"Will propose that node {id} win the election in term {self._current_election.term_number}.")
                self._future_io_loop.call_soon_threadsafe(self._election_decision_future.set_result, LeaderElectionVote(proposed_node_id = id, proposer_id = self._node_id, election_term = term_number, attempt_number = self._current_election.current_attempt_number))
                # self._election_decision_future.set_result(LeaderElectionVote(proposed_node_id = id, proposer_id = self._node_id, election_term = term_number, attempt_number = self._current_election.current_attempt_number))
                return True
            else:
                assert self._election_decision_future != None 
                self.logger.debug(f"Will propose 'FAILURE' for election in term {self._current_election.term_number}.")
                self._future_io_loop.call_soon_threadsafe(self._election_decision_future.set_result, LeaderElectionVote(proposed_node_id = -1, proposer_id = self._node_id, election_term = term_number, attempt_number = self._current_election.current_attempt_number))
                # self._election_decision_future.set_result(LeaderElectionVote(proposed_node_id = -1, proposer_id = self._node_id, election_term = term_number, attempt_number = self._current_election.current_attempt_number))
                return True
        except ValueError as ex:
            self.logger.debug(f"No winner to propose yet for election in term {self._current_election.term_number} because: {ex}")
        
        return False 
    
    def _valueCommitted(self, goObject, value_size: int, value_id: str) -> bytes:
        received_at:float = time.time()

        if value_id != "":
            self.logger.debug(f"Our proposal of size {value_size} bytes was committed.")
        else:
            self.logger.debug(f"Received remote update of size {value_size} bytes")
        
        reader = readCloser(ReadCloser(handle=goObject), value_size)
        
        try:
            committedValue: SynchronizedValue = pickle.load(reader)
        except Exception as ex:
            self.logger.error(f"Failed to unpickle committed value because: {ex}")
            raise ex 
        
        if self.needs_to_catch_up:
            assert self._catchup_value != None 
            assert self._catchup_future != None 
            assert self._catchup_io_loop != None 
            
            if committedValue.key == KEY_CATCHUP and committedValue.proposer_id == self._node_id and committedValue.id == self._catchup_value.id:
                self.logger.debug(f"Received our catch-up value (ID={committedValue.id}, timestamp={committedValue._timestamp}, election term={committedValue.election_term}). We must be caught-up!")
                sys.stderr.flush()
                sys.stdout.flush()
                
                if self._leader_term_before_migration != committedValue.election_term:
                    self.logger.error(f"The leader term before migration was {self._leader_term_before_migration}, while the committed catch-up value has term {committedValue.election_term}. They should be equal.")
                    sys.stderr.flush()
                    sys.stdout.flush()
                    raise ValueError(f"The leader term before migration was {self._leader_term_before_migration}, while the committed catch-up value has term {committedValue.election_term}. They should be equal.")
                
                self._needs_to_catch_up = False 

                self._catchup_io_loop.call_soon_threadsafe(self._catchup_future.set_result, committedValue)
                # self._catchup_future.set_result(committedValue)
                self._catchup_value = None 
                
                self.logger.debug("Scheduled setting of result of catch-up value on catchup future.")
                
                sys.stderr.flush()
                sys.stdout.flush()
                return GoNilError()

        if isinstance(committedValue, LeaderElectionVote):
            return self._handleVote(committedValue, received_at = received_at)
        elif isinstance(committedValue, LeaderElectionProposal):
            return self._handleProposal(committedValue, received_at = received_at)

        # Skip state updates from current node.
        if value_id != "":
            sys.stderr.flush()
            sys.stdout.flush()
            return GoNilError()
        
        if committedValue.election_term < self._leader_term:
            self.logger.warn(f"Committed value has election term {committedValue.election_term} < our leader term of {self._leader_term}...")
            # raise ValueError(f"Leader term of committed value {committedValue.election_term} is less than our current leader term {self._leader_term}")
        
        self.logger.debug(f"Updating self._leader_term from {self._leader_term} to {committedValue.election_term}, the leader term of the committed non-proposal SynchronizedValue.")
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

        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError()

    def _valueRestored_Old(self, rc, sz) -> bytes:
        self.logger.debug(f"Restoring: {rc} {sz}")

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

        self.logger.debug("Restored {}".format(restored))
        return GoNilError()

    # TODO: Debug why, when reading from a read closer and we get to the end, it automatically loops back to the beginning.
    def _valueRestored(self, goObject, aggregate_size: int) -> bytes:
        self.logger.debug(f"Restoring state(s) with combined/aggregate size of {aggregate_size} bytes now...")

        # Set of IDs of SynchronizedValues that have been restored.
        # We use this to monitor for duplicates.
        restored_sync_values: set[str] = set()

        reader = readCloser(ReadCloser(handle=goObject), size = aggregate_size)
        unpickler = pickle.Unpickler(reader)

        syncval = None
        try:
            syncval = unpickler.load()
        except Exception as ex:
            self.logger.error(f"Could not load first synchronized value to restore (aggregate_size = {aggregate_size}) because: {ex}")

        # Recount _ignore_changes
        self._ignore_changes = 0
        restored: int = 0
        # TODO: Debug why, when reading from a read closer and we get to the end, it automatically loops back to the beginning.
        while syncval is not None:
            assert self._change_handler != None 
            # self.logger.debug("Loading next SynchronizedValue to restore.")
            try:
                loaded_value: Optional[SynchronizedValue] = self._load_value(syncval)
            except Exception as ex:
                self.logger.error(f"Unexpected exception encountered while loading SynchronizedValue {syncval}: {ex}")
                return GoError(ex)
            
            if loaded_value.id in restored_sync_values:
                self.logger.error(f"Found duplicate SynchronizedValue during restoration process: {loaded_value}")
                self.logger.error("Previously restored SynchronizedValues:")
                for val in list(restored_sync_values):
                    self.logger.error(val)
                
                # For now, just stop here. I'm not sure why this loops.
                self.logger.debug(f"Restored state with aggregate size of {aggregate_size} bytes. Number of individual values restored: {restored}")
                return GoNilError()
                # return GoError(ValueError(f"Found duplicate SynchronizedValue during restoration process: {loaded_value}"))
            else:
                self.logger.debug(f"Restoring SynchronizedValue: {loaded_value}")

            try:
                self._change_handler(loaded_value)
                restored = restored + 1
            except SyncError as se:
                self.logger.error(f"Error while restoring SynchronizedValue {loaded_value}: {se}")
                return GoError(se)
            except Exception as ex:
                self.logger.error(f"Unexpected exception encountered while restoring SynchronizedValue {loaded_value}: {ex}")
                # return GoError(ex)
            
            restored_sync_values.add(loaded_value.id)

            syncval = None
            loaded_value = None 
            # self.logger.debug(f"syncval before calling load: {syncval}")
            syncval = unpickler.load()
            # self.logger.debug(f"syncval after calling load: {syncval}")

            if syncval != None:
                self.logger.debug(f"Read next Synchronized Value from recovery data: {syncval}")
            else:
                self.logger.debug(f"Got 'None' from recovery data. We're done processing recovered state.")

        self.logger.debug(f"Restored state with aggregate size of {aggregate_size} bytes. Number of individual values restored: {restored}")
        return GoNilError()

    def _load_value(self, val: SynchronizedValue) -> SynchronizedValue:
        """Onload the buffer from the storage server."""
        if type(val.data) is not offloadPath:
            self.logger.debug("Returning synchronization value directly.")
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
            "proposed_values": self._proposed_values,
            "buffered_proposals": self._buffered_proposals,
            "leader_term": self._leader_term,
            "leader_id": self._leader_id,
            "expected_term": self.expected_term,
            "elections": self._elections,
            "current_election": self._current_election,
            "last_completed_election": self._last_completed_election,
        }
        
        return pickle.dumps(data_dict)

    def _load_and_apply_serialized_state(self) -> bool:
        """
        Retrieve the serialized state read by the Go-level LogNode. 
        This state is read from HDFS during migration/error recovery.
        Update our local state with the state retrieved from HDFS.

        Returns:
            (bool) True if serialized state was loaded, indicating that this replica was started after an eviction/migration.
               If no serialized state was loaded, then this simply returns False. 
        """
        self.logger.debug("Loading and applying serialized state. First, retrieving serialized state from LogNode.")

        # PrintTestMessage()

        # a = bytes([0, 1, 2, 3])
        # self.logger.debug(f"Python bytes: {a}")
        # sys.stderr.flush()
        # sys.stdout.flush()
        # b = CreateBytes(10)
        # self.logger.debug(f"Go slice: {b}")
        # sys.stderr.flush()
        # sys.stdout.flush()
        # c = CreateBytes(0)
        # self.logger.debug(f"Go slice: {c}")
        # sys.stderr.flush()
        # sys.stdout.flush()

        # self.logger.debug(f"Python bytes to Go: {Slice_byte.from_bytes(a)}")
        # sys.stderr.flush()
        # sys.stdout.flush()
        # self.logger.debug(f"Go bytes to Python ([3,4,5]): {bytes(Slice_byte([3, 4, 5]))}")
        # sys.stderr.flush()
        # sys.stdout.flush()
        # self.logger.debug(f"Go bytes to Python (b): {bytes(b)}")
        # sys.stderr.flush()
        # sys.stdout.flush()
        # self.logger.debug(f"Go bytes to Python (c): {bytes(c)}")
        # sys.stderr.flush()
        # sys.stdout.flush()
    
        if self._log_node == None:
            self.logger.error("LogNode is None. Cannot retrieve serialized state.")
            sys.stderr.flush()
            sys.stdout.flush()  
            raise ValueError("LogNode is None while trying to retrieve and apply serialized state")

        self.logger.info(">> CALLING INTO GO CODE (_log_node.GetSerializedState)")
        sys.stderr.flush()
        sys.stdout.flush()
        val: Slice_byte = self._log_node.GetSerializedState()
        self.logger.info("<< RETURNED FROM GO CODE (_log_node.GetSerializedState)")
        sys.stderr.flush()
        sys.stdout.flush()
        self.logger.debug(f"Retrieved serialized state from LogNode: {val}")

        try:
            serialized_state_bytes:bytes = bytes(val) # Convert the Go bytes (Slice_byte) to Python bytes.
        except Exception as ex:
            self.logger.error(f"Failed to convert Golang Slice_bytes to Python bytes because: {ex}")
            sys.stderr.flush()
            sys.stdout.flush()
            raise ex 
    
        self.logger.debug("Successfully converted Golang Slice_bytes to Python bytes.")
        # self.logger.debug("Python bytes: ", serialized_state_bytes)
        
        if len(serialized_state_bytes) == 0:
            self.logger.debug("No serialized state found. Nothing to load and apply.")
            return False
        
        try:
            data_dict:dict = pickle.loads(serialized_state_bytes) # json.loads(serialized_state_json)
            if len(data_dict) == 0:
                self.logger.debug("No serialized state found. Nothing to apply.")
                return False
        except Exception as ex:
            self.logger.error(f"Failed to unpickle serialized bytes because: {ex}")
            sys.stderr.flush()
            sys.stdout.flush()
            raise ValueError("Invalid serialized state; could not be unpickled.")
        
        for key, entry in data_dict.items():
            self.logger.debug(f"Retrived state \"{key}\": {str(entry)}")
        
        sys.stderr.flush()
        sys.stdout.flush()
        
        # TODO: 
        # There may be some bugs that arrise from these values being somewhat old or outdated, potentially.
        self._buffered_proposals =  data_dict["buffered_proposals"]
        self._proposed_values = data_dict["proposed_values"]
        self._elections = data_dict["elections"]
        self._current_election = data_dict["current_election"]
        self._last_completed_election = data_dict["last_completed_election"]
        
        # If true, then the "remote updates" that we're receiving are us catching up to where we were before a migration/eviction was triggered.
        # self._catchingUpAfterMigration = True 

        # The value of _leader_term before a migration/eviction was triggered.
        self._leader_term_before_migration: int = data_dict["leader_term"]

        # Commenting these out for now; it's not clear if we should set these in this way yet.
        # self._leader_term = data_dict["leader_term"]
        # self._leader_id = data_dict["leader_id"]
        self._expected_term = data_dict["expected_term"]

        try:
            self._future_io_loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
            self._future_io_loop.set_debug(True)
        except RuntimeError:
            self.logger.error("Failed to get running event loop from asyncio module.")
        
        return True 

    def _get_callback(self, future_name:str = "")-> Tuple[Future, Callable[[str, Exception], Any]]:
        """Get the future object for the specified key."""
        # Prepare callback settings.
        # Callback can be called from a different thread. Schedule the result of the future object to the await thread.
        loop = asyncio.get_running_loop()
        loop.set_debug(True)

        if loop == self._async_loop:
            self.logger.debug("Registering callback future on _async_loop. _async_loop.is_running: %s" % str(self._async_loop.is_running())) # type: ignore
        elif loop == self._start_loop:
            self.logger.debug("Registering callback future on _start_loop. _start_loop.is_running: %s" % str(self._start_loop.is_running())) # type: ignore
        else:
            self.logger.debug("Registering callback future on unknown loop. loop.is_running: %s" % str(loop.is_running()))

        # future: asyncio.Future[Any] = loop.create_future()
        self._async_loop = loop
        
        # def resolve(value, goerr):
        #     err = FromGoError(goerr)
        #     self.logger.debug(f"Resolve Callback called with value {value} and goerr {err}")
        #     if err is None:
        #         future.set_result(value)
        #         self.logger.debug(f"Set result on future with callback: {value}")
        #     else:
        #         future.set_exception(err)
        #         self.logger.debug(f"Set exception on future with callback: {err}")

        future: Future = Future(loop=loop, name = future_name) # type: ignore
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
        if self.has_active_or_failed_election():
            assert self._current_election != None 

            # If we already have an election with a different term number, then that's problematic.
            if self._current_election.term_number != term_number:
                self.logger.error(f"Creating new election with term number {term_number} despite already having an active election with term number {self._current_election.term_number}")
                raise ValueError(f"attempted to create new election while already having an active election")
            else:
                # However, if we have an election with the same term number, then there may have just been some delay in us receiving the 'execute_request' (or 'yield_execute') ZMQ message.
                # During this delay, we may have received a committed proposal from another replica for this election, which prompted us to create the election at that point.
                # So, we'll just return the election that we already have, since the term numbers match. 
                return self._current_election
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
            attempt_number = last_attempt_number + 1 # Could be on one line, but this is more readable in my opinion.

            self.logger.debug(f"Found previous proposal for term {term_number}. Setting attempt number to last attempt number ({last_attempt_number}) + 1 = {attempt_number}")
        else:
            self.logger.debug(f"Found no previous proposal for term {term_number}.")
        
        # Create the new proposal.
        proposal: LeaderElectionProposal = LeaderElectionProposal(key = str(key), proposer_id = self._node_id, election_term = term_number, attempt_number = attempt_number)
        
        # Add the new proposal to the mapping of proposals for the specified term.
        existing_proposals[attempt_number] = proposal 

        # Update the mapping (of proposals for the specified term) in the `self._proposed_values` field.
        self._proposed_values[term_number] = existing_proposals

        # Return the new proposal.
        return proposal 

    async def _offload_value(self, val: SynchronizedValue) -> SynchronizedValue:
        """Offload the buffer to the storage server."""
        # Ensure path exists.
        should_end_execution = val.should_end_execution
        val.set_should_end_execution(False)
        val.set_data(offloadPath(await self._offloader.append(val)))
        val.set_prmap(None)
        val.set_should_end_execution(should_end_execution)
        return val

    async def _append_election_vote(
            self,
            vote: LeaderElectionVote
    ):
        """
        Explicitly propose and append (to the synchronized Raft log) a vote for the winner of the current election.
        """
        await self._serialize_and_append_value(vote)

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
        future, resolve = self._get_callback(future_name = f"append_val[\"{value.key}\"]")
        assert future != None 
        assert resolve != None 
        self.logger.debug(f"Calling 'propose' now for SynchronizedValue: {value}")
        self.logger.info(">> CALLING INTO GO CODE (_log_node.Propose)")
        sys.stderr.flush()
        sys.stdout.flush()
        self._log_node.Propose(NewBytes(dumped), resolve, value.key)
        # await future.result()
        self.logger.info("<< RETURNED FROM GO CODE (_log_node.Propose)")
        sys.stderr.flush()
        sys.stdout.flush()
        self.logger.debug(f"Called 'propose' now for SynchronizedValue: {value}")
        await future.result()
        self.logger.debug(f"Successfully proposed and appended SynchronizedValue: {value}")

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
        # Commented out:
        # This condition is not necessarily true. For example, if we've restarted following a migration, 
        # then it won't be true, as we'll have an active election already when we re-propose something.
        #
        # elif self._current_election.state == ElectionState.ACTIVE or self._current_election.state == ElectionState.COMPLETE:
        #     raise ValueError(f"current election is in state {self._current_election.state}; should be in 'INACTIVE' or 'FAILED' state when `_handle_election()` is called.")

        if self._current_election.is_inactive:
            # Start the election.
            self._current_election.start()
        elif self._current_election.is_in_failed_state:
            # Restart the election.
            self._current_election.restart(latest_attempt_number = proposal.attempt_number)
        
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
        self._future_io_loop = asyncio.get_running_loop()
        self._future_io_loop.set_debug(True)
        # This is the future we'll use to submit a formal vote for who should lead, based on the proposals that are committed to the etcd-raft log.
        self._election_decision_future = self._future_io_loop.create_future()
        # This is the future that we'll use to inform the local kernel replica if it has been selected to "lead" the election (and therefore execute the user-submitted code).
        self._leading_future: Optional[asyncio.Future[int]] = self._future_io_loop.create_future()

        # Create local references.
        _election_decision_future: asyncio.Future[Any] = self._election_decision_future 
        _leading_future: asyncio.Future[int] = self._leading_future 

        buffered_proposals: List[LeaderElectionProposal] = self._buffered_proposals.get(proposal.election_term, [])
        for i, buffered_proposal in enumerate(buffered_proposals):
            self.logger.debug(f"Handling buffered proposal {i+1}/{len(buffered_proposals)} during election term {self._current_election.term_number}: {buffered_proposal}")
            # TODO: Is it OK to just pass the current time for `received_at`? Or should I save the time at which it was received and buffered, and pass that instead?
            self._handleProposal(buffered_proposal, received_at = time.time()) 

        await self._append_election_proposal(proposal)

        voteProposal: LeaderElectionVote = await _election_decision_future
        self._election_decision_future = None 

        # Validate that the term number matches the current election.
        if voteProposal.election_term != self._current_election.term_number:
            raise ValueError(f"received LeaderElectionVote with mis-matched term number ({voteProposal.election_term}) compared to current election term number ({self._current_election.term_number})")
        
        if voteProposal.election_failed:
            self.logger.debug("RaftLog %d: Got decision to propose: election failed. No replicas proposed 'LEAD'." % self._node_id)

            self._current_election.election_failed()
            
            # None of the replicas proposed 'LEAD'
            # It is likely that a migration of some sort will be triggered as a result, leading to another election round for this term.
            return False 
        
        self.logger.debug("RaftLog %d: Appending decision proposal for term %s now." % (self._node_id, voteProposal.election_term))
        await self._append_election_vote(voteProposal)
        self.logger.debug("RaftLog %d: Successfully appended decision proposal for term %s now." % (self._node_id, voteProposal.election_term))

        # Validate the term
        wait, is_leading = self._is_leading(target_term_number)
        if not wait:
            self.logger.debug("RaftLog %d: returning for term %d without waiting, is_leading=%s" % (self._node_id, target_term_number, str(is_leading)))
            return is_leading
        
        # Wait for the future to be set.
        self.logger.debug("Waiting on _leading_future Future to be resolved.")
        await _leading_future
        self.logger.debug("Successfully waited for resolution of _leading_future.")
        self._leading_future = None

        # Validate the term
        wait, is_leading = self._is_leading(target_term_number)
        assert wait == False
        return is_leading

    def sync(self, term):
        """Synchronization changes since specified execution counter."""
        pass

    def reset(self, term, logs: Tuple[SynchronizedValue]):
        """Clear logs equal and before specified term and replaced with specified logs"""
        pass

    def has_active_election(self)->bool:
        """
        Return true if the following two conditions are met:
            - (a): We have an election (i.e., the _current_election field is non-nil)
            - (b): The current election is in the ACTIVE state
        """
        if self.current_election == None:
            return False 
        
        return self.current_election.is_active

    def has_active_or_failed_election(self)->bool:
        """
        Return true if the following two conditions are met:
            - (a): We have an election (i.e., the _current_election field is non-nil)
            - (b): The current election is in the ACTIVE state or the FAILED state
        """
        if self.current_election == None:
            return False 
        
        return self.current_election.is_active or self.current_election.is_in_failed_state

    async def catchup_with_peers(self):
        """
        Propose a new value and wait for it to be committed so as to know that we're "caught up".
        """
        # Ensure that we actually do need to catch up.
        if not self.needs_to_catch_up:
            raise ValueError("no need to catch-up with peers")
        
        # Ensure that the "catchup" value has already been created.
        if self._catchup_value == None:
             raise ValueError("\"catchup\" value is None")
        
        # Ensure that the "catchup" IO loop is set so that Golang code (that has called into Python code)
        # can populate the "catchup" Future with a result (using the "catchup" IO loop).
        if self._catchup_io_loop == None:
             raise ValueError("\"catchup\" IO loop is None")
        
        # Ensure that the "catchup" future has been created already.
        if self._catchup_future == None:
             raise ValueError("\"catchup\" future is None")

        await self.append(self._catchup_value) 
        await self._catchup_future # Wait for the value to be committed.

        # Reset these fields after we're done.
        self._catchup_future = None 
        self._catchup_io_loop = None 
        self._catchup_value = None # This should already be None at this point; we set it to None in the 'value committted' handler.

    async def append(self, value: SynchronizedValue):
        """
        Append some data to the synchronized Raft log.
        """
        if value.key != str(ElectionProposalKey.LEAD) and value.key != str(ElectionProposalKey.YIELD):
            self.logger.debug(f"Updating self._leader_term from {self._leader_term} to {value.election_term}, the election term of the SynchronizedValue (with key=\"{value.key}\") that we're appending.")
            self._leader_term = value.election_term

        if not value.has_operation:
            # Count _ignore_changes
            self._ignore_changes += 1

        # Ensure key is specified.
        if value.key is not None:
            if value.data is not None and type(value.data) is bytes and len(value.data) > MAX_MEMORY_OBJECT:
                value = await self._offload_value(value)

        await self._serialize_and_append_value(value)

    async def add_node(self, node_id, address):
        """Add a node to the etcd-raft cluster."""
        self.logger.info("Adding node %d at addr %s to the SMR cluster." % (node_id, address))
        future, resolve = self._get_callback(future_name = f"add_node[{node_id}]")
        self.logger.info(">> CALLING INTO GO CODE (_log_node.AddNode)")
        sys.stderr.flush()
        sys.stdout.flush()
        self._log_node.AddNode(node_id, address, resolve)
        self.logger.info("<< RETURNED FROM GO CODE (_log_node.AddNode)")
        sys.stderr.flush()
        sys.stdout.flush()
        res = await future.result()
        # await future 
        # res = future.result()
        self.logger.info("Result of AddNode: %s" % str(res))

    async def update_node(self, node_id, address):
        """Add a node to the etcd-raft  cluster."""
        self.logger.info("Updating node %d with new addr %s." % (node_id, address))
        future, resolve = self._get_callback(future_name = f"update_node[{node_id}]")
        self.logger.info(">> CALLING INTO GO CODE (_log_node.UpdateNode)")
        sys.stderr.flush()
        sys.stdout.flush()
        self._log_node.UpdateNode(node_id, address, resolve)
        self.logger.info("<< RETURNED FROM GO CODE (_log_node.UpdateNode)")
        sys.stderr.flush()
        sys.stdout.flush()
        res = await future.result()
        # await future 
        # res = future.result()
        self.logger.info("Result of UpdateNode: %s" % str(res))

    async def remove_node(self, node_id):
        """Remove a node from the etcd-raft cluster."""
        self.logger.info("Removing node %d from the SMR cluster." % node_id)
        future, resolve = self._get_callback(future_name = f"remove_node[{node_id}]")

        try:
            self.logger.info(">> CALLING INTO GO CODE (_log_node.RemoveNode)")
            sys.stderr.flush()
            sys.stdout.flush()
            self._log_node.RemoveNode(node_id, resolve)
            self.logger.info("<< RETURNED FROM GO CODE (_log_node.RemoveNode)")
            sys.stderr.flush()
            sys.stdout.flush()
        except Exception as ex:
            self.logger.info("<< RETURNED FROM GO CODE (_log_node.RemoveNode)")
            sys.stderr.flush()
            sys.stdout.flush()
            self.logger.error("Error in LogNode while removing replica %d: %s" % (node_id, str(ex)))

        res = await future.result()
        # await future 
        # res = future.result()
        self.logger.info("Result of RemoveNode: %s" % str(res))
    
    @property 
    def needs_to_catch_up(self) -> bool:
        """
        If we loaded serialized state when we were started, then we were created during a migration/eviction procedure.

        As a result, we should propose a new value and watch for it to be committed, so that we know that we're up-to-date with the rest of the cluster.
        """
        return self._needs_to_catch_up

    @property
    def term(self) -> int:
        """Current leader term."""
        if self._current_election != None and self._current_election.term_number != self._leader_term:
            self.logger.warn(f"Returning _leader_term of {self._leader_term}; however, _current_election.term_number = {self._current_election.term_number}")

        return self._leader_term

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
    def num_changes(self) -> int:
        """The number of incremental changes since first term or the latest checkpoint."""
        self.logger.info(">> CALLING INTO GO CODE (_log_node.NumChanges)")
        sys.stderr.flush()
        sys.stdout.flush()
        num_changes:int = self._log_node.NumChanges()
        self.logger.info("<< RETURNED FROM GO CODE (_log_node.NumChanges)")
        sys.stderr.flush()
        sys.stdout.flush()

        return num_changes - self._ignore_changes

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
        faulthandler.dump_traceback_later(timeout = 30, repeat = True, file = sys.stderr, exit = False)
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
        self._async_loop.set_debug(True)
        self._start_loop = self._async_loop

        self.logger.info(">> CALLING INTO GO CODE (_log_node.Start)")
        sys.stderr.flush()
        sys.stdout.flush()

        startSuccessful: bool = self._log_node.Start(config)
        self.logger.info("<< RETURNED FROM GO CODE (_log_node.Start)")
        sys.stderr.flush()
        sys.stdout.flush()

        
        if not startSuccessful:
            self.logger.error("Failed to start LogNode.")
            raise RuntimeError("failed to start the Golang-level LogNode component")
        
        self.logger.info("Successfully started RaftLog and LogNode.")

    # Close the LogNode's HDFS client.
    def closeHdfsClient(self)->None:
        self.logger.info(">> CALLING INTO GO CODE (_log_node.CloseHdfsClient)")
        sys.stderr.flush()
        sys.stdout.flush()

        self._log_node.CloseHdfsClient()
        
        self.logger.info("<< RETURNED FROM GO CODE (_log_node.CloseHdfsClient)")
        sys.stderr.flush()
        sys.stdout.flush()


    # IMPORTANT: This does NOT close the HDFS client within the LogNode. 
    # This is because, when migrating a raft cluster member, we must first stop the raft
    # node before copying the contents of its data directory.
    #
    # To close the HDFS client within the LogNode, call the `closeHdfsClient` method.
    def close(self)->None:
        """
        Ensure all async coroutines have completed. Clean up resources. Stop the LogNode.
        """
        self.logger.warn(f"Closing LogNode {self._node_id} now.")
        
        self.logger.info(">> CALLING INTO GO CODE (_log_node.Close)")
        sys.stderr.flush()
        sys.stdout.flush()

        self._log_node.Close()
        
        self.logger.info("<< RETURNED FROM GO CODE (_log_node.Close)")
        sys.stderr.flush()
        sys.stdout.flush()

        
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
        
        future, resolve = self._get_callback(future_name = "write_data_hdfs")
        
        self.logger.info(">> CALLING INTO GO CODE (_log_node.WriteDataDirectoryToHDFS)")
        sys.stderr.flush()
        sys.stdout.flush()

        # This will return immediately, as the actual work of the method is performed by a separate goroutine.
        self._log_node.WriteDataDirectoryToHDFS(Slice_byte(serialized_state), resolve)
        
        self.logger.info("<< RETURNED FROM GO CODE (_log_node.WriteDataDirectoryToHDFS)")
        sys.stderr.flush()
        sys.stdout.flush()
        
        # Wait for the data to be written to HDFS without blocking the IO loop.
        waldir_path:str = await future.result()
        return waldir_path

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