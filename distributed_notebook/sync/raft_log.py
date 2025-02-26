import asyncio
import logging
import os
import pickle
import random
import threading
import traceback
from collections import OrderedDict
from pickle import PickleError
from typing import Tuple, Callable, Optional, Any, Iterable, Dict, List

import debugpy
import sys
import time

from distributed_notebook.kernel.iopub_notifier import IOPubNotification

from .checkpoint import Checkpoint
from .election import Election, ElectionAlreadyDecidedError, ElectionNotStartedError
from .errors import (
    print_trace,
    SyncError,
    GoError,
    GoNilError,
    InconsistentTermNumberError,
    DiscardMessageError,
)
from .file_log import FileLog
from .future import Future
from .log import (
    SynchronizedValue,
    LeaderElectionVote,
    BufferedLeaderElectionVote,
    LeaderElectionProposal,
    BufferedLeaderElectionProposal,
    ElectionProposalKey,
    KEY_CATCHUP,
    ExecutionCompleteNotification,
)
from .reader import readCloser
from ..logs import ColoredLogFormatter
from ..smr.go import Slice_string, Slice_int, Slice_byte
from ..smr.smr import LogNode, NewLogNode, NewConfig, NewBytes, WriteCloser, ReadCloser

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
            node_id: int,
            kernel_id: str,
            base_path: str = "/store",
            remote_storage_hostname: str = "172.17.0.1:9000",
            remote_storage: str = "hdfs",
            load_data_from_remote_storage: bool = False,
            peer_addresses: Optional[Iterable[str]] = None,
            peer_ids: Optional[Iterable[int]] = None,
            num_replicas: int = 3,
            join: bool = False,
            debug_port: int = 8464,
            heartbeat_tick: int = 10,  # Raft-related
            election_tick: int = 1,  # Raft-related
            skip_create_log_node: bool = False,
            report_error_callback: Callable[[str, str], None] = None,
            send_notification_func: Callable[[str, str, int], None] = None,
            remote_storage_read_latency_callback: Optional[Callable[[int], None]] = None,
            fast_forward_execution_count_handler: Callable[[], None] = None,
            set_execution_count_handler: Callable[[int], None] = None,
            loaded_serialized_state_callback: Callable[[dict[str, Any]], None] = None,
            shell_io_loop: asyncio.AbstractEventLoop = None,
            election_timeout_seconds: float = 10,
            deployment_mode: str = "LOCAL",
            send_iopub_notification: Callable[[IOPubNotification, Optional[Dict[str, Any]]], None] = None,
    ):
        self._snapshotCallback = None
        self._shouldSnapshotCallback = None
        if len(remote_storage_hostname) == 0:
            raise ValueError("RemoteStorage hostname is empty.")

        if peer_addresses is None:
            peer_addresses = []

        if peer_ids is None:
            peer_ids = []

        self.log: logging.Logger = logging.getLogger(
            __class__.__name__ + str(node_id)
        )
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        self.log.info("Creating RaftNode %d now." % node_id)

        if debug_port <= 1023 or debug_port >= 65535:
            if debug_port < 0:
                self.log.warning(
                    "Debug port specified as -1. Golang HTTP debug server will be disabled."
                )
            else:
                raise ValueError(f"Invalid debug port specified: {debug_port}")

        self._kernel_id = kernel_id

        # The term that the leader is expecting.
        self._expected_term: int = 0
        # Updated after a LEAD call. This is the term of the LEADER. Used to check if received proposals are old/new.
        self._leader_term: int = 0
        # The id of the leader.
        self._leader_id: int = 0
        self._send_iopub_notification: Callable[[IOPubNotification, Optional[Dict[str, Any]]], None] = send_iopub_notification
        self._shell_io_loop: asyncio.AbstractEventLoop = shell_io_loop
        self._persistent_store_path: str = base_path
        self._node_id: int = node_id
        self._offloader: FileLog = FileLog(self._persistent_store_path)
        self._num_replicas: int = num_replicas
        self._last_winner_id: int = -1
        self._report_error_callback = report_error_callback
        self._send_notification_func = send_notification_func
        self._deployment_mode = deployment_mode
        self._leader_term_before_migration: int = -1
        self._restore_namespace_time_seconds: float = 0.0
        self._received_vote_future: Optional[asyncio.Future] = None
        self._fast_forward_execution_count_handler: Callable[[], None] = (
            fast_forward_execution_count_handler
        )
        self._set_execution_count_handler: Callable[[int], None] = (
            set_execution_count_handler
        )
        self._loaded_serialized_state_callback: Callable[
            [dict[str, dict[str, Any]]], None
        ] = loaded_serialized_state_callback

        # How long to wait to receive other proposals before making a decision (if we can, like if we
        # have at least received one LEAD proposal).
        self._election_timeout_sec = election_timeout_seconds

        try:
            self._create_persistent_store_directory(base_path)
        except Exception as ex:
            self.log.error(f'Error while creating persistent datastore directory "{base_path}": {ex}')

        self.log.info("persistent store path: %s" % self._persistent_store_path)
        self.log.info('remote storage hostname: "%s"' % remote_storage_hostname)
        self.log.info('remote_storage: "%s"', remote_storage)
        self.log.info('should read data from RemoteStorage: "%s"' % load_data_from_remote_storage)
        self.log.info("peer addresses: %s" % peer_addresses)
        self.log.info("peer smr node IDs: %s" % peer_ids)
        self.log.info("join: %s" % join)
        self.log.info("debug_port: %d" % debug_port)

        self.log.info(">> CALLING INTO GO CODE (NewLogNode)")
        sys.stderr.flush()
        sys.stdout.flush()

        if not skip_create_log_node:
            self._log_node: LogNode = self.create_log_node(
                node_id=node_id,
                remote_storage_hostname=remote_storage_hostname,
                remote_storage=remote_storage,
                should_read_data=load_data_from_remote_storage,
                peer_addrs=peer_addresses,
                peer_ids=peer_ids,
                join=join,
                debug_port=debug_port,
                deployment_mode=deployment_mode,
            )

        self.log.info(f"Successfully created LogNode {node_id}.")

        if hasattr(self, "_log_node") and self._log_node is not None:
            remote_storage_read_latency: int = self._log_node.RemoteStorageReadLatencyMilliseconds()
            if remote_storage_read_latency > 0:
                self.log.debug(f"Retrieved remote storage read latency of {remote_storage_read_latency} "
                               f"milliseconds from LogNode.")

                if remote_storage_read_latency_callback is not None:
                    remote_storage_read_latency_callback(remote_storage_read_latency)
                else:
                    self.log.warning("Callback for reporting remote storage read latency is None. "
                                     "Cannot report remote storage read latency.")

        # Indicates whether we've created the first Election / at least one Election
        self.__created_first_election: bool = False

        # Mapping from term number to the election associated with that term.
        self._elections: Dict[int, Election] = {}
        self._elections_by_jupyter_message_id: Dict[str, Election] = {}
        # The current/active election.
        self._current_election: Optional[Election] = None
        # The most recent election to have been completed successfully.
        self._last_completed_election: Optional[Election] = None
        # Control access to key parts of the election.
        self._election_lock: threading.Lock = threading.Lock()

        # TBD
        self._change_handler: Optional[Callable[[SynchronizedValue], None]] = None

        # The number of elections we've skipped.
        self._num_elections_skipped: int = 0

        # If we receive a proposal with a larger term number than our current election, then it is possible
        # that we simply received the proposal before receiving the associated "execute_request" or "yield_request" message
        # that would've prompted us to start the election locally. So, we'll just buffer the proposal for now, and when
        # we receive the "execute_request" or "yield_request" message, we'll process any buffered proposals at that point.
        #
        # This map maintains the buffered proposals. The mapping is from term number to a list of buffered proposals for that term.
        self._buffered_proposals: dict[int, List[BufferedLeaderElectionProposal]] = {}
        # Ensures atomic access to the _buffered_proposals dictionary (required because we may be switching between
        # multiple Python threads/goroutines that are accessing the _buffered_proposals dictionary).
        self._buffered_proposals_lock: threading.Lock = threading.Lock()

        # _buffered_votes serves the same purpose as _buffered_proposals, but _buffered_votes is for LeaderElectionVote
        # objects, whereas _buffered_proposals is for LeaderElectionProposal objects.
        self._buffered_votes: dict[int, List[BufferedLeaderElectionVote]] = {}
        # Ensures atomic access to the _buffered_votes dictionary (required because we may be switching between
        # multiple Python threads/goroutines that are accessing the _buffered_votes dictionary).
        self._buffered_votes_lock: threading.Lock = threading.Lock()

        # Mapping from term number -> Dict. The inner map is attempt number -> proposal.
        self._proposed_values: OrderedDict[
            int, OrderedDict[int, LeaderElectionProposal]
        ] = OrderedDict()

        # Future that is resolved when we propose that somebody win the current election.
        # This future returns the `LeaderElectionVote` that we will propose to nominate/synchronize the winner of the election with our peers.
        self._election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = (
            None
        )
        # The IO loop on which the `_election_decision_future` is/was created.
        self._future_io_loop: Optional[asyncio.AbstractEventLoop] = None

        # The _fallback_future_io_loop is used if self._future_io_loop is None when we receive a proposal.
        # This can occur if we receive a proposal from a peer before receiving the "execute_request" message
        # (or equivalently the "yield_request" message) from our Local Daemon.
        #
        # The _fallback_future_io_loop is set to the Shell handler's IO loop, and should be set shortly after the
        # kernel is created.
        self._fallback_future_io_loop: Optional[asyncio.AbstractEventLoop] = None

        # The SynchronizedValue that we propose/append and then wait to see get committed in order to know that we've caught-up with our peers after a migration/restart.
        self._catchup_value: Optional[SynchronizedValue] = None
        # Future that is created so that we can wait for the `self._catchup_value` to be fully committed. This just returns the committed `_catchup_value`.
        self._catchup_future: Optional[asyncio.Future[SynchronizedValue]] = None
        # The IO loop that the `self._catchup_future` is created on.
        self._catchup_io_loop: Optional[asyncio.AbstractEventLoop] = None

        self._ignore_changes: int = 0

        # This can be set such that it will be resolved when close() is called.
        self._closed: Optional[Callable[[str, Exception], None]] = None

        self._heartbeat_tick: int = heartbeat_tick
        self._election_tick: int = election_tick

        # Called by Go (into Python) when a value is committed.
        self._valueCommittedCallback: Callable[[Any, int, str], Any] = (
            self.__value_committed_wrapper
        )
        # Called by Go (into Python) when a value is restored (from a checkpoint/backup).
        # Note: this must not be an awaitable/it must not run on an IO loop.
        # Because the Go LogNode::Start function is called by Python from within the asyncio IO loop,
        # the IO loop will be blocked until LogNode::Start returns. We can call Python functions from Go
        # in this scenario, but only if they are not executed on the io loop.
        self._valueRestoredCallback: Callable[[Any, int], Any] = self.__value_restored

        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._start_loop: Optional[asyncio.AbstractEventLoop] = None

        sys.stderr.flush()
        sys.stdout.flush()

        self._restoration_time_seconds: float = 0.0
        if hasattr(self, "_log_node"):
            # This will just do nothing if there's no serialized state to be loaded.
            self._needs_to_catch_up: bool = self.load_and_apply_serialized_state()
        else:
            self._needs_to_catch_up: bool = False

        # If we do need to catch up, then we'll create the state necessary to do so now.
        # As soon as we call `RaftLog::start`, we could begin receiving proposals, so we need this state to exist now.
        # (We compare committed values against `self._catchup_value` when `self._need_to_catch_up` is true.)
        catchup_start_time: float = time.time()

        def caught_up_callback(f: Any):
            self._restore_namespace_time_seconds = time.time() - catchup_start_time
            self.log.debug(f"Restored user namespace in {self.restore_namespace_time_seconds:,} seconds.")

        if self._needs_to_catch_up:
            # We pass the last election term, as we don't want to win the current election.
            # That is, if we pass self._leader_term_before_migration + 1 as the election term,
            # then all our peers will set their leader_term fields to _leader_term_before_migration + 1,
            # and then when we try to restart the election, the term of the election to be restarted,
            # which is equal to _leader_term_before_migration + 1, will be equal to _leader_term_before_migration
            # + 1. And that's wrong. If we're holding an election, then the leader_term is supposed to be
            # less than the term of the election. We're electing a leader for that term.
            self._catchup_value = SynchronizedValue(
                None,
                None,
                proposer_id=self._node_id,
                key=KEY_CATCHUP,
                election_term=self._leader_term_before_migration,
                should_end_execution=False,
                operation=KEY_CATCHUP,
            )
            self._catchup_io_loop = asyncio.get_running_loop()
            self._catchup_io_loop.set_debug(True)
            self._catchup_future = self._catchup_io_loop.create_future()
            self._catchup_future.add_done_callback(caught_up_callback)
            self.log.debug(
                f"Created new 'catchup value' with ID={self._catchup_value.id}, timestamp={self._catchup_value.timestamp}, and election term={self._catchup_value.election_term}."
            )

        sys.stderr.flush()
        sys.stdout.flush()

    @property
    def future_io_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        return self._future_io_loop

    @property
    def fallback_future_io_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        return self._fallback_future_io_loop

    @fallback_future_io_loop.setter
    def fallback_future_io_loop(self, loop: Optional[asyncio.AbstractEventLoop]):
        self._fallback_future_io_loop = loop

    def __str__(self):
        return f'RaftLog[KernelId={self._kernel_id},NodeID={self._node_id}]'

    def __repr__(self):
        return f'RaftLog[KernelId={self._kernel_id},NodeID={self._node_id}]'

    def set_fast_forward_executions_handler(
            self, fast_forward_execution_count_handler: Callable[[], None]
    ):
        self._fast_forward_execution_count_handler = (
            fast_forward_execution_count_handler
        )

    def set_set_execution_count_handler(
            self, set_execution_count_handler: Callable[[int], None]
    ):
        self._set_execution_count_handler = set_execution_count_handler

    def create_log_node(
            self,
            node_id: int,
            remote_storage_hostname: str = "172.17.0.1:9000",
            remote_storage: str = "hdfs",
            should_read_data: bool = False,
            peer_addrs: Iterable[str] = None,
            peer_ids: Iterable[int] = None,
            join: bool = False,
            debug_port: int = 8464,
            deployment_mode: str = "LOCAL",
    ) -> LogNode:
        if peer_ids is None:
            peer_ids = []

        if peer_addrs is None:
            peer_addrs = []

        log_node: LogNode = NewLogNode(
            self._persistent_store_path,
            node_id,
            remote_storage_hostname,
            remote_storage,
            should_read_data,
            Slice_string(peer_addrs),
            Slice_int(peer_ids),
            join,
            debug_port,
            deployment_mode,
        )
        self.log.info("<< RETURNED FROM GO CODE (NewLogNode)")
        sys.stderr.flush()
        sys.stdout.flush()

        self.log.info(">> CALLING INTO GO CODE (_log_node.ConnectedToRemoteStorage)")
        sys.stderr.flush()
        sys.stdout.flush()
        if log_node is None:
            self.log.error("Failed to create LogNode.")
            sys.stderr.flush()
            sys.stdout.flush()
            raise RuntimeError("Failed to create LogNode.")
        elif not log_node.ConnectedToRemoteStorage():
            self.log.error("The LogNode failed to connect to RemoteStorage.")
            sys.stderr.flush()
            sys.stdout.flush()
            raise RuntimeError("The LogNode failed to connect to RemoteStorage")
        self.log.info(
            "<< RETURNED FROM GO CODE (_log_node.ConnectedToRemoteStorage)"
        )
        sys.stderr.flush()
        sys.stdout.flush()

        return log_node

    @property
    def election_decision_future(self) -> Optional[asyncio.Future[LeaderElectionVote]]:
        """
        Future that is resolved when we propose that somebody win the current election.

        This future returns the `LeaderElectionVote` that we will propose to nominate/synchronize
        the winner of the election with our peers.
        """
        return self._election_decision_future

    @property
    def node_id(self) -> int:
        """
        Return this node's SMR node ID.
        """
        return self._node_id

    def _create_persistent_store_directory(self, path: str):
        """
        Create a directory at the specified path if it does not already exist.
        """
        if path != "" and not os.path.exists(path):
            self.log.debug(f'Creating persistent store directory: "{path}"')
            os.makedirs(path, 0o750, exist_ok=True)  # It's OK if it already exists.
            self.log.debug(f'Created persistent store directory "{path}" (or it already exists).')
        elif path == "":
            self.log.warning("Persistent store specified as empty string. Skipping directory creation.")
        elif os.path.exists(path):
            self.log.debug(f'Persistent store path "{path}" already exists. Skipping directory creation.')

    def __buffer_vote(
            self, vote: LeaderElectionVote, received_at: float = time.time()
    ) -> bytes:
        # Save the vote in the "buffered votes" dictionary.
        with self._buffered_votes_lock:
            buffered_votes: List[BufferedLeaderElectionVote] = self._buffered_votes.get(vote.election_term, [])
            buffered_votes.append(BufferedLeaderElectionVote(vote=vote, received_at=received_at))
            self._buffered_votes[vote.election_term] = buffered_votes
            sys.stderr.flush()
            sys.stdout.flush()
            return GoNilError()

    def __handle_vote(
            self,
            vote: LeaderElectionVote,
            received_at=time.time(),
            buffered_vote: bool = True,
    ) -> bytes:
        """
        Handle a vote proposal.

        :param vote: the vote proposal that we've received.
        :param received_at: the time at which we received the vote proposal.
        :param buffered_vote: if True, then we're handling a buffered vote proposal, and thus we should not buffer it again.
        """
        if self.needs_to_catch_up:
            if (
                    vote.election_term > self._leader_term_before_migration
                    and self._current_election is not None
                    and vote.attempt_number > self._current_election.current_attempt_number
            ):
                self.log.warning(f"Received vote from term {vote.election_term} "
                                 f"(with attempt number {vote.attempt_number})."
                                 f"The vote's term is > the election term prior "
                                 f"to our migration (i.e., {self._leader_term_before_migration}). "
                                 f"Buffering vote now: {vote}")
                self.__buffer_vote(vote, received_at=received_at)
                sys.stderr.flush()
                sys.stdout.flush()
                return GoNilError()
            else:
                self.log.debug(f"Discarding old LeaderElectionVote from term {vote.election_term} "
                               f"with attempt number {vote.attempt_number}, "
                               f"as we need to catch-up: {vote}")
                sys.stderr.flush()
                sys.stdout.flush()
                return GoNilError()

        # If we receive an old vote out-of-order or after a delay, then we can just discard it.
        if self._current_election is not None and vote.election_term < self._current_election.term_number:
            self.log.warning(
                f'Received old vote for node "{vote.proposed_node_id}" from node {vote.proposer_id} '
                f"with term number {vote.election_term}, while our current election is for term  "
                f"{self._current_election.term_number}... Will just discard the vote."
            )
            sys.stderr.flush()
            sys.stdout.flush()
            return GoNilError()

        # If we do not have an election upon receiving a vote, then we buffer the vote, as we presumably
        # haven't received the associated 'execute_request' or 'yield_request' message, whereas one of our peer
        # replicas did.
        #
        # Likewise, if we receive a vote with a larger term number than our current election, then it is possible
        # that we simply received the vote before receiving the associated "execute_request" or "yield_request" message
        # that would've prompted us to start the election locally. So, we'll just buffer the vote for now, and when
        # we receive the "execute_request" or "yield_request" message, we'll process any buffered proposals and
        # buffered votes at that point.
        #
        # Also, we check this first before checking if we should simply discard the vote, in case we receive a legitimate,
        # new execution request early for some reason. This shouldn't happen, but if it does, we can just buffer the request.
        if self._current_election is None:
            self.log.warning(
                f'Received vote for node "{vote.proposed_node_id}" from node {vote.proposer_id} '
                f"while our local election is None. Match: {self._node_id == vote.proposer_id}. "
                f"Current election is None? {self._current_election is None}. "
                f"Proposal term: {vote.election_term}. Will buffer vote for now. "
                f"Proposal: {str(vote)}"
            )

            # This is basically just a sanity check to make sure we don't somehow get
            # stuck in a loop of buffering the same vote(s) over and over again.
            if not buffered_vote:
                raise ValueError("We're already handling a buffered vote. We should not be trying to buffer it again!")

            return self.__buffer_vote(vote, received_at=received_at)
        elif vote.election_term > self.current_election_term:
            self.log.warning(
                f'Received vote for node "{vote.proposed_node_id}" from node {vote.proposer_id} '
                f"from future election term {vote.election_term} "
                f"while local election is for term {self.current_election_term}. "
                f"Match: {self._node_id == vote.proposer_id}. Will buffer vote for now. "
                f"Proposal: {str(vote)}"
            )

            # This is basically just a sanity check to make sure we don't somehow get
            # stuck in a loop of buffering the same vote(s) over and over again.
            if not buffered_vote:
                raise ValueError("We're already handling a buffered vote. We should not be trying to buffer it again!")

            return self.__buffer_vote(vote, received_at=received_at)

        self.log.debug(f"Received VOTE: {str(vote)}")

        # The first 'VOTE' proposal received during the term automatically wins.
        with self._election_lock:
            was_first_vote_proposal: bool = self._current_election.add_vote_proposal(
                vote, overwrite=True, received_at=received_at)

        if not was_first_vote_proposal:
            self.log.debug(f"We've already received at least 1 other 'VOTE' "
                           f"proposal during term {self._current_election.term_number}. "
                           f"Ignoring 'VOTE' proposal from node {vote.proposer_id}.")
            return GoNilError()

        if self._leader_term < vote.election_term:
            self.log.debug(f"Our 'leader_term' ({self._leader_term}) < 'election_term' of latest committed 'SYNC' "
                           f"({vote.election_term}). Setting our 'leader_term' to {vote.election_term} and the "
                           f"'leader_id' to {vote.proposed_node_id} (from newly-committed value).")
            self._leader_term = vote.election_term
            self._leader_id = vote.proposed_node_id

            if vote.proposed_node_id == self.node_id:
                self.log.debug(f"We have won in term {vote.election_term} as proposed by node {vote.proposer_id}.")
            else:
                self.log.debug("Node %d has won in term %d as proposed by node %d."
                               % (vote.proposed_node_id, vote.election_term, vote.proposer_id))
            
            if self._send_iopub_notification is not None:
                self._send_iopub_notification(
                    IOPubNotification.ElectionFirstVoteCommitted,
                    {
                        "term_number": vote.election_term,
                        "proposer_id": vote.proposer_id,
                        "proposed_node_id": vote.proposed_node_id,
                        "kernel_id": self._kernel_id,
                        "node_id": self.node_id
                    }
                )

            with self._election_lock:
                self._current_election.set_election_vote_completed(vote.proposed_node_id)

                _received_vote_future = self._received_vote_future
                if (
                        _received_vote_future is not None
                        and vote.election_term == self._received_vote_future_term
                        and not _received_vote_future.done()
                ):
                    self._received_vote_future = None
                    self._future_io_loop.call_soon_threadsafe(_received_vote_future.set_result, vote)

            self._last_winner_id = vote.proposed_node_id
            self._last_completed_election = self._current_election
        else:
            self.log.warning("Our leader_term (%d) >= the 'election_term' of latest committed 'SYNC' message (%d)..."
                             % (self._leader_term, vote.election_term))

        # Set the future if the term is expected.
        _leading_future = self._leading_future
        if _leading_future is not None and self._leader_term >= self._expected_term:
            self.log.debug(f"Scheduling the setting of result on '_leading_future' future to {self._leader_term}."            )
            # self._future_io_loop.call_later(0, _leading_future.set_result, self._leader_term) # type: ignore

            if self._future_io_loop is None and self._fallback_future_io_loop is not None:
                self.log.warning("Our 'future' IO loop is None. Attempting to use the fallback 'future' IO loop...")
                self._future_io_loop = self._fallback_future_io_loop
                self._future_io_loop.set_debug(True)

            if self._future_io_loop is None:
                self.log.error(f"Our 'future' IO loop is None; we cannot schedule "
                               f"result of '_leading_future' for term {self._leader_term}...")
                sys.stderr.flush()
                sys.stdout.flush()
                raise ValueError(f"'future' IO loop is None while trying to schedule"
                                 f"result of '_leading_future' during term {self._leader_term}")

            def set_leading_future_result(term):
                self.log.debug(f"Setting result of _leading_future to {term} now.")
                _leading_future.set_result(term)
                self.log.debug(f"Set result of _leading_future to {term}.")

            self._future_io_loop.call_soon_threadsafe(set_leading_future_result, self._leader_term)
            # leading_future.set_result(self._leader_term)
            self._leading_future = None  # Ensure the future is set only once.
            self.log.debug("Scheduled setting of result on '_leading_future' future.")
        else:
            self.log.debug(
                "Skipping setting result on _leading_future. _leading_future is None: %s. self._leader_term (%d) >= self._expected_term (%d): %s."
                % (
                    self._leading_future is None,
                    self._leader_term,
                    self._expected_term,
                    self._leader_term >= self._expected_term,
                )
            )

        self._ignore_changes = self._ignore_changes + 1

        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError()

    def __fast_forward_to_future_election(self, notification: ExecutionCompleteNotification) -> bytes:
        """
        Fast-forward to a future election upon receiving an ExecutionCompleteNotification with term number
        greater than that of the local, current election.

        For example, if our local election is for term 5, and we receive an ExecutionCompleteNotification for
        term 6 (or 7, or 10, or 1,000,000), then we will mark our current, local election as having finished.
        Then, we'll create a new election object for the term number specified in the ExecutionCompleteNotification,
        and we'll mark that as complete.
        """

        # Make sure that we actually need to fast-forward.
        if (
                self.current_election is not None
                and notification.election_term <= self.current_election.term_number
        ):
            self.log.warning(
                f"Instructed to fast-forward, however ExecutionCompleteNotification has term number "
                f"{notification.election_term} and current, local election has term number "
                f"{self.current_election.term_number}, so no fast-forward is required..."
            )

            return GoNilError()

        # If our local election is non-null, then we can compute how many terms we're skipping and print a log
        # message indicating as such. We can also be sure to skip the current election.
        current_term_number: int = 0
        if self.current_election is not None:
            current_term_number = self.current_election.term_number
            num_terms_to_skip: int = notification.election_term - current_term_number
            self.log.debug(
                f"Fast-forwarding from election term {current_term_number} to election term "
                f"{notification.election_term}. Skipping ahead by {num_terms_to_skip} term number(s). "
                f"Existing local election is/was for term {self.current_election.term_number} and is "
                f"in state {self.current_election.state.get_name()}."
            )

            # If our local election hasn't been started yet, then start it.
            if self._current_election.is_inactive:
                self.log.debug(
                    f"Fast-forwarding election {current_term_number} to ACTIVE state "
                    f"from state {self._current_election.election_state.get_name()}"
                )
                self._current_election.start()

            # If we've not finished the voting phase in our current election, then do that next.
            if not self._current_election.voting_phase_completed_successfully:
                self.log.debug(f"Fast-forwarding election {current_term_number} to VOTING_COMPLETE state "
                               f"from state {self._current_election.election_state.get_name()}")
                self._current_election.set_election_vote_completed(notification.proposer_id)

                # TODO: Should we try setting value on "vote received" future here?

            # Now designate the current election as complete (skipped, specifically, in this case).
            if not self._current_election.code_execution_completed_successfully:
                self.log.debug(f"Fast-forwarding election {current_term_number} to EXECUTION_COMPLETE state "
                               f"(and subsequently to the SKIPPED state) from current state "
                               f"of {self._current_election.election_state.get_name()}")
                self._current_election.set_execution_complete(fast_forwarding=True)
                self._num_elections_skipped += 1

                try:
                    self._set_execution_count_handler(current_term_number)
                except ValueError:
                    self.log.warning(f"Failed to set execution count to {current_term_number}; "
                                     f"current value must be higher...")
                    pass

                # self._fast_forward_execution_count_handler()
        else:
            self.log.debug(f"Fast-forwarding from election term {current_term_number} to election term "
                           f"{notification.election_term}. Skipping ahead by {notification.election_term} "
                           f"term number(s). We do not have an active/existing local election as of right now.")

        # Define a function to create and skip elections so we can skip ahead as far as is necessary.
        def create_and_skip_election(
                election_term: int,
                set_election_complete: bool,
                jupyter_message_id: str = "",
        ):
            """
            Create an election for the specified term, optionally skipping it immediately.
            """
            if election_term < 0:
                raise ValueError(f"Invalid term number while creating and skipping election: {election_term}")

            self.log.debug(f"Creating election {election_term} during fast-forward. "
                           f"set_election_complete={set_election_complete}, jupyter_message_id={jupyter_message_id}")

            # Create a new election.
            election: Election = Election(
                election_term,
                self._num_replicas,
                jupyter_message_id,
                timeout_seconds=self._election_timeout_sec,
            )
            self._elections[election_term] = election

            if jupyter_message_id != "":
                if jupyter_message_id in self._elections_by_jupyter_message_id:
                    # TODO: What should we do here?
                    self.log.error(f"We already have an election associated with Jupyter msg '{jupyter_message_id}': "
                                   f"{self._elections_by_jupyter_message_id[jupyter_message_id]}")

                self._elections_by_jupyter_message_id[jupyter_message_id] = election

            # Elections contain a sort of (singly-)linked list between themselves.
            # We're performing an append-to-end-of-linked-list operation here.
            self._last_completed_election = self._current_election
            self._current_election = election

            # Start the election.
            self._current_election.start()

            # We know who won the voting in the election for which we received the "execute complete" notification --
            # it's whichever node proposed/appended the "execution complete" notification.
            self._current_election.set_election_vote_completed(notification.proposer_id)

            # TODO: Should we try setting value on "vote received" future here?

            if set_election_complete:
                self.log.debug(
                    f"Calling 'set_execution_complete' to fully skip election {election_term}."
                )
                self.current_election.set_execution_complete(fast_forwarding=True)
                self._num_elections_skipped += 1

                try:
                    self._set_execution_count_handler(election_term)
                except ValueError:
                    self.log.warning(
                        f"Failed to set execution count to {election_term}; current value must be higher..."
                    )
                    pass

                # self._fast_forward_execution_count_handler()
            else:
                self.log.debug(
                    f"Not calling 'set_execution_complete' while creating & skipping election {election_term}."
                )

        self.log.debug(
            f"Creating and skipping election(s) from term {current_term_number + 1} to term "
            f"{notification.election_term}."
        )

        # Create and entirely skip any elections between the current one and the election right before the
        # one for which we just received the "execution complete" notification.
        #
        # So, if we're on term 5, and we just got an "execute complete" notification for term 10, then we'll create
        # and immediately skip elections 6, 7, 8, and 9. We'll handle the election for term 10 after the for-loop.
        for term_number in range(current_term_number + 1, notification.election_term):
            create_and_skip_election(term_number, True)

        # We call this one more time outside the for-loop so that we can pass set_election_complete as False instead of True.
        # The reason we don't want to call set_election_complete here is that __fast_forward_to_future_election is called
        # by __handle_execution_complete_notification. We want to skip ahead so that our current election has the same
        # term number as the "execution complete" notification, so that we can just call set_election_complete like
        # normal in __handle_execution_complete_notification. So, we skip ahead, calling set_election_complete for all
        # elections from term i to term j-1, where i is the term of whatever election we happen to have currently, and
        # term j is the term of the "execution complete" notification. Then, for term j, we create the election and
        # skip through the phases such that the election for term j is in the state right before execution completes
        # (i.e., voting phase complete / execution phase started). So, when we return from this method (i.e., the
        # __fast_forward_to_future_election method), we're ready to call set_election_complete on the election for
        # term j in __handle_execution_complete_notification.
        create_and_skip_election(
            notification.election_term,
            False,
            jupyter_message_id=notification.jupyter_message_id,
        )

        self.log.debug(
            f"Finished creating and skipping election(s) from term {current_term_number + 1} to term "
            f"{notification.election_term}. Our local election now has term {self.current_election.term_number} "
            f"and is in state {self.current_election.state.get_name()}."
        )

        self._leader_id = notification.proposer_id
        self._leader_term = notification.election_term

        return GoNilError()

    def __handle_execution_complete_notification_while_catching_up(self, notification: ExecutionCompleteNotification):
        if notification.election_term > self._leader_term_before_migration:
            self.log.warning(f"Received ExecutionCompleteNotification from future term {notification.election_term} "
                             f"which is > the election term prior to our migration "
                             f"(i.e., {self._leader_term_before_migration}). "
                             f"But the election shouldn't be able to end until we've caught-up and started "
                             f"participating again...")
            raise ValueError(f"Received ExecutionCompleteNotification from term {notification.election_term} "
                             f"with attempt number {notification.attempt_number}, "
                             f"which is >= the election term prior to our migration "
                             f"(i.e., {self._leader_term_before_migration}).")

        # This case should be covered by the above case, but just in case it isn't...
        if notification.election_term > self._current_election.term_number:
            self.log.warning(f"Received ExecutionCompleteNotification from future term {notification.election_term} "
                             f"which is > the election term prior to our migration "
                             f"(i.e., {self._current_election.term_number}). "
                             f"But the election shouldn't be able to end until we've caught-up and started "
                             f"participating again...")
            raise ValueError(f"Unexpected case. Received ExecutionCompleteNotification from term "
                             f"{notification.election_term} with attempt number {notification.attempt_number}, "
                             f"which is >= our current election term of {self._current_election.term_number}.")

        # If this leader notification is strictly old, then we'll just ignore it.
        if self._current_election.term_number > notification.election_term:
            self.log.debug(f"Discarding old ExecutionCompleteNotification from old term={notification.election_term} "
                           f", attempt_number={notification.attempt_number}, as we need to catch-up: {notification}")
            return

        self.log.debug(f"Received ExecutionCompleteNotification for current election term "
                       f"{notification.election_term} while catching up. "
                       f"Current election state: {self._current_election.state.get_name()}")
        assert self._current_election.term_number == notification.election_term

        # First, check if we know that the voting phase has completed.
        # If not, then we'll update that first.
        if not self.current_election.voting_phase_completed_successfully:
            self.log.debug(f"We first must record that the voting phase for election {notification.election_term} "
                           f"has completed, as we apparently didn't know that before migrating...")

            with self._election_lock:
                self.current_election.set_election_vote_completed(notification.proposer_id)

        # Now, check if we know that the code execution completed successfully.
        # If we know about it already, then we'll just return.
        if self._current_election.code_execution_completed_successfully:
            self.log.debug(f"Discarding ExecutionCompleteNotification from current term {notification.election_term} with "
                           f"attempt number {notification.attempt_number}, as we already know that election finished: "
                           f"{notification}")
            return

        # Record that the code execution phase completed successfully.
        with self._election_lock:
            self.log.debug(f"Recording that election for term {notification.election_term} has completed. "
                           f"Learned about this whilst catching up.")
            self.current_election.set_execution_complete(
                catching_up=True,
                fast_forwarding=False,
                fast_forwarded_winner_id=notification.proposer_id
            )

    def __handle_inconsistent_term_numbers(self, notification: ExecutionCompleteNotification)->bool:
        """
        Called when handling an ExecutionCompleteNotification with an unexpected term number.

        Return a boolean indicating whether we're fast-forwarding now.
        """
        notification_term: int = notification.election_term

        self.log.warning(f"Current election is for term {self.current_election.term_number} "
                         f"(state={self.current_election.state.get_name()}), "
                         f"but we just received a notification that election "
                         f"{notification_term} has finished...")

        if notification_term > self.current_election.term_number:
            self.__fast_forward_to_future_election(notification)
            # fast_forwarding = True
            return True

        # This may be an error state, or we may be receiving this notification late / after a migration.
        # That is, we may have been migrated before the Python handler for the "execution complete" notification
        # for the previous election finished. So, we're handling it post-migration. We may even have an
        # "execute_request" message that is blocked, waiting for the old election to complete. Let's see.
        prior_election: Optional[Election] = self._elections.get(notification_term, None)

        # If we don't even have an election with this term, then something is seriously wrong.
        # We already know the term number is a mismatch, and that it's not greater than ours.
        # So, it's less than ours, but we don't have a record of an election from that term? Bad.
        if prior_election is None:
            self.log.error(f"Inconsistent term numbers. Current: {self.current_election.term_number}. "
                           f"Notification: {notification_term}. "
                           f"We don't even have an election for term {notification_term}...")

            raise InconsistentTermNumberError(
                f"Inconsistent term numbers. Current: {self.current_election.term_number}. "
                f"Notification: {notification_term}. We don't even have an election for term {notification_term}...",
                election=self.current_election,
                value=notification,
            )

        # If the prior election is already recorded as having been completed successfully,
        # then indeed this is an error. Not necessarily the end of the world, but we shouldn't
        # be receiving this notification now.
        if prior_election.code_execution_completed_successfully:
            self.log.error(f"Inconsistent term numbers. Current: {self.current_election.term_number}. "
                           f"Notification: {notification_term}. Election from term {notification_term} is "
                           f"already marked as having completed successfully...")

            raise InconsistentTermNumberError(
                f"Inconsistent term numbers. Current election: {self.current_election.term_number}. "
                f"Notification: {notification_term}.",
                election=self.current_election,
                value=notification,
            )

        self.log.debug(f"Received 'old' ExecutionCompleteNotification for term {notification_term}; "
                       f"however, election {notification_term} hasn't been recorded as complete yet.")

        # First, check if we know that the voting phase has completed.
        # If not, then we'll update that first.
        if not prior_election.voting_phase_completed_successfully:
            self.log.debug(f"We first must record that the voting phase for previous election {notification_term} "
                           f"has completed, as we apparently didn't know that already...")

            with self._election_lock:
                prior_election.set_election_vote_completed(notification.proposer_id)

        # Now, check if we know that the code execution completed successfully.
        # If we know about it already, then we'll just return.
        if prior_election.code_execution_completed_successfully:
            self.log.debug(f"Discarding ExecutionCompleteNotification from previous term {notification_term} with "
                           f"attempt number {notification.attempt_number}, as we already know that election finished: "
                           f"{notification}")
            return False

        # Record that the code execution phase completed successfully.
        with self._election_lock:
            self.log.debug(f"Recording that election for term {notification_term} has completed. Learned about this "
                           f"at an unusual time -- perhaps due to an inconveniently-timed migration.")

            try:
                prior_election.set_execution_complete(
                    catching_up=True,
                    fast_forwarding=False,
                    fast_forwarded_winner_id=notification.proposer_id
                )
            except ValueError:
                self.log.warning(f"Apparently nobody was waiting to learn that "
                                 f"old election {notification_term} has finished...")

        return False

    def __handle_execution_complete_notification(self, notification: ExecutionCompleteNotification) -> bytes:
        """
        Handles a ExecutionCompleteNotification indicating that code execution has completed for a particular election.

        :param notification: the ExecutionCompleteNotification that we received
        """
        self.log.debug(f'Received "execution complete" notification for election term '
                       f"{notification.election_term} from node {notification.proposer_id}.")

        if self.needs_to_catch_up:
            self.__handle_execution_complete_notification_while_catching_up(notification)
            sys.stderr.flush()
            sys.stdout.flush()
            return GoNilError()

        with self._election_lock:
            fast_forwarding: bool = False

            if self.current_election is None:
                self.log.warning(f"We just received a notification that code execution has completed for "
                                 f"election {notification.election_term}; however, our current election is nil...")
                self.__fast_forward_to_future_election(notification)
                fast_forwarding = True
            elif self.current_election.term_number != notification.election_term:
                fast_forwarding = self.__handle_inconsistent_term_numbers(notification)

            if self.leader_id != notification.proposer_id:
                self.log.warning(f'Current leader ID is {self.leader_id}, but we just received an '
                                 f'"election finished" notification with proposer ID = {notification.proposer_id}. '
                                 f'Notification term number: {notification.election_term}. Our local election term: '
                                 f'{self.current_election_term}. Leader term: {self.leader_term}.')

                # Pretty sure this is a bug/race condition of some sort, in which something updated our leader term,
                # but not our leader ID. Maybe the leader checkpointed some state right before sending the 'execution
                # complete' notification, and so we updated our leader term but not our leader ID. (I don't think that
                # specific scenario is possible, since we still need to process a vote before this point, and if we
                # process the vote, then we'd have updated our leader ID then, unless we discarded the vote for some
                # reason?) In any case, if our leader term matches the notification's term, and if our current election
                # is the one for which we just received the notification, then we'll just update our leader ID, but we'll
                # still send an error notification so that I can potentially debug this.
                if self.leader_term == notification.election_term and self.current_election_term == notification.election_term:
                    self._leader_id = notification.proposer_id

                    # This is more of a 'warning report' rather than an 'error report', strictly speaking.
                    self._report_error_callback(f"Inconsistency detected between our local leader ID and the "
                                                f"proposer ID of 'election finished' notification.",
                                                f'Leader ID: {self.leader_id}. "Election finished" '
                                                f'notification proposer ID: {notification.proposer_id}. '
                                                f"Notification term number: {notification.election_term}. "
                                                f"Our local election term: {self.current_election_term}. "
                                                f"Leader term: {self.leader_term}.")
                else:
                    raise ValueError(f'Inconsistency detected between our local leader ID '
                                     f'and the proposer ID of "election finished" notification. '
                                     f'Leader ID: {self.leader_id}. "Election finished" notification proposer ID: '
                                     f"{notification.proposer_id}. "
                                     f"Notification term number: {notification.election_term}. "
                                     f"Our local election term: {self.current_election_term}. "
                                     f"Leader term: {self.leader_term}.")

            self.current_election.set_execution_complete(
                fast_forwarding=fast_forwarding,
                fast_forwarded_winner_id=notification.proposer_id,
            )

            if fast_forwarding:
                self._num_elections_skipped += 1
                try:
                    self._set_execution_count_handler(notification.election_term)
                except ValueError:
                    self.log.warning(f"Failed to set execution count to {notification.election_term}; "
                                     f"current value must be higher...")
                    pass

        return GoNilError()

    def __buffer_proposal(self, proposal: LeaderElectionProposal, received_at: float = time.time()) -> bytes:
        # Save the proposal in the "buffered proposals" mapping.
        with self._buffered_proposals_lock:
            buffered_proposals: List[BufferedLeaderElectionProposal] = (
                self._buffered_proposals.get(proposal.election_term, [])
            )
            buffered_proposals.append(BufferedLeaderElectionProposal(proposal=proposal, received_at=received_at))
            self._buffered_proposals[proposal.election_term] = buffered_proposals
            sys.stderr.flush()
            sys.stdout.flush()
            return GoNilError()

    def __handle_proposal(
            self, proposal: LeaderElectionProposal, received_at: float = 0
    ) -> bytes:
        """Handle a committed LEAD/YIELD proposal.

        Args:
            proposal (LeaderElectionProposal): the committed proposal.
            received_at (float): the time at which we received this proposal.
        """
        # assert self._current_election is not None

        # If we do not have an election upon receiving a proposal, then we buffer the proposal, as we presumably
        # haven't received the associated 'execute_request' or 'yield_request' message, whereas one of our peer
        # replicas did.
        #
        # Likewise, if we receive a proposal with a larger term number than our current election, then it is possible
        # that we simply received the proposal before receiving the associated "execute_request" or "yield_request" message
        # that would've prompted us to start the election locally. So, we'll just buffer the proposal for now, and when
        # we receive the "execute_request" or "yield_request" message, we'll process any buffered proposals and
        # buffered votes at that point.
        #
        # Also, we check this first before checking if we should simply discard the proposal, in case we receive a legitimate,
        # new execution request early for some reason. This shouldn't happen, but if it does, we can just buffer the request.
        if self._current_election is None:
            self.log.warning(
                f'Received proposal "{proposal.key}" from node {proposal.proposer_id} '
                f"while our local election is None. Match: {self._node_id == proposal.proposer_id}. "
                f"Current election is None? {self._current_election is None}. "
                f"Proposal term: {proposal.election_term}. Will buffer proposal for now. "
                f"Proposal: {str(proposal)}"
            )
            return self.__buffer_proposal(proposal, received_at=received_at)
        elif proposal.election_term > self._current_election.term_number:
            self.log.warning(
                f'Received proposal "{proposal.key}" from node {proposal.proposer_id} '
                f"from future election term {proposal.election_term} "
                f"while local election is for term {self._current_election.term_number}. "
                f"Match: {self._node_id == proposal.proposer_id}. Will buffer proposal for now. "
                f"Proposal: {str(proposal)}"
            )
            return self.__buffer_proposal(proposal, received_at=received_at)

        if self.needs_to_catch_up:
            if (
                    proposal.election_term > self._leader_term_before_migration
                    and proposal.attempt_number
                    > self._current_election.current_attempt_number
            ):
                # TODO: We probably need to keep track of these in case we receive any votes/proposals from the latest election while we're catching up.
                self.log.warning(f"Received proposal from term {proposal.election_term} "
                                 f"(with attempt number {proposal.attempt_number})."
                                 f"The proposal's term is > the election term prior to our migration "
                                 f"(i.e., {self._leader_term_before_migration}). Buffering proposal now: {proposal}.")
                self.__buffer_proposal(proposal, received_at=received_at)
                sys.stderr.flush()
                sys.stdout.flush()
                return GoNilError()
            else:
                self.log.debug(f"Discarding old LeaderElectionProposal from term {proposal.election_term} "
                               f"with attempt number {proposal.attempt_number}, "
                               f"as we need to catch-up: {proposal}")
                sys.stderr.flush()
                sys.stdout.flush()
                return GoNilError()

        if self._future_io_loop is None:
            try:
                self._future_io_loop = asyncio.get_running_loop()
                self._future_io_loop.set_debug(True)
            except RuntimeError:
                if self._fallback_future_io_loop is not None:
                    self.log.warning("Our 'future' IO loop is None. Attempting to use the fallback 'future' IO loop...")
                    self._future_io_loop = self._fallback_future_io_loop
                    self._future_io_loop.set_debug(True)
                else:
                    raise ValueError("Future IO loop cannot be nil whilst handling a proposal; "
                                     "attempted to resolve _future_io_loop, but could not do so.")

        self.log.debug(f'Received proposal "{proposal.key}" from node {proposal.proposer_id}: '
                       f'{str(proposal)}. Match: {self._node_id == proposal.proposer_id}.')

        with self._election_lock:
            # val will only be non-None if this is the first LEAD proposal we're receiving for this election term.
            val: Optional[tuple[asyncio.Future[Any], float]] = (
                self._current_election.add_proposal(
                    proposal, self._future_io_loop, received_at=received_at
                )
            )

        if val is not None:
            if self._send_iopub_notification is not None:
                self._send_iopub_notification(
                    IOPubNotification.ElectionFirstLeadProposalCommitted,
                    {
                        "term_number": proposal.election_term,
                        "proposer_id": proposal.proposer_id,
                        "kernel_id": self._kernel_id,
                        "node_id": self.node_id
                    }
                )
            
            # Future to decide the result of the election by a certain time limit.
            _pick_and_propose_winner_future, _discard_after = val

            async def decide_election():
                if self._current_election is None:
                    self.log.error("decide_election called, but current election is None...")
                    raise ValueError("Current election is None in `decide_election` callback.")

                current_term: int = self._current_election.term_number

                sleep_duration: float = _discard_after - time.time()
                assert sleep_duration > 0
                self.log.debug(f"decide_election called for election {current_term}. "
                               f"Sleeping for {sleep_duration} seconds in decide_election coroutine for election {current_term}.")
                await asyncio.sleep(sleep_duration)
                self.log.debug(f"Woke up in decide_election call for election {current_term}.")

                if _pick_and_propose_winner_future.done():
                    self.log.debug(f"Election {current_term} has already been decided; "
                                   f"returning from decide_election coroutine now.")
                    return

                if self._current_election.term_number != current_term:
                    self.log.warning(f"Election term has changed in resolve(). "
                                     f"Was {current_term}, is now {self._current_election.term_number}.")
                    return

                try:
                    picked_a_winner: bool = self.__try_pick_winner_to_propose(
                        current_term
                    )

                    if not picked_a_winner:
                        if self._current_election.is_active:
                            self.log.error(f"Could not select a winner for election term {current_term} "
                                           f"after timeout period elapsed...")
                            self.log.error(f"Received proposals: {self._current_election.proposals}")
                            # Note: the timeout period is not set until we receive our first lead proposal,
                            # so we should necessarily be able to select a winner
                            raise ValueError(f"Could not decide election term {current_term} "
                                             f"despite timeout period elapsing")

                    # Commented-out:
                    # We already set this future's result inside the Election class when we call
                    # __try_pick_winner_to_propose (which eventually calls methods of the Election class).from
                    #
                    # We caught the error and continued immediately, so I don't think it caused any problems,
                    # but still. It's unnecessary.
                    #
                    # _pick_and_propose_winner_future.set_result(1)  # Generic result set here
                except asyncio.InvalidStateError as ex:
                    self.log.error(f"Future for picking and proposing a winner of election term {current_term} "
                                   f"has already been resolved...: {ex}")

            if self._future_io_loop is None:
                if self._fallback_future_io_loop is not None:
                    self.log.warning("Our 'future' IO loop is None. Attempting to use the fallback 'future' IO loop...")
                    self._future_io_loop = self._fallback_future_io_loop
                    self._future_io_loop.set_debug(True)
                else:
                    self.log.error("Future IO loop is None. Cannot schedule `resolve()` future on loop.")
                    raise ValueError("self._future_io_loop is None when it shouldn't be")

            # Schedule `decide_election` to be called.
            # It will sleep until the discardAt time expires, at which point a decision needs to be made.
            # If a decision was already made for that election, then the `decide_election` function will simply return.
            self.decide_election_future: asyncio.Future = (
                asyncio.run_coroutine_threadsafe(decide_election(), self._future_io_loop)
            )
        else:
            self.log.debug(
                f'No future returned after registering "{proposal.election_proposal_key}" proposal '
                f"from node {proposal.proposer_id} with election for term "
                f"{self._current_election.term_number}. "
                f"Must not have been the first proposal for that election."
            )

        self.log.debug(
            f"Received {self._current_election.num_proposals_received} proposal(s) "
            f"and discarded {self._current_election.num_discarded_proposals} proposal(s) "
            f"so far during term {self._current_election.term_number}."
        )

        self.__try_pick_winner_to_propose(proposal.election_term)

        self._ignore_changes = self._ignore_changes + 1
        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError()

    def __try_pick_winner_to_propose(self, term_number: int) -> bool:
        """
        Try to select a winner to propose for the current election.

        Returns:
            True if a winner was selected for proposal (including just proposing 'FAILURE' due to all nodes
            proposing 'YIELD'); otherwise, return False.
        """
        self.log.debug(f"Trying to pick winner for election {term_number}.")

        if self._current_election is None:
            raise ValueError(f"cannot try to pick winner for election {term_number}; "
                             f"current election field is null.")

        if self._current_election.voting_phase_completed_successfully:
            self.log.debug(f"Voting phase has already completed for election {term_number}.")
            return False

        if self._future_io_loop is None:
            try:
                self._future_io_loop = asyncio.get_running_loop()
                self._future_io_loop.set_debug(True)
            except RuntimeError:
                if self._fallback_future_io_loop is not None:
                    self.log.warning("Our 'future' IO loop is None. Attempting to use the fallback 'future' IO loop...")
                    self._future_io_loop = self._fallback_future_io_loop
                    self._future_io_loop.set_debug(True)
                else:
                    raise ValueError(f"cannot try to pick winner for election {term_number}; "
                                     f"'future IO loop' field is null, and there is no running IO loop right now.")

        try:
            # Select a winner.
            with self._election_lock:
                id_of_winner_to_propose: int = (
                    self._current_election.pick_winner_to_propose(
                        last_winner_id=self._last_winner_id
                    )
                )

            if id_of_winner_to_propose > 0:
                assert self._election_decision_future is not None
                self.log.debug(f"Will propose that node {id_of_winner_to_propose} "
                               f"win the election in term {self._current_election.term_number}.")
                self._future_io_loop.call_soon_threadsafe(
                    self._election_decision_future.set_result,
                    LeaderElectionVote(
                        proposed_node_id=id_of_winner_to_propose,
                        jupyter_message_id=self._current_election.jupyter_message_id,
                        proposer_id=self._node_id,
                        election_term=term_number,
                        attempt_number=self._current_election.current_attempt_number,
                    ),
                )
                return True
            else:
                assert self._election_decision_future is not None
                self.log.debug(f"Will propose 'FAILURE' for election in term {self._current_election.term_number}.")
                self._future_io_loop.call_soon_threadsafe(
                    self._election_decision_future.set_result,
                    LeaderElectionVote(
                        proposed_node_id=-1,
                        jupyter_message_id=self._current_election.jupyter_message_id,
                        proposer_id=self._node_id,
                        election_term=term_number,
                        attempt_number=self._current_election.current_attempt_number,
                    ),
                )
                return True
        except ElectionAlreadyDecidedError as ex:
            self.log.debug(f"Winner already selected for election {term_number}.")
        except ElectionNotStartedError as ex:
            self.log.error(f"ElectionNotStartedError encountered while trying "
                           f"to pick winner for election {term_number}: {ex}")
            raise ex  # Re-raise.
        except ValueError as ex:
            self.log.debug(f"No winner to propose yet for election in term "
                           f"{self._current_election.term_number} because: {ex}")

        return False

    def __value_committed_wrapper(
            self, goObject, value_size: int, value_id: str
    ) -> bytes:
        """
        Wrapper around RaftLog::_valueCommitted so I can print the return value, as apparently we're sometimes returning nil?
        """
        self.log.debug(f'Calling self._valueCommitted with value ID="{value_id}" of size {value_size}.')
        sys.stderr.flush()
        sys.stdout.flush()
        ret = None
        try:
            ret = self.__value_committed(goObject, value_size, value_id)
        except Exception as ex:
            self.log.error(
                f"Exception encountered in self._valueCommitted while handling synchronized value with "
                f"ID=\"{value_id}\" of size {value_size} bytes: {str(ex)}. Traceback: "
                f"{''.join(traceback.format_exception(type(ex), ex, ex.__traceback__, 99))}"
            )
            print_trace(limit=10)
            sys.stderr.flush()
            sys.stdout.flush()
            self._report_error_callback(
                "Exception While Processing Committed Value",
                f"{type(ex).__name__}: {str(ex)}",
            )
        finally:
            # self.logger.debug(f"Returning from self._valueCommitted with value: {ret}")
            sys.stderr.flush()
            sys.stdout.flush()

            if ret is None:
                self.log.error("We were about to return None from the value-changed handler...")
                ret = b""

        return ret

    def __deserialize_go_object(self, goObject) -> SynchronizedValue:
        reader = readCloser(ReadCloser(handle=goObject))

        try:
            committedValue: SynchronizedValue = pickle.load(reader)
        except Exception as ex:
            self.log.error(f"Failed to unpickle committed value because: {ex}")
            raise ex

        return committedValue

    def __value_committed(self, goObject, value_size: int, value_id: str) -> bytes:
        sys.stderr.flush()
        sys.stdout.flush()
        received_at: float = time.time()

        if value_id != "":
            self.log.debug(f"Our proposal of size {value_size} bytes was committed. "
                           f"type(goObject): {type(goObject).__name__}")
        else:
            self.log.debug(f"Received remote update of size {value_size} bytes. "
                           f"type(goObject): {type(goObject).__name__}")

        if isinstance(goObject, SynchronizedValue):
            committedValue: SynchronizedValue = goObject
        else:
            committedValue: SynchronizedValue = self.__deserialize_go_object(goObject)

        self.log.debug(f"Value of type {type(committedValue).__name__} and size {value_size} bytes has been "
                       "committed to the RaftLog. Handling now...")

        if self.needs_to_catch_up:
            assert self._catchup_value is not None
            assert self._catchup_future is not None
            assert self._catchup_io_loop is not None

            if (
                    committedValue.key == KEY_CATCHUP
                    and committedValue.proposer_id == self._node_id
                    and committedValue.id == self._catchup_value.id
            ):
                return self.__catchup_value_committed(committedValue)

        if isinstance(committedValue, LeaderElectionVote):
            return self.__handle_vote(committedValue, received_at=received_at)
        elif isinstance(committedValue, LeaderElectionProposal):
            return self.__handle_proposal(committedValue, received_at=received_at)
        elif isinstance(committedValue, ExecutionCompleteNotification):
            return self.__handle_execution_complete_notification(committedValue)

        # Skip state updates from current node.
        if value_id != "":
            sys.stderr.flush()
            sys.stdout.flush()
            return GoNilError()

        self.log.debug(f"Received SynchronizedValue: {str(committedValue)}")

        if committedValue.election_term < self._leader_term:
            self.log.warning(f"Committed value has election term {committedValue.election_term} < "
                             f"our leader term of {self._leader_term}...")

        self.log.debug(
            f"Updating self._leader_term from {self._leader_term} to {committedValue.election_term}, "
            f"the leader term of the committed non-proposal SynchronizedValue."
        )
        self._leader_term = committedValue.election_term

        # For values synchronized from other replicas or replayed, count _ignore_changes
        if not committedValue.has_operation:
            self._ignore_changes = self._ignore_changes + 1

        assert self._change_handler is not None
        try:
            self._change_handler(self._load_value(committedValue))
        except Exception as ex:
            self.log.error(f"Failed to handle changed value because: {ex}")
            print_trace(limit=10)
            raise ex

        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError()

    def __catchup_value_committed(self, catchupValue: SynchronizedValue):
        """
        Handler for when the 'catchup' value is committed, indicating that we've fully caught-up to our peers.
        """
        self.log.debug(
            f"Received our catch-up value (ID={catchupValue.id}, timestamp={catchupValue.timestamp}, "
            f"election term={catchupValue.election_term}). We must be caught up!\n\n"
        )
        sys.stderr.flush()
        sys.stdout.flush()

        if self._leader_term_before_migration != catchupValue.election_term:
            self.log.error(
                f"The leader term before migration was {self._leader_term_before_migration}, "
                f'while the committed "catch-up" value has term {catchupValue.election_term}. '
                f'The term of the "catch-up" value should be equal to last leader term.'
            )
            # f"The term of the \"catch-up\" value should be one greater than the last leader term.")
            sys.stderr.flush()
            sys.stdout.flush()
            raise ValueError(
                f"The leader term before migration was {self._leader_term_before_migration}, "
                f'while the committed "catch-up" value has term {catchupValue.election_term}. '
                f'The term of the "catch-up" value should be equal to last leader term.'
            )
            # f"The term of the \"catch-up\" value should be one greater than the last leader term.")

        self._needs_to_catch_up = False

        self._catchup_io_loop.call_soon_threadsafe(
            self._catchup_future.set_result, catchupValue
        )
        self._catchup_value = None

        self.log.debug(
            "Scheduled setting of result of catch-up value on catchup future."
        )

        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError()

    def __value_restored_old(self, rc, sz) -> bytes:
        sys.stderr.flush()
        sys.stdout.flush()
        self.log.debug(f"Restoring: {rc} {sz}")

        reader = readCloser(ReadCloser(handle=rc), sz)
        unpickler = pickle.Unpickler(reader)

        synchronizedValue: Optional[SynchronizedValue] = None
        try:
            synchronizedValue = unpickler.load()
        except Exception:
            pass

        # Recount _ignore_changes
        self._ignore_changes = 0
        restored = 0
        while synchronizedValue is not None:
            try:
                assert self._change_handler is not None
                self._change_handler(self._load_value(synchronizedValue))
                restored = restored + 1

                synchronizedValue = None
                synchronizedValue = unpickler.load()
            except SyncError as se:
                self.log.error("Error on restoring snapshot: {}".format(se))
                return GoError(se)
            except Exception:
                pass

        self.log.debug("Restored {}".format(restored))
        return GoNilError()

    # TODO: Debug why, when reading from a read closer and we get to the end, it automatically loops back to the beginning.
    def __value_restored(self, goObject, aggregate_size: int) -> bytes:
        self.log.debug(
            f"Restoring state(s) with combined/aggregate size of {aggregate_size} bytes now..."
        )

        debugpy.breakpoint()

        # Set of IDs of SynchronizedValues that have been restored.
        # We use this to monitor for duplicates.
        restored_sync_values: set[str] = set()

        reader = readCloser(ReadCloser(handle=goObject), size=aggregate_size)
        unpickler = pickle.Unpickler(reader)

        synchronizedValue: Optional[SynchronizedValue] = None
        try:
            synchronizedValue = unpickler.load()
        except Exception as ex:
            self.log.error(
                f"Could not load first synchronized value to restore (aggregate_size = {aggregate_size}) because: {ex}"
            )

        # Recount _ignore_changes
        self._ignore_changes = 0
        restored: int = 0
        # TODO: Debug why, when reading from a read closer and we get to the end, it automatically loops back to the beginning.
        while synchronizedValue is not None:
            assert self._change_handler is not None
            # self.logger.debug("Loading next SynchronizedValue to restore.")
            try:
                loaded_value: Optional[SynchronizedValue] = self._load_value(
                    synchronizedValue
                )
            except Exception as ex:
                self.log.error(
                    f"Unexpected exception encountered while loading SynchronizedValue {synchronizedValue}: {ex}"
                )
                return GoError(ex)

            if loaded_value.id in restored_sync_values:
                self.log.warning(
                    f"Found duplicate SynchronizedValue during restoration process: {loaded_value}"
                )
                self.log.warning("Previously restored SynchronizedValues:")
                for val in list(restored_sync_values):
                    self.log.error(val)

                # For now, just stop here. I'm not sure why this loops.
                self.log.debug(
                    f"Restored state with aggregate size of {aggregate_size} bytes. Number of individual values restored: {restored}"
                )
                return GoNilError()
                # return GoError(ValueError(f"Found duplicate SynchronizedValue during restoration process: {loaded_value}"))
            else:
                self.log.debug(f"Restoring SynchronizedValue: {loaded_value}")

            try:
                self._change_handler(loaded_value)
                restored = restored + 1
            except SyncError as se:
                self.log.error(
                    f"Error while restoring SynchronizedValue {loaded_value}: {se}"
                )
                return GoError(se)
            except Exception as ex:
                self.log.error(
                    f"Unexpected exception encountered while restoring SynchronizedValue {loaded_value}: {ex}"
                )
                # return GoError(ex)

            restored_sync_values.add(loaded_value.id)

            synchronizedValue = None
            loaded_value = None
            # self.logger.debug(f"syncval before calling load: {syncval}")
            synchronizedValue = unpickler.load()
            # self.logger.debug(f"syncval after calling load: {syncval}")

            if synchronizedValue is not None:
                self.log.debug(
                    f"Read next Synchronized Value from recovery data: {synchronizedValue}"
                )
            else:
                self.log.debug(
                    "Got 'None' from recovery data. We're done processing recovered state."
                )

        self.log.debug(
            f"Restored state with aggregate size of {aggregate_size} bytes. Number of individual values restored: {restored}"
        )
        return GoNilError()

    def _load_value(self, val: SynchronizedValue) -> SynchronizedValue:
        """Onload the buffer from the remote_storage server."""
        if type(val.data) is not offloadPath:
            self.log.debug("Returning synchronization value directly.")
            return val

        should_end_execution = val.should_end_execution
        val = self._offloader._load(val.data.path)  # type: ignore
        val.set_should_end_execution(should_end_execution)
        return val

    def _get_serialized_state(
            self,
            last_resource_request: Optional[
                Dict[str, float | int | List[float] | List[int]]
            ] = None,
            remote_storage_definitions: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """
        Serialize important state so that it can be written to RemoteStorage (for recovery purposes).

        This return value of this function should be passed to the `self._log_node.WriteDataDirectoryToRemoteStorage` function.
        """
        data_dict: dict = {
            "kernel_id": self._kernel_id,  # string
            "proposed_values": self._proposed_values,
            # leader proposals, which generally contain a string and a few ints
            "buffered_proposals": self._buffered_proposals,
            # leader proposals, which generally contain a string and a few ints
            "buffered_votes": self._buffered_votes,  # election votes, which generally contain a string and a few ints
            "leader_term": self._leader_term,  # int
            "leader_id": self._leader_id,  # int
            "expected_term": self.expected_term,  # int
            "elections": self._elections,  # map of Election objects
            "current_election": self._current_election,  # Election object
            "last_completed_election": self._last_completed_election,  # Election object
        }

        # Add the resource request entry, if available.
        if last_resource_request is not None:
            self.log.debug(
                f"Adding 'last_resource_request' entry to data dictionary for serialized state: "
                f"{last_resource_request}"
            )
            data_dict["last_resource_request"] = last_resource_request

        # Add the remote storage definitions entry, if available.
        if remote_storage_definitions is not None:
            self.log.debug(
                f"Adding 'remote_storage_definitions' entry to data dictionary for serialized state: "
                f"{remote_storage_definitions}"
            )
            data_dict["remote_storage_definitions"] = remote_storage_definitions

        self.log.debug(
            f"RaftLog {self._node_id} returning state dictionary containing {len(data_dict)} entries:"
        )
        for key, val in data_dict.items():
            self.log.debug(f'"{key}" ({type(val).__name__}): {val}')

        try:
            serialized_data: bytes = pickle.dumps(data_dict)
        except AttributeError as ex:
            self.log.error(
                "Failed to pickle data dictionary due to AttributeError: {ex}"
            )
            raise ex
        except PickleError as ex:
            self.log.error(
                "Failed to pickle data dictionary due to PickleError: {ex}"
            )
            raise ex
        except Exception as ex:
            self.log.error(
                "Failed to pickle data dictionary due to unexpected exception: {ex}"
            )
            raise ex

        return serialized_data

    def retrieve_serialized_state_from_remote_storage(self) -> bytes:
        """
        Retrieve our serialized state from remote storage (via the Golang-level LogNode).

        If there is no serialized state, then the returned bytes object will be empty.
        """
        # self.logger.info(">> CALLING INTO GO CODE (_log_node.GetSerializedState)")
        sys.stderr.flush()
        sys.stdout.flush()
        val: Slice_byte = self._log_node.GetSerializedState()
        # self.logger.info("<< RETURNED FROM GO CODE (_log_node.GetSerializedState)")
        sys.stderr.flush()
        sys.stdout.flush()
        self.log.debug(f"Retrieved serialized state from LogNode: {val}")

        try:
            serialized_state_bytes: bytes = bytes(
                val
            )  # Convert the Go bytes (Slice_byte) to Python bytes.
            return serialized_state_bytes
        except Exception as ex:
            self.log.error(
                f"Failed to convert Golang Slice_bytes to Python bytes because: {ex}"
            )
            sys.stderr.flush()
            sys.stdout.flush()
            raise ex

    @property
    def restore_namespace_time_seconds(self) -> float:
        """
        Return the time spent restoring the user namespace.
        """
        return self._restore_namespace_time_seconds

    def clear_restore_namespace_time_seconds(self):
        """
        Clear the 'restore_namespace_time_seconds' metric.
        """
        self._restore_namespace_time_seconds = 0

    def load_and_apply_serialized_state(self) -> bool:
        """
        Retrieve the serialized state read by the Go-level LogNode.
        This state is read from RemoteStorage during migration/error recovery.
        Update our local state with the state retrieved from RemoteStorage.

        Returns:
            (bool) True if serialized state was loaded, indicating that this replica was started after an eviction/migration.
               If no serialized state was loaded, then this simply returns False.
        """
        self.log.debug(
            "Loading and applying serialized state. First, retrieving serialized state from LogNode."
        )

        if self._log_node is None:
            self.log.error("LogNode is None. Cannot retrieve serialized state.")
            sys.stderr.flush()
            sys.stdout.flush()
            raise ValueError(
                "LogNode is None while trying to retrieve and apply serialized state"
            )

        start_time: float = time.time()
        serialized_state_bytes: bytes = self.retrieve_serialized_state_from_remote_storage()
        self._restoration_time_seconds = time.time() - start_time

        self.log.debug("Successfully converted Golang Slice_bytes to Python bytes.")

        if len(serialized_state_bytes) == 0:
            self.log.debug("No serialized state found. Nothing to load and apply.")
            return False

        try:
            data_dict: dict = pickle.loads(
                serialized_state_bytes
            )  # json.loads(serialized_state_json)
            if len(data_dict) == 0:
                self.log.debug("No serialized state found. Nothing to apply.")
                return False
        except Exception as ex:
            self.log.error(f"Failed to unpickle serialized bytes because: {ex}")
            sys.stderr.flush()
            sys.stdout.flush()
            raise ValueError("Invalid serialized state; could not be unpickled.")

        for key, entry in data_dict.items():
            self.log.debug(f'Retrieved state "{key}": {str(entry)}')

        sys.stderr.flush()
        sys.stdout.flush()

        # TODO:
        # There may be some bugs that arrise from these values being somewhat old or outdated, potentially.
        self._buffered_proposals = data_dict["buffered_proposals"]
        self._buffered_votes = data_dict["buffered_votes"]
        self._proposed_values = data_dict["proposed_values"]
        self._elections = data_dict["elections"]
        self._current_election = data_dict["current_election"]
        self._last_completed_election = data_dict["last_completed_election"]

        # Ensure the "election_finished_condition_waiter" loops are set on any elections that we
        # (a) already know about and (b) aren't finished yet in some capacity.
        for term_number, prior_election in self._elections.items():
            voting_done: bool = prior_election.voting_phase_completed_successfully
            execution_done: bool = prior_election.code_execution_completed_successfully

            # Ensure the "election_finished_condition_waiter" loop is set.
            if not voting_done or not execution_done:
                prior_election.set_election_finished_condition_waiter_loop(self._shell_io_loop)

        # The value of _leader_term before a migration/eviction was triggered.
        self._leader_term_before_migration: int = data_dict["leader_term"]

        # Commenting these out for now; it's not clear if we should set these in this way yet.
        # self._leader_term = data_dict["leader_term"]
        # self._leader_id = data_dict["leader_id"]
        self._expected_term = data_dict["expected_term"]

        try:
            # TODO: Is this correct? I'm pretty sure this will be in the control IO loop.
            #       Don't we want this to be the shell's IO loop?
            self._future_io_loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
            self._future_io_loop.set_debug(True)

            self.log.debug(f"Current/running event loop is equal to self._shell_io_loop: "
                           f"{self._shell_io_loop == self._future_io_loop}")
        except RuntimeError:
            self.log.error("Failed to get running event loop from asyncio module.")

        if self._loaded_serialized_state_callback is not None:
            last_resource_request: Optional[
                Dict[str, float | int | List[float] | List[int]]
            ] = data_dict.get("last_resource_request", None)
            remote_storage_definitions: Optional[Dict[str, Any]] = data_dict.get(
                "remote_storage_definitions", None
            )

            state_dict: dict[str, dict[str, Any]] = {}
            if last_resource_request is not None:
                state_dict["last_resource_request"] = last_resource_request
            if remote_storage_definitions is not None:
                state_dict["remote_storage_definitions"] = remote_storage_definitions

            self.log.debug("Calling 'loaded serialized state' callback now.")

            self._loaded_serialized_state_callback(state_dict)

        return True

    def _get_callback(
            self, future_name: str = ""
    ) -> Tuple[Future, Callable[[str, Exception], Any]]:
        """Get the future object for the specified key."""
        # Prepare callback settings.
        # Callback can be called from a different thread. Schedule the result of the future object to the await thread.
        loop = asyncio.get_running_loop()
        loop.set_debug(True)

        if loop == self._async_loop:
            self.log.debug(
                "Registering callback future on _async_loop. _async_loop.is_running: %s"
                % str(self._async_loop.is_running())
            )  # type: ignore
        elif loop == self._start_loop:
            self.log.debug(
                "Registering callback future on _start_loop. _start_loop.is_running: %s"
                % str(self._start_loop.is_running())
            )  # type: ignore
        else:
            self.log.debug(
                "Registering callback future on unknown loop. loop.is_running: %s"
                % str(loop.is_running())
            )

        self._async_loop = loop

        future: Future = Future(loop=loop, name=future_name)  # type: ignore
        self._async_loop = loop

        def resolve(key, err):
            # must use local variable
            asyncio.run_coroutine_threadsafe(future.resolve(key, err), loop)  # type: ignore

        return future, resolve

    def _is_leading(self, term: int) -> Tuple[bool, bool]:
        """Check if the current node is leading, return (wait, is_leading)"""
        if self._leader_term > term:
            return False, False
        elif self._leader_term == term:
            return False, self._leader_id == self._node_id
        else:
            return True, False

    def get_known_election_terms(self) -> list[int]:
        """
        :return: a list of term numbers for which we have an associated Election object
        """
        return list(self._elections.keys())

    def get_election(self, term_number: int, jupyter_msg_id: Optional[str] = None) -> Any:
        """
        Returns the election with the specified term number, if one exists.

        If the term number is given as -1, then resolution via the JupyterMessageID is attempted.
        """
        if term_number >= 0:
            return self._elections.get(term_number, None)

        assert jupyter_msg_id is not None
        return self._elections_by_jupyter_message_id.get(jupyter_msg_id, None)

    async def _create_election_proposal_or_vote(
            self, key: ElectionProposalKey, term_number: int, jupyter_message_id: str, target_replica_id: int = -1,
    ) -> LeaderElectionProposal | LeaderElectionVote:
        """
        Create and register a proposal for the current term.

        This updates the `self._proposed_values` field.

        The attempt number for the new proposal is "calculated" based on whether there
        already exists a previous proposal for this election term.
        """
        attempt_num: int = 1

        # Get the existing proposals for the specified term.
        existing_proposals: OrderedDict[int, LeaderElectionProposal] = (
            self._proposed_values.get(term_number, OrderedDict())
        )

        # If there is at least one existing proposal for the specified term,
        # then we'll get the most-recent proposal's attempt number.
        if len(existing_proposals) > 0:
            # This is O(1), as OrderedDict uses a doubly-linked list internally.
            last_attempt_num: int = next(reversed(existing_proposals))
            attempt_num = last_attempt_num + 1

            self.log.debug(f"Found previous proposal for term {term_number}. "
                           f"Setting attempt number to last attempt number ({last_attempt_num}) + 1 = {attempt_num}")
        else:
            self.log.debug(f"Found no previous proposal for term {term_number}.")

        # If a specific replica ID was specified, then we "short-circuit" the election
        # and immediately propose a vote rather than a 'LEAD' or 'YIELD' proposal.
        if target_replica_id >= 1:
            vote: LeaderElectionVote = LeaderElectionVote(
                        proposed_node_id=target_replica_id,
                        jupyter_message_id=jupyter_message_id,
                        proposer_id=self._node_id,
                        election_term=term_number,
                        attempt_number=attempt_num,
                    )
            return vote 

        # Create the new proposal.
        proposal: LeaderElectionProposal = LeaderElectionProposal(
            key=str(key),
            proposer_id=self._node_id,
            election_term=term_number,
            attempt_number=attempt_num,
            jupyter_message_id=jupyter_message_id,
        )

        # Add the new proposal to the mapping of proposals for the specified term.
        existing_proposals[attempt_num] = proposal

        # Update the mapping (of proposals for the specified term) in the `self._proposed_values` field.
        self._proposed_values[term_number] = existing_proposals

        # Return the new proposal.
        return proposal

    async def _offload_value(self, val: SynchronizedValue) -> SynchronizedValue:
        """Offload the buffer to the remote_storage server."""
        # Ensure path exists.
        should_end_execution = val.should_end_execution
        val.set_should_end_execution(False)
        val.set_data(offloadPath(await self._offloader.append(val)))
        val.set_prmap(None)
        val.set_should_end_execution(should_end_execution)
        return val

    async def _append_election_vote(self, vote: LeaderElectionVote):
        """
        Explicitly propose and append (to the synchronized Raft log) a vote for the winner of the current election.

        This function exists so that we can mock proposals of LeaderElectionProposal objects specifically,
        rather than mocking the more generic _serialize_and_append_value method.
        """
        self.log.debug(f"Serializing and appending election vote: {vote}")
        await self._serialize_and_append_value(vote)

    async def _append_election_proposal(self, proposal: LeaderElectionProposal):
        """
        Explicitly propose and append (to the synchronized Raft log) a proposal for the current election.

        This function exists so that we can mock proposals of LeaderElectionProposal objects specifically,
        rather than mocking the more generic _serialize_and_append_value method.
        """
        self.log.debug(f"Serializing and appending election proposal: {proposal}")
        await self._serialize_and_append_value(proposal)

    async def _append_catchup_value(self, value: SynchronizedValue):
        """
        Explicitly propose and append (to the synchronized Raft log) a SynchronizedValue object, which will
        serve as an indicator that we've "caught up" to our peers when we're replaying the raft log during
        following migration.

        That is, we call our "value committed" callback for all the previously-committed values in the raft
        cluster. Before we do that though, we commit this "catchup value", so that we know we're done replaying
        once we see the catchup value passed as an argument to our "value committed" callback.

        This function exists so that we can mock proposals of ExecutionCompleteNotification objects specifically,
        rather than mocking the more generic _serialize_and_append_value method.
        """
        self.log.debug(f'Serializing and appending "catch-up" value: {value}')
        await self._serialize_and_append_value(value)

    async def append_execution_end_notification(
            self, notification: ExecutionCompleteNotification
    ):
        """
        Explicitly propose and append (to the synchronized Raft log) a ExecutionCompleteNotification object to
        signify that we've finished executing code in the current election.

        This function exists so that we can mock proposals of ExecutionCompleteNotification objects specifically,
        rather than mocking the more generic _serialize_and_append_value method.

        :param notification: the notification to be appended to the sync log
        """
        self.log.debug(
            f'Serializing and appending "execution complete" notification: {notification}'
        )
        await self._serialize_and_append_value(notification)

    async def _serialize_and_append_value(self, value: SynchronizedValue):
        """
        Serialize the SynchronizedValue (using the pickle module) and explicitly propose and append it to the synchronized etcd-raft log.
        """
        # Serialize the value.
        dumped = pickle.dumps(value)

        # Propose and wait the future.
        future, resolve = self._get_callback(future_name=f'append_val["{value.key}"]')
        assert future is not None
        assert resolve is not None
        self.log.debug(f"Calling 'propose' now for SynchronizedValue: {value}")
        self.propose(dumped, resolve, value.key)
        # await future.result()
        self.log.debug(f"Called 'propose' for SynchronizedValue: {value}")
        await future.result()
        self.log.debug(f"Successfully proposed and appended SynchronizedValue: {value}")

    def propose(self, value: bytes, resolve: Callable[[str, Exception], Any], key: str):
        sys.stderr.flush()
        sys.stdout.flush()
        # self.logger.info(">> CALLING INTO GO CODE (_log_node.Propose)")
        self._log_node.Propose(NewBytes(value), resolve, key)
        # self.logger.info("<< RETURNED FROM GO CODE (_log_node.Propose)")
        sys.stderr.flush()
        sys.stdout.flush()

    async def _check_prev_election_state(self):
        """
        Check if the current/previous election is done.
        """
        if self._current_election is None:
            return

        if self._current_election.code_execution_completed_successfully:
            return

        if self._current_election.was_skipped:
            return

        current_term: int = self._current_election.term_number
        if self._current_election.voting_phase_completed_successfully:
            self.log.warning(f"Current/previous election for term {current_term} completed voting phase, "
                             f"but we've not yet received the 'execution complete' notification yet...")

            await self._current_election.wait_for_election_to_end()
            return

        self.log.warning(f"Current/previous election for term {current_term} has not even finished voting yet...")
        self.log.warning(f"We must be pretty far behind...")

        await self._current_election.wait_for_election_to_end()
        return

    async def _validate_prev_election(self, term_number: int)->int:
        """
        Called while creating a new election.

        Waits a bit for previous election to resolve before giving up on that and just plowing on ahead.
        """

        # Cache this locally.
        current_term: int = 1
        if self._current_election is not None:
            current_term = self._current_election.term_number

        await self._check_prev_election_state()

        # If we don't have a current election, then we'll use the specified term number, which should be 1.
        if self._current_election is None:
            assert term_number == 1
            return term_number

        # If we originally specified something higher, then we'll assume that we know what we're doing.
        if term_number > current_term:
            return term_number

        self.log.warning(f"Specified term number is {term_number}; "
                         f"however, previous election has term {current_term}.")
        self.log.warning(f"Using term number {current_term + 1} instead...")
        return current_term + 1

    async def _create_new_election(self, term_number: int = -1, jupyter_message_id: str = ""):
        """
        Creates the next election with the target term number and Jupyter message ID.

        This should only be called when we do not yet have a local election or when the last local election
        completed successfully.
        """

        # Check if the previous election finished. If it hasn't finished yet, then we'll wait a bit for it to finish
        # before just plowing on ahead. We'll eventually be blocked by the Raft-based election protocol anyway.
        #
        # The real issue is determining what term number we should be using. If the last election isn't over yet, then
        # whatever term number we specified is liable to be incorrect, because we base the term number on the
        # Synchronizer's execution count, and that won't be incremented until we're done synchronizing with the primary
        # replica at the conclusion of the current/last election.
        try:
            term_number = await self._validate_prev_election(term_number)
        except ValueError:
            self.log.warning("Previous election has not yet completed. Making educated guess about term number...")
            term_number = self._current_election.term_number + 1


        # Create a new election. We don't have an existing election to restart/use.
        election: Election = Election(
            term_number,
            self._num_replicas,
            jupyter_message_id,
            timeout_seconds=self._election_timeout_sec,
        )
        self._elections[term_number] = election

        if jupyter_message_id in self._elections_by_jupyter_message_id:
            self.log.warning(f"We already have an election associated with Jupyter msg '{jupyter_message_id}': "
                             f"{self._elections_by_jupyter_message_id[jupyter_message_id]}")

            existing_election: Election = self._elections_by_jupyter_message_id[jupyter_message_id]

            if existing_election.code_execution_completed_successfully:
                self.log.warning(f"Existing election associated with Jupyter msg '{jupyter_message_id}' already "
                                 f"completed. We must have received the Jupyter msg after a long delay. Discarding.")
                raise ValueError("Election associated with Jupyter execute_request "
                                 f"{jupyter_message_id} has already completed")

        self._elections_by_jupyter_message_id[jupyter_message_id] = election

        # Elections contain a sort of (singly-)linked list between themselves.
        # We're performing an append-to-end-of-linked-list operation here.
        self._last_completed_election = self._current_election
        self._current_election = election

        # If we're bumping the election term to a new number, ensure that the last election
        # we know about did in fact complete successfully.
        if self._last_completed_election is not None:
            assert (
                    self._last_completed_election.code_execution_completed_successfully
                    or self._last_completed_election.was_skipped
            )

        self.log.info(f"Created new election with term number {term_number}")

        # Flip this flag to True once we've created the first Election.
        if not self.__created_first_election:
            self.__created_first_election = True

    def _validate_or_restart_current_election(
            self,
            term_number: int = -1,
            jupyter_message_id: str = "",
            expected_attempt_number: int = -1,
    ):
        """
        Validate the state of the current active election. This should be called by the handle_election method.
        We make sure that the term number and Jupyter message IDs are consistent with the proposal we just received.

        If the local election is in the 'failed' state, then we restart it.

        Args:
            term_number: the expected term number of the current election
            jupyter_message_id: the expected jupyter message ID of the current election
            expected_attempt_number: the expected attempt number of the current election
        """
        assert self._current_election is not None and expected_attempt_number >= 0
        assert self._current_election.is_active or self._current_election.is_in_failed_state

        # If we already have an election with a different term number, then that's problematic.
        if self._current_election.term_number != term_number:
            self.log.error(f"Creating new election with term number {term_number} despite "
                           f"already having an active election with "
                           f"term number {self._current_election.term_number}")

            raise ValueError("attempted to create new election while already having an active election")

        # If the Jupyter message IDs do not match, then that is problematic.
        if self._current_election.jupyter_message_id != jupyter_message_id:
            raise ValueError(f"Attempting to get or retrieve election for term {term_number} with "
                             f"JupyterMessageID={jupyter_message_id}, which does not match the JupyterMessageID "
                             f"of our current election for term {term_number}, "
                             f"{self._current_election.jupyter_message_id}.")

        # If we have an election with the same term number, then there may have just been some delay in us receiving
        # the 'execute_request' (or 'yield_request') ZMQ message.
        #
        # During this delay, we may have received a committed proposal from another replica for this election,
        # which prompted us to either create or restart the election at that point.
        #
        # So, if we have a current election already, and that election is in a non-active state, then we restart it.
        # If we have a current election that is already active, then we should have at least one proposal already
        # (otherwise, why would the election be active already?)
        if self._current_election.is_active:
            self.log.debug(f"Reusing existing, already-active election {self._current_election.term_number}. "
                           f"Number of proposals received (not counting ours): "
                           f"{self._current_election.num_proposals_received}.")

            # Sanity check.
            # If the current election is already active, then we necessarily should have received a proposal from a peer,
            # which triggered either the creation of this election, or the restarting of the election if it had already
            # existed and was in the failed state.
            if self._current_election.num_proposals_received == 0:
                raise ValueError(f"Existing election for term {term_number} is already active; "
                                 f"however, it has no registered proposals, so it should not be active already")
        else:
            assert self._current_election.is_in_failed_state
            self.log.debug(f"Restarting existing election {self._current_election.term_number}. "
                           f"Current state: {self._current_election.election_state.get_name()}.")
            self._current_election.restart(latest_attempt_number=expected_attempt_number)

    def _handle_unexpected_election(
            self,
            term_number: int = -1,
    ):
        """
        This function will always raise an exception. This is called by _handle_election when the local election
        is not in one of the expected states.
        """
        if self._current_election.term_number > term_number:
            # If we're creating a new election, its term number should be greater than that of the current election.
            self.log.error(
                f"Attempted to create new election with term number {term_number} despite already previous election "
                f"having a larger term number of {self._current_election.term_number}"
            )
            raise ValueError(
                f"Attempted to create new election with term number smaller than previous election's term number "
                f"({term_number} < {self._current_election.term_number})"
            )
        else:
            self.log.error(
                f"Current election with term number {self._current_election.term_number} is in unexpected state "
                f"{self._current_election.election_state.get_name()}."
            )
            raise ValueError(
                f"Current election (term number: {self._current_election.term_number}) is in unexpected state: "
                f"{self._current_election.election_state.get_name()}"
            )

    async def _prepare_election(
            self,
            target_term_number: int = -1,
            jupyter_message_id: str = "",
            expected_attempt_number: int = -1,
    ) -> bool:
        """
        Prepare an Election to be processed.

        This involves either creating a new Election or restarting the current Election.

        This also performs a series of checks to see if we're in an error state, based on the values passed to it
        and the current local state.

        This does not return an Election object. This simply updates the _current_election instance variable.

        This does return a boolean flag which, if True, indicates that the election should be processed and, if False,
        indicates that the "execute_request" or "yield_request" that we received is for an old, skipped election
        and should simply be discarded.
        """
        with self._election_lock:
            assert target_term_number > 0
            assert expected_attempt_number > 0
            assert jupyter_message_id is not None and jupyter_message_id != ""

            # If the current election field is None, then we've never had an election before, and
            # so we create the election and return.
            if self._current_election is None:
                self.log.debug(f"Current election is None. Creating new election for term {target_term_number} "
                               f"with Jupyter message ID = {jupyter_message_id}.")
                await self._create_new_election(term_number=target_term_number, jupyter_message_id=jupyter_message_id)
                return True

            if target_term_number == self.current_election_term and (
                    self._current_election.is_active
                    or self._current_election.is_in_failed_state
            ):
                self.log.debug(f"Validating or restarting existing/current election for term {target_term_number}.")
                self._validate_or_restart_current_election(
                    term_number=target_term_number,
                    jupyter_message_id=jupyter_message_id,
                    expected_attempt_number=expected_attempt_number,
                )
                return True

            target_election: Optional[Election] = self._elections.get(target_term_number)
            if target_election is None:
                self.log.debug(f"Could not find existing election with term number {target_term_number}. "
                               f"Trying to look up by jupyter message ID of {jupyter_message_id}.")
                target_election = self._elections_by_jupyter_message_id.get(jupyter_message_id)

                if target_election is None:
                    self.log.debug(f"Failed to find existing election associated "
                                   f"with Jupyter message ID {jupyter_message_id}.")
                else:
                    self.log.debug(f"Found existing election associated with Jupyter message ID {jupyter_message_id}. "
                                   f"Election has term {target_election.term_number} and is in state "
                                   f"{target_election.election_state.get_name()}.")

            if target_election is not None:
                if target_election.was_skipped:
                    self.log.warning(f"Requested preparation of election {target_term_number}; "
                                     f"however, that election was skipped.")
                    return False
                else:
                    raise ValueError(f"Attempting to prepare election {target_term_number}, "
                                     f"which is in state {target_election.election_state.get_name()}. "
                                     f"Current local election {self.current_election_term} "
                                     f"is in state {self._current_election.election_state.get_name()}.")

            if (
                    target_term_number == self.current_election_term
                    and not self._current_election.voting_phase_completed_successfully
                    and not self._current_election.code_execution_completed_successfully
            ):
                self._handle_unexpected_election(term_number=target_term_number)
                return False  # The above method raises an exception, so we won't actually return.

            await self._create_new_election(term_number=target_term_number, jupyter_message_id=jupyter_message_id)

            return True

    async def _propose_election_proposal(
        self,
        proposal: LeaderElectionProposal,
        election_term: int,
        num_buffered_proposals_processed: int = 0,
        num_buffered_votes_processed: int = 0,
        _election_decision_future: Optional[asyncio.Future] = None,
        _received_vote_future: Optional[asyncio.Future] = None,
    )-> tuple[bool, bool, Optional[LeaderElectionVote]]:
        self.log.debug(f"Preparing to propose our own value for election {election_term} "
                    f"after processing {num_buffered_proposals_processed} buffered proposal(s) "
                    f"and {num_buffered_votes_processed} buffered votes: {proposal}")

        if _received_vote_future is not None and _received_vote_future.done():
            self.log.debug(f"Was going to to propose our own value for election {election_term}, but we already "
                           f"received a vote for this election term. Skipping proposal.")
        else:
            await self._append_election_proposal(proposal)

        self.log.debug(f"Waiting on 'election decision' and 'received vote' futures for term {election_term}.")

        futures: List[asyncio.Future] = []

        if _election_decision_future is not None:
            futures.append(_election_decision_future)

        if _received_vote_future is not None:
            futures.append(_received_vote_future)

        if len(futures) == 0:
            self.log.warning(f"Both 'election decision' future and 'received vote' futures are None "
                            f"while processing buffered votes for election {election_term}...")
            return True, False, None

        done, pending = await asyncio.wait([_election_decision_future, _received_vote_future],
                                        return_when=asyncio.FIRST_COMPLETED)

        if _received_vote_future in done or _received_vote_future.done():
            voteReceived: LeaderElectionVote = _received_vote_future.result()
            self.log.debug(f"The voting phase for election {election_term} has already completed, "
                        f"before we had a chance to propose our own vote. "
                        f"Received vote: {voteReceived}")

            if self._current_election.term_number != election_term:
                self.log.error(f"Current election has term {self._current_election.term_number} "
                            f"while handling election {election_term}...")

                msg: str = (f"Current election has term {self._current_election.term_number} "
                              f"while handling election {election_term}...")

                self._send_notification_func(msg, msg, 1)
                wait, is_leading = self._is_leading(election_term)
                assert wait == False

                return True, is_leading, None

            assert self._current_election.voting_phase_completed_successfully

            self._received_vote_future = None
            self._election_decision_future = None

            return True, voteReceived.proposed_node_id == self.node_id, None

        assert _election_decision_future.done()
        voteProposal: LeaderElectionVote = _election_decision_future.result()

        if voteProposal is not None:
            assert isinstance(voteProposal, LeaderElectionVote)
        
        return False, False, voteProposal

    async def _process_proposals(
            self,
            buffered_proposals: list[BufferedLeaderElectionProposal],
            election_term: int,
            num_buffered_votes_processed: int,
            proposalOrVote: LeaderElectionProposal | LeaderElectionVote,
            _election_decision_future: asyncio.Future[Any],
            _leading_future: asyncio.Future[int],
            _received_vote_future: asyncio.Future[Any],
    )->tuple[bool,bool]:
        """
        :param buffered_proposals:
        :param election_term:
        :param num_buffered_votes_processed:
        :param proposalOrVote:
        :param _election_decision_future:
        :param _leading_future:
        :param _received_vote_future:
        :return: a tuple where 1st element indicates if we're done processing the election, and 2nd is result if so.
        """
        num_buffered_proposals_processed: int = 0

        if len(buffered_proposals) > 0:
            self.log.debug(f"Processing the {len(buffered_proposals)} "
                           f"buffered proposal(s) for election {election_term} now.")

            for i, buffered_proposal in enumerate(buffered_proposals):
                self.log.debug(f"Handling buffered proposal {i + 1}/{len(buffered_proposals)} "
                               f"during election term {election_term}: {buffered_proposal}")

                # TODO: Is it OK to just pass the current time for `received_at`?
                #       Or should I save the time at which it was received and buffered, and pass that instead?
                self.__handle_proposal(
                    buffered_proposal.proposal,
                    received_at=buffered_proposal.received_at,
                )
                self.log.debug(f"Handled buffered proposal {i + 1}/{len(buffered_proposals)} "
                               f"during election term {election_term}.")
                num_buffered_proposals_processed += 1

        if isinstance(proposalOrVote, LeaderElectionProposal):
            isDone, isLeading, voteProposal = await self._propose_election_proposal(
                proposalOrVote, election_term,
                num_buffered_proposals_processed = num_buffered_proposals_processed,
                num_buffered_votes_processed = num_buffered_votes_processed,
                _election_decision_future = _election_decision_future,
                _received_vote_future = _received_vote_future)

            if voteProposal is None or isDone:
                return isDone, isLeading

            assert isinstance(proposalOrVote, LeaderElectionVote)
        else:
            assert isinstance(proposalOrVote, LeaderElectionVote)
            voteProposal: LeaderElectionVote = proposalOrVote

        self.log.debug(f"Finished waiting on 'election decision' future for term {election_term}: {voteProposal}")
        self._received_vote_future = None
        self._election_decision_future = None

        # Validate that the term number matches the current election.
        if voteProposal.election_term != election_term:
            raise ValueError(f"Received LeaderElectionVote with mis-matched term number ({voteProposal.election_term}) "
                             f"compared to current election term number ({election_term})")

        # Are we proposing that the election failed?
        if voteProposal.election_failed:
            self.log.debug(f"RaftLog {self._node_id}: Got decision to propose: election failed. "
                           f"No replicas proposed 'LEAD'.")

            with self._election_lock:
                self._current_election.set_election_failed()

            # None of the replicas proposed 'LEAD'
            # It is likely that a migration of some sort will be triggered as a result, leading to another election round for this term.
            return True, False

        self.log.debug(f"RaftLog {self._node_id}: Appending vote proposal "
                       f"for term {voteProposal.election_term} now.")

        await self._append_election_vote(voteProposal)

        self.log.debug(f"RaftLog {self._node_id}: Successfully appended vote "
                       f"proposal for term {voteProposal.election_term} now.")

        return False, False

    async def _process_buffered_votes(self, votes: list[BufferedLeaderElectionVote], term: int) -> tuple[bool, int]:
        self.log.debug(f"Processing the {len(votes)} buffered vote(s) for election {term} now.")

        if len(votes) == 0:
            return False, 0

        skip_proposals: bool = False
        num_buffered_votes_processed: int = 0

        for i, buffered_vote in enumerate(votes):
            self.log.debug(f"Handling buffered vote {i + 1}/{len(votes)} "
                           f"during election term {term}: {buffered_vote}")

            # TODO: Is it OK to just pass the current time for `received_at`? Or should I save the time at which it was received and buffered, and pass that instead?
            self.__handle_vote(buffered_vote.vote, received_at=buffered_vote.received_at)
            self.log.debug(f"Handled buffered vote {i + 1}/{len(votes)} "
                           f"during election term {term}.")

            num_buffered_votes_processed += 1

            if self._current_election.voting_phase_completed_successfully:
                self.log.debug(f"Voting phase for current election ({term}) voting phase has ended after "
                               f"processing buffered vote #{i}.")
                skip_proposals = True
                break
            else:
                self.log.debug(f"Voting phase for current election {term} has not ended after processing "
                               f"buffered vote #{i}.")

        self.log.debug(f"Finished processing buffered votes for election {term}. "
                       f"Processed {num_buffered_votes_processed}/{len(votes)} buffered vote(s).")

        return skip_proposals, num_buffered_votes_processed

    async def _handle_election(
            self,
            proposalOrVote: LeaderElectionProposal | LeaderElectionVote,
            target_term_number: int = -1,
            jupyter_message_id: str = "",
    ) -> bool:
        """
        Orchestrate an election. Return a boolean indicating whether we are now the "leader".

        The election should have been set up/created prior to calling this function.

        The `target_term_number` argument is just a safety mechanism to ensure that the current election
        matches the intended/target term number.
        """
        
        if isinstance(proposalOrVote, LeaderElectionVote):
            self.log.debug(f"RaftLog {self._node_id} short-circuiting election in term {target_term_number}, "
                        f"attempt #{proposalOrVote.attempt_number}. Will be voting for node {target_term_number}.")
        elif isinstance(proposalOrVote, LeaderElectionProposal):
            self.log.debug(f"RaftLog {self._node_id} handling election in term {target_term_number}, "
                        f"attempt #{proposalOrVote.attempt_number}. Will be proposing {proposalOrVote.key}.")
        else:
            raise ValueError(f"Illegal type of proposal/vote passed to 'handle election': {type(proposalOrVote).__name__}")

        should_handle_election: bool = await self._prepare_election(
            target_term_number=target_term_number,
            jupyter_message_id=jupyter_message_id,
            expected_attempt_number=proposalOrVote.attempt_number,
        )
        assert (self._current_election is not None)  # The current election field must be non-null.

        if not should_handle_election:
            # Erase the proposed value we created for this term.
            self._proposed_values.pop(target_term_number)
            raise DiscardMessageError(f"Message received by replica {self._node_id} of kernel {self._kernel_id}"
                                      f"for election {target_term_number} should be discarded, "
                                      f"as that election was skipped.")

        if self._current_election.election_finished_condition_waiter_loop is None:
            self._current_election.election_finished_condition_waiter_loop = asyncio.get_running_loop()

        try:
            if self._current_election.is_inactive:
                # Start the election.
                self._current_election.start()
        except Exception as ex:
            self.log.error(f"Exception while starting or restarting election {target_term_number}: {ex}")
            raise ex  # Just re-raise the exception.

        if self._last_completed_election is not None and self._leader_term >= target_term_number:
            self.log.warning(f"Current leader term {self._leader_term} >= "
                             f"specified target term {target_term_number}...")
            return False

        # The proposalOrVote's term number must match the specified target term number.
        if proposalOrVote.election_term != target_term_number:
            raise ValueError(f"{type(proposalOrVote).__name__} is targeting election term {proposalOrVote.election_term}, "
                             f"whereas caller specified election term {target_term_number}")

        # Do some additional sanity checks:
        # The proposalOrVote must already be registered.
        # This means that there will be at least one proposalOrVote for the specified
        # target term number (which matches the proposalOrVote's term number; we
        # already checked verified that above).
        if isinstance(proposalOrVote, LeaderElectionProposal):
            # At least one proposalOrVote for the specified term?
            assert target_term_number in self._proposed_values

            # The proposalOrVote is registered under its attempt number?
            assert proposalOrVote.attempt_number in self._proposed_values[target_term_number]
        
            # Equality check for ultimate sanity check.
            assert self._proposed_values[target_term_number][proposalOrVote.attempt_number] == proposalOrVote

        # Define the `_leading` feature.
        # Save a reference to the currently-running IO loop so that we can resolve
        # the `_leading` future on this same IO loop later.
        self._future_io_loop = asyncio.get_running_loop()
        self._future_io_loop.set_debug(True)
        # This is the future we'll use to submit a formal vote for who should lead,
        # based on the proposals that are committed to the etcd-raft log.
        self._election_decision_future = self._future_io_loop.create_future()
        self._received_vote_future = self._future_io_loop.create_future()
        
        self._received_vote_future_term: int = target_term_number
        # This is the future that we'll use to inform the local kernel replica if
        # it has been selected to "lead" the election (and therefore execute the user-submitted code).
        self._leading_future: Optional[asyncio.Future[int]] = self._future_io_loop.create_future()

        # Create local references.
        _election_decision_future: asyncio.Future[Any] = self._election_decision_future
        _leading_future: asyncio.Future[int] = self._leading_future
        _received_vote_future: asyncio.Future[Any] = self._received_vote_future

        # Process any buffered votes and proposals that we may have received.
        # If we have any buffered votes, then we'll process those first, as that'll presumably be all we need to do.
        buffered_votes: List[BufferedLeaderElectionVote] = self._buffered_votes.get(proposalOrVote.election_term, [])
        buffered_proposals: List[BufferedLeaderElectionProposal] = self._buffered_proposals.get(proposalOrVote.election_term, [])

        # If skip_proposals is True, then we'll skip both any buffered proposals, and we'll just elect not to
        # propose something ourselves. skip_proposals is set to True if we have a buffered vote that decides
        # the election for us.
        skip_proposals: bool = False

        num_buffered_votes_processed: int = 0

        election_term: int = self._current_election.term_number

        self.log.debug(f"There are {len(buffered_proposals)} buffered proposalOrVote(s) and {len(buffered_votes)} "
                       f"buffered vote(s) for election {election_term}.")

        if len(buffered_votes) > 0:
            skip_proposals, num_buffered_votes_processed = await self._process_buffered_votes(buffered_votes, election_term)

        if skip_proposals:
            self.log.debug(f"Skipping the {len(buffered_proposals)} buffered proposal(s) as well as our own proposal "
                           f"for election {election_term}.")
        else:
            done, is_leading = await self._process_proposals(
                buffered_proposals,
                election_term,
                num_buffered_votes_processed,
                proposalOrVote,
                _election_decision_future,
                _leading_future,
                _received_vote_future,
            )

            if done:
                self.log.debug(f"Finished handling election {election_term} while processing "
                               f"{len(buffered_proposals)} buffered proposal(s). is_leading={is_leading}")
                return is_leading

            self.log.debug(f"Not yet finished handling election {election_term} after processing "
                           f"{len(buffered_proposals)} buffered proposal(s). is_leading={is_leading}")

        # Validate the term
        wait, is_leading = self._is_leading(target_term_number)
        if not wait:
            self.log.debug(
                "RaftLog %d: returning for term %d without waiting, is_leading=%s"
                % (self._node_id, target_term_number, str(is_leading))
            )
            return is_leading

        # Wait for the future to be set.
        self.log.debug("Waiting on _leading_future Future to be resolved.")
        await _leading_future
        self.log.debug("Successfully waited for resolution of _leading_future.")
        self._leading_future = None

        # Validate the term
        wait, is_leading = self._is_leading(target_term_number)
        assert wait == False
        return is_leading

    @property
    def restoration_time_seconds(self) -> float:
        """
        Return the time spent on restoring previous state.
        """
        return self._restoration_time_seconds

    def clear_restoration_time(self):
        """
        Clear the 'restoration_time_seconds' metric.
        """
        self._restoration_time_seconds = 0.0

    def has_active_election(self) -> bool:
        """
        Return true if the following two conditions are met:
            - (a): We have an election (i.e., the _current_election field is non-nil)
            - (b): The current election is in the ACTIVE state
        """
        if self.current_election is None:
            return False

        return self.current_election.is_active

    async def catchup_with_peers(self) -> None:
        """
        Propose a new value and wait for it to be commited to know that we're "caught up".
        """
        # Ensure that we actually do need to catch up.
        if not self.needs_to_catch_up:
            self.log.error("needs_to_catch_up is False in catchup_with_peers")
            sys.stderr.flush()
            sys.stdout.flush()
            raise ValueError("no need to catch-up with peers")

        # Ensure that the "catchup" value has already been created.
        if self._catchup_value is None:
            self.log.error("_catchup_value is None in catchup_with_peers")
            sys.stderr.flush()
            sys.stdout.flush()
            raise ValueError('"catchup" value is None')

        # Ensure that the "catchup" IO loop is set so that Golang code (that has called into Python code)
        # can populate the "catchup" Future with a result (using the "catchup" IO loop).
        if self._catchup_io_loop is None:
            self.log.error("_catchup_io_loop is None in catchup_with_peers")
            sys.stderr.flush()
            sys.stdout.flush()
            raise ValueError('"catchup" IO loop is None')

        # Ensure that the "catchup" future has been created already.
        if self._catchup_future is None:
            self.log.error("_catchup_future is None in catchup_with_peers")
            sys.stderr.flush()
            sys.stdout.flush()
            raise ValueError('"catchup" future is None')

        # Ensure the "election_finished_condition_waiter" loops are set on any elections that we
        # (a) already know about and (b) aren't finished yet in some capacity.
        for term_number, prior_election in self._elections.items():
            voting_done: bool = prior_election.voting_phase_completed_successfully
            execution_done: bool = prior_election.code_execution_completed_successfully

            # Ensure the "election_finished_condition_waiter" loop is set.
            if not voting_done or not execution_done:
                prior_election.set_election_finished_condition_waiter_loop(asyncio.get_running_loop())

        self.log.debug('Proposing & appending our "catch up" value now.')

        await self._append_catchup_value(self._catchup_value)

        self.log.debug(
            'We\'ve successfully proposed & appended our "catch up" value.'
        )

        await self.wait_until_we_have_caught_up()

    async def wait_until_we_have_caught_up(self):
        """
        Called by catchup_with_peers. Exists as a separate function so we can mock it while unit testing.

        Basically just awaits the self._catchup_future variable and then sets a bunch of state to None afterwards.
        """
        await self._catchup_future  # Wait for the value to be committed.

        if self._send_notification_func is not None:
            self._send_notification_func(
                "Caught Up After Migration",
                f"Replica {self._node_id} of Kernel {self._kernel_id} has caught-up to its peers following a migration operation.",
                2,
            )

        self.log.debug("We've successfully caught up to our peer replicas.")

        # Reset these fields after we're done.
        self._catchup_future = None
        self._catchup_io_loop = None
        self._catchup_value = None  # This should already be None at this point; we set it to None in the 'value committed' handler.

    async def append(self, value: SynchronizedValue):
        """
        Append some data to the synchronized Raft log.
        """
        if value.key != str(ElectionProposalKey.LEAD) and value.key != str(
                ElectionProposalKey.YIELD
        ):
            self.log.debug(f'Updating self._leader_term from {self._leader_term} to {value.election_term}, the '
                           f'election term of the SynchronizedValue (with key="{value.key}") that we\'re appending.')
            self._leader_term = value.election_term

        if not value.has_operation:
            # Count _ignore_changes
            self._ignore_changes += 1

        # Ensure key is specified.
        if value.key is not None:
            if (
                    value.data is not None
                    and type(value.data) is bytes
                    and len(value.data) > MAX_MEMORY_OBJECT
            ):
                self.log.debug(
                    f'Offloading value with key "{value.key}" before proposing/appending it.'
                )
                value = await self._offload_value(value)
                self.log.debug(f'Successfully offloaded value with key "{value.key}" before proposing/appending it.')

        await self._serialize_and_append_value(value)

    async def add_node(self, node_id, address):
        """
        Add a node to the etcd-raft cluster.

        NOTE: As of right now (5:39pm EST, Oct 11, 2024), this method is not actually used/called.

        Args:
            node_id: the ID of the node being added.
            address: the IP address of the node being added.
        """
        self.log.info(
            "Adding node %d at addr %s to the SMR cluster." % (node_id, address)
        )
        future, resolve = self._get_callback(future_name=f"add_node[{node_id}]")
        # self.logger.info(">> CALLING INTO GO CODE (_log_node.AddHost)")
        sys.stderr.flush()
        sys.stdout.flush()
        self._log_node.AddNode(node_id, address, resolve)
        # self.logger.info("<< RETURNED FROM GO CODE (_log_node.AddHost)")
        sys.stderr.flush()
        sys.stdout.flush()
        res = await future.result()
        # await future
        # res = future.result()
        self.log.info("Result of AddHost: %s" % str(res))

    async def update_node(self, node_id, address):
        """Add a node to the etcd-raft  cluster."""
        self.log.info("Updating node %d with new addr %s." % (node_id, address))
        future, resolve = self._get_callback(future_name=f"update_node[{node_id}]")
        # self.logger.info(">> CALLING INTO GO CODE (_log_node.UpdateNode)")
        sys.stderr.flush()
        sys.stdout.flush()
        self._log_node.UpdateNode(node_id, address, resolve)
        # self.logger.info("<< RETURNED FROM GO CODE (_log_node.UpdateNode)")
        sys.stderr.flush()
        sys.stdout.flush()
        res = await future.result()
        # await future
        # res = future.result()
        self.log.info("Result of UpdateNode: %s" % str(res))

    async def remove_node(self, node_id):
        """Remove a node from the etcd-raft cluster."""
        self.log.info("Removing node %d from the SMR cluster." % node_id)
        future, resolve = self._get_callback(future_name=f"remove_node[{node_id}]")

        try:
            # self.logger.info(">> CALLING INTO GO CODE (_log_node.RemoveNode)")
            sys.stderr.flush()
            sys.stdout.flush()
            self._log_node.RemoveNode(node_id, resolve)
            # self.logger.info("<< RETURNED FROM GO CODE (_log_node.RemoveNode)")
            sys.stderr.flush()
            sys.stdout.flush()
        except Exception as ex:
            # self.logger.info("<< RETURNED FROM GO CODE (_log_node.RemoveNode)")
            sys.stderr.flush()
            sys.stdout.flush()
            self.log.error(
                "Error in LogNode while removing replica %d: %s" % (node_id, str(ex))
            )

        res = await future.result()
        # await future
        # res = future.result()
        self.log.info("Result of RemoveNode: %s" % str(res))

    @property
    def current_election_term(self) -> int:
        """
        Return the term number of the current local election.

        If the current local election is None, then this will return 0.
        """
        if self._current_election is not None:
            return self._current_election.term_number

        return 0

    @property
    def num_elections_skipped(self) -> int:
        """
        The number of elections we've skipped.

        Returns: The number of elections we've skipped.
        """
        return self._num_elections_skipped

    @property
    def created_first_election(self) -> bool:
        """
        :return: return a boolean indicating whether we've created the first election yet.
        """
        return self.__created_first_election

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
        if (
                self._current_election is not None
                and self._current_election.term_number != self._leader_term
        ):
            self.log.warning(
                f"Returning _leader_term of {self._leader_term}; however, _current_election.term_number = {self._current_election.term_number}"
            )

        return self._leader_term

    @property
    def leader_term(self) -> int:
        """
        Updated after a LEAD call. This is the term of the LEADER. Used to check if received proposals are old/new.
        """
        return self._leader_term

    @property
    def leader_id(self) -> int:
        """
        ID of the current leader. Updated after a LEAD call. Used to check if received proposals are old/new.
        """
        return self._leader_id

    @property
    def num_changes(self) -> int:
        """The number of incremental changes since first term or the latest checkpoint."""
        # self.logger.info(">> CALLING INTO GO CODE (_log_node.NumChanges)")
        sys.stderr.flush()
        sys.stdout.flush()
        num_changes: int = self._log_node.NumChanges()
        # self.logger.info("<< RETURNED FROM GO CODE (_log_node.NumChanges)")
        sys.stderr.flush()
        sys.stdout.flush()

        return num_changes - self._ignore_changes

    @property
    def expected_term(self) -> Optional[int]:
        """
        This is the term number we're expecting for the current election.

        It's equal to the current election's term number.
        """
        if self._current_election is not None:
            return self._current_election.term_number

        return -1

    @property
    def current_election(self) -> Optional[Election]:
        return self._current_election

    @property
    def last_completed_election(self) -> Optional[Election]:
        return self._last_completed_election

    def start(self, handler: Callable[[SynchronizedValue], None]) -> None:
        """
        Register the change handler, restore internal states, and start monitoring for changes committed to the Raft log.
        """
        # faulthandler.dump_traceback_later(timeout = 30, repeat = True, file = sys.stderr, exit = False)
        self.log.debug("Starting RaftLog")
        
        self._change_handler = handler

        config = NewConfig()
        config.ElectionTick = self._heartbeat_tick
        config.HeartbeatTick = self._election_tick

        config = config.WithChangeCallback(self._valueCommittedCallback)
        config = config.WithRestoreCallback(self._valueRestoredCallback)

        if self._shouldSnapshotCallback is not None:
            config = config.WithShouldSnapshotCallback(self._shouldSnapshotCallback)
        if self._snapshotCallback is not None:
            config = config.WithSnapshotCallback(self._snapshotCallback)

        self.log.info(f"Starting LogNode {self._node_id} now.")

        try:
            self._async_loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
            self._async_loop.set_debug(True)
        except RuntimeError:
            self.log.warning("No asyncio Event Loop running...")
            self._async_loop: Optional[asyncio.AbstractEventLoop] = None

        self._start_loop: Optional[asyncio.AbstractEventLoop] = self._async_loop

        # self.logger.info(">> CALLING INTO GO CODE (_log_node.Start)")
        sys.stderr.flush()
        sys.stdout.flush()

        self.log.debug("RaftLog::start: starting LogNode now.")
        startSuccessful: bool = self._log_node.Start(config)

        # self.logger.info("<< RETURNED FROM GO CODE (_log_node.Start)")
        sys.stderr.flush()
        sys.stdout.flush()

        if not startSuccessful:
            self.log.error("Failed to start LogNode.")
            raise RuntimeError("failed to start the Golang-level LogNode component")

        self.log.info("Successfully started RaftLog and LogNode.")

    def close_remote_storage_client(self) -> None:
        """
        Close the LogNode's RemoteStorage client.
        """
        # self.logger.info(">> CALLING INTO GO CODE (_log_node.CloseRemoteStorageClient)")
        sys.stderr.flush()
        sys.stdout.flush()

        self._log_node.CloseRemoteStorageClient()

        # self.logger.info("<< RETURNED FROM GO CODE (_log_node.CloseRemoteStorageClient)")
        sys.stderr.flush()
        sys.stdout.flush()

    # IMPORTANT: This does NOT close the RemoteStorage client within the LogNode.
    # This is because, when migrating a raft cluster member, we must first stop the raft
    # node before copying the contents of its data directory.
    #
    # To close the RemoteStorage client within the LogNode, call the `CloseRemoteStorageClient` method.
    def close(self) -> None:
        """
        Ensure all async coroutines have completed. Clean up resources. Stop the LogNode.
        """
        self.log.warning(f"Closing LogNode {self._node_id} now.")

        # self.logger.info(">> CALLING INTO GO CODE (_log_node.Close)")
        sys.stderr.flush()
        sys.stdout.flush()

        self._log_node.Close()

        self.log.info("<< RETURNED FROM GO CODE (_log_node.Close)")
        sys.stderr.flush()
        sys.stdout.flush()

        if self._closed is not None:
            if self._start_loop is None:
                self.log.error(
                    "Cannot resolve '_closed' future; start loop is None..."
                )
            else:
                asyncio.run_coroutine_threadsafe(
                    self._closed.resolve(None, None), self._start_loop
                )
                self._closed = None

        self.log.debug("RaftLog %d has closed." % self._node_id)

    def set_should_checkpoint_callback(self, callback):
        """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
        callback will be in the form callback(SyncLog) bool"""
        if callback is None:
            self._shouldSnapshotCallback = None
            return

        def should_snapshot_callback(logNode):
            sys.stderr.flush()
            sys.stdout.flush()
            logNode = LogNode(handle=logNode)
            self.log.debug(f"shouldSnapshotCallback called with logNode = {logNode}")
            sys.stderr.flush()
            sys.stdout.flush()
            # Initialize object using LogNode(handle=logNode) if necessary.
            # print("in direct shouldSnapshotCallback")
            return callback(self)

        self._shouldSnapshotCallback = should_snapshot_callback

    def set_checkpoint_callback(self, callback):
        """Set the callback that will be called when the SyncLog decides to checkpoint.
        callback will be in the form callback(Checkpointer)."""
        if callback is None:
            self._snapshotCallback = None
            return

        def snapshot_callback(wc) -> bytes:
            sys.stderr.flush()
            sys.stdout.flush()
            try:
                self.log.debug(f"SnapshotCallback called with wc = {wc}")
                sys.stderr.flush()
                sys.stdout.flush()
                checkpointer = Checkpoint(writeCloser(WriteCloser(handle=wc)))
                self.log.debug("Created Checkpoint object. Calling callback now.")
                sys.stderr.flush()
                sys.stdout.flush()
                callback(checkpointer)
                # Reset _ignore_changes
                self._ignore_changes = 0
                return GoNilError()
            except Exception as e:
                self.log.error("Error on snapshotting: {}".format(e))
                return GoError(e)

        self._snapshotCallback = snapshot_callback

    async def write_data_dir_to_remote_storage(
            self,
            last_resource_request: Optional[
                Dict[str, float | int | List[float] | List[int]]
            ] = None,
            remote_storage_definitions: Optional[Dict[str, Any]] = None,
    ):
        """
        Write the contents of the etcd-Raft data directory to RemoteStorage.
        """
        self.log.info("Writing etcd-Raft data directory to RemoteStorage.")

        serialized_state: bytes = self._get_serialized_state(
            last_resource_request=last_resource_request,
            remote_storage_definitions=remote_storage_definitions,
        )
        self.log.info(
            "Serialized important state to be written along with etcd-Raft data. Size: %d bytes."
            % len(serialized_state)
        )

        future, resolve = self._get_callback(future_name="write_data_remote_storage")

        # self.logger.info(">> CALLING INTO GO CODE (_log_node.WriteDataDirectoryToRemoteStorage)")
        sys.stderr.flush()
        sys.stdout.flush()

        # This will return immediately, as the actual work of the method is performed by a separate goroutine.
        self._log_node.WriteDataDirectoryToRemoteStorage(
            Slice_byte(serialized_state), resolve
        )

        # self.logger.info("<< RETURNED FROM GO CODE (_log_node.WriteDataDirectoryToRemoteStorage)")
        sys.stderr.flush()
        sys.stdout.flush()

        # Wait for the data to be written to RemoteStorage without blocking the IO loop.
        waldir_path: str = await future.result()
        return waldir_path

    async def wait_for_election_to_end(self, term_number: int):
        """
        Wait until the leader of the specified election finishes executing the code,
        or until we know that all replicas yielded.

        :param term_number: the term number of the election
        """
        self.log.debug(
            f"Waiting for leader to finish executing code (or to learn that all replicas yielded) "
            f"for election term {term_number}."
        )

        if self.current_election.term_number != term_number:
            self.log.error(
                f"Current election has term number {self.current_election.term_number}, "
                f"whereas the term number specified is {term_number}. "
                "Cannot wait for specified election to end."
            )
            raise ValueError(
                f"Current election has term number {self.current_election.term_number}, "
                f"whereas the term number specified is {term_number}. "
                "Cannot wait for specified election to end."
            )

        # Wait for the election to be finished, either because the leader finished execution the user-submitted code,
        # or because all replicas proposed "yield".
        self.log.debug("Waiting for current election to end (or fail).")
        await self.current_election.wait_for_election_to_end()
        self.log.debug(
            f"Election {term_number} has finished (or failed): {self.current_election.completion_reason}."
        )

    async def notify_execution_complete(self, term_number: int):
        """
        Notify our peer replicas that we have finished executing the code for the specified election.

        :param term_number: the term of the election for which we served as leader and executed
        the user-submitted code.
        """
        election = self._elections.get(term_number, None)

        if election is None:
            raise ValueError(f"No election found for term number {term_number}. "
                             "Cannot notify peer replicas of execution complete...")
        elif not election.voting_phase_completed_successfully:
            raise ValueError(f"Election {term_number} has not yet completed or did not complete successfully. "
                             "Cannot notify peer replicas of execution complete...")
        elif self._node_id != election.winner_id:
            raise ValueError(f"We did not win election {term_number}. "
                             f"Instead, node {election.winner_id} won election {term_number}. "
                             "Cannot notify peer replicas of execution complete...")

        notification = ExecutionCompleteNotification(
            election.jupyter_message_id,
            proposer_id=self._node_id,
            election_term=term_number,
        )
        self.log.debug(f"Serializing & appending ExecutionCompleteNotification[Node={self._node_id},Term={term_number},"
                       f"ValueID={notification.id}] now.")
        await self.append_execution_end_notification(notification)
        self.log.debug("Finished serializing & appending "
                       f"ExecutionCompleteNotification[Node={self._node_id},Term={term_number},"
                       f"ValueID={notification.id}].")

    async def try_lead_execution(self, jupyter_message_id: str, term_number: int, target_replica_id: int = -1) -> bool:
        """
        Request to serve as the leader for the update of a term (and therefore to be the
        replica to execute user-submitted code).

        A subsequent call to append (without successfully being elected as leader) will fail.
        """
        if target_replica_id >= 1 and target_replica_id != self._node_id:
            raise ValueError(f"Target replica ID specified as {target_replica_id} "
                             f"but we're still proposing 'LEAD' as node {self._node_id}.")
            
        self.log.debug(f"RaftLog {self._node_id} is proposing to lead term {term_number}"
                       f"[target_replica_id = {target_replica_id}].")

        proposalOrVote: LeaderElectionProposal | LeaderElectionVote = await self._create_election_proposal_or_vote(
            ElectionProposalKey.LEAD, term_number, jupyter_message_id, target_replica_id = target_replica_id
        )

        # Orchestrate/carry out the election.
        is_leading: bool = await self._handle_election(
            proposalOrVote,
            target_term_number=term_number,
            jupyter_message_id=jupyter_message_id,
        )

        return is_leading

    async def try_yield_execution(self, jupyter_message_id: str, term_number: int, target_replica_id: int = -1) -> bool:
        """
        Request to explicitly yield the current term update (and therefore the execution of user-submitted code) to another replica.
        """
        if target_replica_id >= 1 and target_replica_id == self._node_id:
            raise ValueError(f"Target replica ID specified as our node ID {target_replica_id} "
                             f"but we're proposing 'YIELD'...")

        self.log.debug(f"RaftLog {self._node_id} is proposing to yield term {term_number}"
                       f"[target_replica_id = {target_replica_id}].")

        proposalOrVote: LeaderElectionProposal | LeaderElectionVote = await self._create_election_proposal_or_vote(
            ElectionProposalKey.YIELD, term_number, jupyter_message_id, target_replica_id = target_replica_id
        )

        # Orchestrate/carry out the election.
        is_leading: bool = await self._handle_election(
            proposalOrVote,
            target_term_number=term_number,
            jupyter_message_id=jupyter_message_id,
        )

        # If is_leading is True, then we have a problem, as we proposed YIELD.
        # We should never be elected leader if we propose YIELD.
        if is_leading:
            raise RuntimeError(f"we were elected leader of election {term_number} despite proposing 'YIELD'")

        # Return hard-coded False, as is_leading must be False.
        return False

    async def does_election_already_exist(self, jupyter_msg_id: str) -> bool:
        """
        Check if an election for the given Jupyter msg ID already exists.

        The Jupyter msg id would come from an "execute_request" or a
        "yield_request" message.
        """
        return jupyter_msg_id in self._elections_by_jupyter_message_id

    async def is_election_voting_complete(self, jupyter_msg_id: str) -> bool:
        """
        Check if an election for the given Jupyter msg ID already exists.

        If so, return True if the voting phase of the election is complete.

        The Jupyter msg id would come from an "execute_request" or a
        "yield_request" message.
        """
        if jupyter_msg_id not in self._elections_by_jupyter_message_id:
            return False

        election: Election = self._elections_by_jupyter_message_id[jupyter_msg_id]

        if election is None:
            self.log.error(f'Expected to find non-null election '
                           f'associated with Jupyter message "{jupyter_msg_id}"...')
            return False

        return election.voting_phase_completed_successfully

    async def is_election_execution_complete(self, jupyter_msg_id: str) -> bool:
        """
        Check if an election for the given Jupyter msg ID already exists.

        If so, return True if the execution phase election is complete.

        The Jupyter msg id would come from an "execute_request" or a
        "yield_request" message.
        """
        if jupyter_msg_id not in self._elections_by_jupyter_message_id:
            return False

        election: Election = self._elections_by_jupyter_message_id[jupyter_msg_id]

        if election is None:
            self.log.error(f'Expected to find non-null election '
                           f'associated with Jupyter message "{jupyter_msg_id}"...')
            return False

        return election.code_execution_completed_successfully
