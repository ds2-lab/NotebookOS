import asyncio
import datetime
import logging
import threading
from enum import IntEnum
from typing import Dict, Optional, List, MutableMapping, Any

import time

from .log import LeaderElectionVote, LeaderElectionProposal
from ..logs import ColoredLogFormatter

# Indicates that the election was completed because the elected leader finished executing the user-submitted code.
ExecutionCompleted = "execution_completed"

# Indicates that the election was completed because all replicas proposed yield.
# Note that when all replicas propose yield, the election will presumably be restarted,
# so it is not totally complete in the same way that it is when the elected leader of
# an election successfully finishes executing the user-submitted code.
AllReplicasProposedYield = "all_proposed_yield"

# Indicates that the election was simply skipped locally.
# This occurs if messages from our Local Daemon are dropped or delayed long enough for the election
# to be orchestrated by our peers, without our participation.
ElectionSkipped = "election_skipped"


class ElectionTimestamps(object):
    """
    ElectionTimestamps encapsulates some finer-grained metrics/timestamps about the election, namely how long
    each phase lasts.
    """

    def __init__(self, proposal_phase_start_time: float = 0):
        self.creation_time: float = time.time() * 1.0e3
        self.proposal_phase_start_time: float = proposal_phase_start_time  # includes the "voting" phase
        self.execution_phase_start_time: float = 0
        self.end_time: float = 0


class ElectionState(IntEnum):
    INACTIVE = 1  # Created, but not yet started.
    ACTIVE = 2  # Active, in progress
    VOTE_COMPLETE = 3  # The voting phase finished successfully, as in some node proposed 'LEAD' and was elected.
    FAILED = 4  # Failed, as in all nodes proposed 'YIELD', and the election has not been restarted yet.
    EXECUTION_COMPLETE = 5  # The execution of the associated user-submitted code has completed for this election.
    SKIPPED = 6  # Skipped because messages from our Local Daemon were delayed, and the election was orchestrated to its conclusion by our peers.

    def get_name(self) -> str:
        if self.value == 1:
            return "INACTIVE"
        elif self.value == 2:
            return "ACTIVE"
        elif self.value == 3:
            return "VOTE_COMPLETE"
        elif self.value == 4:
            return "FAILED"
        elif self.value == 5:
            return "EXECUTION_COMPLETE"
        elif self.value == 6:
            return "SKIPPED"
        else:
            raise ValueError(f"Unknown or unsupported Enum value for ElectionState: {self.value}")


class ElectionNotStartedError(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)


class ElectionAlreadyDecidedError(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

class Election(object):
    """
    Encapsulates the information about the current election term.
    """

    def __init__(
            self,
            term_number: int,
            num_replicas: int,
            jupyter_message_id: str,
            timeout_seconds: float = 10,
            future_io_loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._jupyter_message_id: str = jupyter_message_id

        # The term number of the election.
        # Each election has a unique term number.
        # Term numbers monotonically increase over time.
        self._term_number: int = term_number

        self._num_replicas: int = num_replicas
        """ # The number of replicas in the SMR cluster, so we know how many proposals to expect. """

        self._buffered_votes: List[LeaderElectionVote] = []
        """
        _buffered_votes serves the same purpose as _buffered_proposals, but _buffered_votes is for LeaderElectionVote
        objects, whereas _buffered_proposals is for LeaderElectionProposal objects.        
        """

        self._buffered_votes_lock: threading.Lock = threading.Lock()
        """
        Ensures atomic access to the _buffered_votes dictionary (required because we may be switching between
        multiple Python threads/goroutines that are accessing the _buffered_votes dictionary).        
        """

        self._buffered_proposals: List[LeaderElectionProposal]
        """
        If we receive a proposal with a larger term number than our current election, then it is possible
        that we simply received the proposal before receiving the associated "execute_request" or "yield_request" message
        that would've prompted us to start the election locally. So, we'll just buffer the proposal for now, and when
        we receive the "execute_request" or "yield_request" message, we'll process any buffered proposals at that point.     
        """

        self._buffered_proposals_lock: threading.Lock = threading.Lock()
        """
        Ensures atomic access to the _buffered_proposals dictionary (required because we may be switching between
        multiple Python threads/goroutines that are accessing the _buffered_proposals dictionary).
        """

        self._proposals: Dict[int, LeaderElectionProposal] = {}
        """ Mapping from SMR Node ID to the LeaderElectionProposal that it proposed during this election term. """

        self._election_timestamps: Dict[int, ElectionTimestamps] = {
            1: ElectionTimestamps(proposal_phase_start_time=time.time() * 1.0e3)}
        """ Mapping from attempt number to the associated ElectionTimestamps object. """
        self._current_election_timestamps: ElectionTimestamps = self._election_timestamps[1]

        # Set of node IDs for which we're missing proposals.
        # The set of node IDs for which we've received proposals is simply `self._proposals.keys()`.
        # This field is reset if the election is restarted.
        self._missing_proposals: set[int] = set()
        for node_id in range(1, num_replicas + 1):
            self._missing_proposals.add(node_id)

        self._vote_proposals: Dict[int, LeaderElectionVote] = {}
        """ Mapping from SMR Node ID to the LeaderElectionVote that it proposed during this election term. """

        # Mapping from SMR node ID to an inner mapping.
        # The inner mapping is a mapping from attempt number to the associated LeaderElectionProposal proposed by that node (during that attempt) during this election term.
        self._discarded_proposals: Dict[int, Dict[int, LeaderElectionProposal]] = {}

        # Mapping from SMR node ID to an inner mapping.
        # The inner mapping is a mapping from attempt number to the associated LeaderElectionVote proposed by that node (during that attempt) during this election term.
        self._discarded_vote_proposals: Dict[int, Dict[int, LeaderElectionVote]] = {}

        self._election_state: ElectionState = ElectionState.INACTIVE
        """ The current state/status of the election. """

        if future_io_loop is None:
            self._future_io_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        else:
            self._future_io_loop: asyncio.AbstractEventLoop = future_io_loop

        self._received_vote_future: Optional[asyncio.Future[Any]] = self._future_io_loop.create_future()
        self._received_vote_future_lock: threading.Lock = threading.Lock()

        self._leading_future_lock: threading.Lock = threading.Lock()
        self._leading_future: Optional[asyncio.Future[int]] = self._future_io_loop.create_future()
        """
        This is the future that we'll use to inform the local kernel replica if
        it has been selected to "lead" the election (and therefore execute the user-submitted code).
        Create local references.
        """

        # Flag mostly used for debugging/sanity-checking.
        # If we see that we've received 3 'YIELD' proposals, then the election already knows that it is going to fail.
        # The failure isn't made official until one of the replicas had a 'FAILURE' sync/vote proposal committed.
        self._expecting_failure: bool = False

        # This is set to True if the election automatically transitions itself to the 'FAILED' state upon receiving 3 'YIELD' proposals.
        # This field is reset if the election is restarted.
        self._auto_failed: bool = False

        # True if the winner for this election already been selected; otherwise, this is False.
        # This field is reset if the election is restarted.
        self._winner_selected: bool = False

        # The node ID of whatever node the local replica proposed as the winner.
        # This field is reset if the election is restarted.
        self._proposed_winner: int = 0

        self._first_lead_proposal: Optional[LeaderElectionProposal] = None
        """ 
        The first 'LEAD' proposal we received during this election.
        This field is reset if the election is restarted.        
        """

        self._num_discarded_proposals: int = 0
        """ 
        The number of proposals that have been discarded (due to being received after the timeout period).
        This value is reset if the election fails and is restarted.        
        """

        self._lifetime_num_discarded_proposals: int = 0
        """
        The total number of proposals that have been discarded (due to being received after the timeout period) 
        across all attempts/failures.
        """

        self._num_discarded_vote_proposals: int = 0
        """ The number of 'vote' proposals that have been discarded (due to being received after the timeout period). """

        self._num_lead_proposals_received: int = 0
        """ 
        The number of 'LEAD' proposals that we've received, excluding any proposals that were discarded.
        If the election fails (i.e., everybody proposes 'YIELD') and is later restarted, then this value is reset to 0.        
        """

        self._num_yield_proposals_received: int = 0
        """ 
        The number of 'YIELD' proposals that we've received, excluding any proposals that were discarded.
        If the election fails (i.e., everybody proposes 'YIELD') and is later restarted, then this value is reset to 0.        
        """

        self._discard_after: float = -1
        """ The time at which we will start discarding new proposals. """

        self._timeout: float = timeout_seconds
        """ The duration of time that must elapse before we start discarding new proposals. """

        self._winner_id: int = -1
        """ 
        The SMR node ID Of the winner of the last election.
        A value of -1 indicates that the winner has not been chosen/identified yet.        
        """

        self._num_restarts: int = 0
        """ The number of times that the election has been restarted. """

        self._current_attempt_number: int = 0
        """ The current attempt number of proposals (i.e., the largest attempt number we've received). """

        self._pick_and_propose_winner_future: Optional[asyncio.Future[Any]] = None
        """ Future created when the first 'LEAD' proposal for the current election is received. """

        self.election_finished_event: Optional[asyncio.Event] = asyncio.Event()
        """
        Condition that can be awaited until a notification is received that the leader has finished
        executing the user-submitted code for this election term.
        
        This condition is also marked as complete if a notification is received that all replicas
        proposed yield.
        """

        self._election_waiter_mutex: Optional[threading.Lock] = threading.Lock()
        self._election_finished_condition_waiter_loop: Optional[asyncio.AbstractEventLoop] = None
        """
        The asyncio IO loop that any entity waiting for the election to complete is expected to be running within.
        """

        self._election_decision_future_mutex: Optional[asyncio.Lock] = asyncio.Lock()
        self._election_decision_future: Optional[asyncio.Future[Any]] = self._future_io_loop.create_future()
        """
        This is the future we'll use to submit a formal vote for who should lead,
        based on the proposals that are committed to the etcd-raft log.
        """

        # Used with the self.election_waiter_cond variable.
        # self.election_waiter_mutex: Optional[asyncio.Lock] = asyncio.Lock()
        # Used to notify the thread/coroutine attempting to set the self.election_finished_event Event
        # that the self.election_finished_condition_waiter_loop field has been populated, as it is possible
        # for the coroutine that sets the self.election_finished_event to try to do so before the other
        # coroutine has had a chance to populate the self.election_finished_condition_waiter_loop field.
        # self.election_waiter_cond: Optional[asyncio.Condition] = asyncio.Condition(lock = self.election_waiter_mutex)

        # The completion_reason specifies why notify was called on the election_finished_event field.
        # The reasons can be that the leader has finished executing the user-submitted code for this election term,
        # or because all replicas proposed yield.
        self.completion_reason = "N/A"

        self.log: logging.Logger = logging.getLogger(__class__.__name__ + str(term_number))
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (f"Election[Term={self.term_number},"
                f"State={self.election_state},"
                f"AttemptNumber={self.current_attempt_number},"
                f"TotalProposalsReceived={self.num_proposals_received},"
                f"NumProposalsDiscarded={self.num_discarded_vote_proposals},"
                f"NumProposalsAccepted={self.num_proposals_accepted}"
                f"]")

    def buffer_vote(self, vote: LeaderElectionVote) -> None:
        """
        Append the specified LeaderElectionVote object to the list of buffered LeaderElectionVote objects.

        :param vote: the LeaderElectionVote to buffer.
        """
        with self._buffered_votes_lock:
            self._buffered_votes.append(vote)

    def get_election_metadata(self) -> Dict[str, Any]:
        """
        Returns: a dictionary of JSON-serializable metadata to be embedded in the "execute_reply" message
        that is sent back to the client following the conclusion of this election.
        """

        # Update "test_get_election_metadata" unit test if any fields are added/removed.
        metadata: Dict[str, Any] = {
            "term_number": self._term_number,
            "election_state": self._election_state,
            "election_state_string": self._election_state.get_name(),
            "winner_selected": self._winner_selected,
            "winner_id": self.winner_id,
            "proposals": {k: v.get_metadata() for k, v in self._proposals.items()},
            "vote_proposals": {k: v.get_metadata() for k, v in self._vote_proposals.items()},
            "discarded_proposals": {k: v.get_metadata() for k, v in self._discarded_proposals.items()},
            "num_discarded_proposals": self._num_discarded_proposals,
            "num_discarded_vote_proposals": self._num_discarded_vote_proposals,
            "num_lead_proposals_received": self._num_lead_proposals_received,
            "num_yield_proposals_received": self._num_yield_proposals_received,
            "num_restarts": self._num_restarts,
            "current_attempt_number": self._current_attempt_number,
            "completion_reason": self.completion_reason,
            "missing_proposals": list(self._missing_proposals),
        }

        return metadata

    def __getstate__(self):
        """
        Override so that we can omit any non-pickle-able fields, such as the `_pick_and_propose_winner_future` field.
        """
        state = self.__dict__.copy()

        keys_to_remove: List[str] = [
            "_pick_and_propose_winner_future", "election_finished_event",
            "log", "_future_io_loop", "_received_vote_future", "_received_vote_future_lock",
            "_leading_future", "_leading_future_lock", "election_finished_condition_waiter_loop",
            "_election_decision_future", "_election_decision_future_mutex", "election_waiter_mutex",
            "_buffered_proposals_lock", "_buffered_votes_lock"
        ]

        for key in keys_to_remove:
            if key in state:
                del state[key]

        self.log.debug(f"Election {self.term_number} returning state dictionary containing {len(state)} entries:")
        for key, val in state.items():
            self.log.debug(f"\"{key}\" ({type(val).__name__}): {val}")

        return state

    def __setstate__(self, state):
        """
        Override so that we can add back any non-pickle-able fields, such as the `_pick_and_propose_winner_future` field.
        """
        self.__dict__.update(state)
        self._pick_and_propose_winner_future: Optional[asyncio.Future[Any]] = None
        self.election_finished_event: Optional[asyncio.Event] = asyncio.Event()

        try:
            getattr(self, "election_finished_condition_waiter_loop")
        except AttributeError:
            self._election_finished_condition_waiter_loop: Optional[asyncio.AbstractEventLoop] = None

        try:
            getattr(self, "log")
        except AttributeError:
            self.log: logging.Logger = logging.getLogger(__class__.__name__ + str(self.term_number))
            self.log.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(ColoredLogFormatter())
            self.log.addHandler(ch)

    @property
    def buffered_votes(self) -> List[LeaderElectionVote]:
        """
        The LeaderElectionVote instances that were received before this election was started.
        """
        return self._buffered_votes

    @property
    def jupyter_message_id(self) -> str:
        """
        Return the Jupyter message ID from the "execute_request" (or "yield_request") associated with this election.
        """
        return self._jupyter_message_id

    @property
    def num_lead_proposals_received(self) -> int:
        """
        The number of 'LEAD' proposals received.
        """
        return self._num_lead_proposals_received

    @property
    def num_yield_proposals_received(self) -> int:
        """
        The number of 'YIELD' proposals received.
        """
        return self._num_yield_proposals_received

    @property
    def num_discarded_vote_proposals(self) -> int:
        """
        The number of 'vote' proposals that have been discarded (due to being received after the timeout period).
        """
        return self._num_discarded_vote_proposals

    @property
    def num_discarded_proposals(self) -> int:
        """
        The number of proposals that have been discarded (due to being received after the timeout period).
        """
        return self._num_discarded_proposals

    @property
    def lifetime_num_discarded_proposals(self) -> int:
        """
        The number of proposals that have been discarded (due to being received after the timeout period).
        """
        return self._lifetime_num_discarded_proposals

    @property
    def current_attempt_number(self) -> int:
        """
        The current attempt number of proposals (i.e., the largest attempt number we've received).
        """
        return self._current_attempt_number

    @property
    def num_restarts(self) -> int:
        """
        The number of times that the election has been restarted.
        """
        return self._num_restarts

    @property
    def winner(self) -> int:
        """
        Alias of `winner_id`.

        The SMR node ID Of the winner of the last election.
        A value of -1 indicates that the winner has not been chosen/identified yet.
        """
        return self._winner_id

    @property
    def first_lead_proposal(self) -> Optional[LeaderElectionProposal]:
        return self._first_lead_proposal

    @property
    def first_lead_proposal_at(self) -> float:
        """
        The time at which we received the first 'LEAD' proposal.

        -1 indicates that we've not yet received a 'LEAD' proposal.
        """
        if self._first_lead_proposal is None:
            return -1

        return self._first_lead_proposal.timestamp

    @property
    def winner_id(self) -> int:
        """
        The SMR node ID Of the winner of the last election.
        A value of -1 indicates that the winner has not been chosen/identified yet.
        """
        return self._winner_id

    @property
    def state(self) -> ElectionState:
        """
        Alias of `election_state`

        The current state/status of the election.
        """
        return self._election_state

    @property
    def election_state(self) -> ElectionState:
        """
        The current state/status of the election.
        """
        return self._election_state

    @property
    def current_election_timestamps(self) -> Optional[ElectionTimestamps]:
        return self._current_election_timestamps

    @property
    def is_in_failed_state(self) -> bool:
        return self._election_state == ElectionState.FAILED

    @property
    def voting_phase_completed_successfully(self) -> bool:
        """
        Return a bool indicating whether the voting phase of this election has completed,
        and that a leader was successfully elected.
        """

        # If the election has also finished the code-execution phase, then the voting phase is necessarily done,
        # so we also need to check on that.
        return self._election_state == ElectionState.VOTE_COMPLETE or self.code_execution_completed_successfully or self.was_skipped

    @property
    def code_execution_completed_successfully(self) -> bool:
        """
        Return a bool indicating whether the elected leader of this election has finished
        executing the user-submitted code.
        """
        return self._election_state == ElectionState.EXECUTION_COMPLETE or self._election_state == ElectionState.SKIPPED

    @property
    def has_been_started(self) -> bool:
        return self._election_state != ElectionState.INACTIVE

    @property
    def is_active(self) -> bool:
        return self._election_state == ElectionState.ACTIVE

    @property
    def was_skipped(self) -> bool:
        return self._election_state == ElectionState.SKIPPED

    @property
    def is_inactive(self) -> bool:
        return self._election_state == ElectionState.INACTIVE

    @property
    def term_number(self) -> int:
        """
        Return the term of this election.

        Each election has a unique term number. Term numbers monotonically increase over time.
        """
        return self._term_number

    @property
    def num_proposals_accepted(self) -> int:
        """
        The number of proposals we've received, NOT including discarded proposals.
        """
        return len(self._proposals)

    @property
    def num_proposals_received(self) -> int:
        """
        The number of proposals we've received, including discarded proposals.
        """
        return len(self._proposals) + self._num_discarded_proposals

    @property
    def num_vote_proposals_accepted(self) -> int:
        """
        The number of 'vote' proposals we've received, NOT including discarded 'vote' proposals.
        """
        return len(self._vote_proposals)

    @property
    def num_vote_proposals_received(self) -> int:
        """
        The number of 'vote' proposals we've received, including discarded 'vote' proposals.
        """
        return len(self._vote_proposals) + self._num_discarded_vote_proposals

    @property
    def proposals(self) -> MutableMapping[int, LeaderElectionProposal]:
        """
        Return a mapping from SMR Node ID to the LeaderElectionProposal proposed by that node during this election.
        """
        return self._proposals

    @property
    def election_decision_future(self) -> Optional[asyncio.Future[Any]]:
        """
        This is the future we'll use to submit a formal vote for who should lead,
        based on the proposals that are committed to the etcd-raft log.
        """
        return self._election_decision_future

    async def get_and_clear_election_decision_future(self) -> Optional[asyncio.Future[Any]]:
        async with self._election_decision_future_mutex:
            tmp: Optional[asyncio.Future[Any]] = self._election_decision_future
            self._election_decision_future = None
            return tmp

    @property
    def all_proposals_received(self) -> bool:
        """
        True if we've received a proposal from every node.
        """
        return len(self._proposals) == self._num_replicas

    def _try_set_pick_and_propose_winner_future_result(self):
        """
        Set the result on the `self._pick_and_propose_winner_future` asyncio.Future object, if it exists and has not already been completed.
        """
        # If this future is non-nil, then set its result so that it won't be repeated later.
        if self._pick_and_propose_winner_future is not None:
            assert not self._pick_and_propose_winner_future.done()
            self._pick_and_propose_winner_future.set_result(1)

    def _received_proposal_from_node(self, node_id: int) -> bool:
        return node_id in self._proposals

    def _discard_old_proposals(self, proposer_id: int = -1, latest_attempt_number: int = -1):
        """
        Discard all proposals and 'vote' proposals with attempt numbers < `latest_attempt_number`.

        If we discard the first-received 'LEAD' proposal from this term, then we'll also cancel and clear the `_pick_and_propose_winner_future` Future.
        """
        # We just have the option to not discard proposals for a particular node if we specify it via the `proposer_id` field.
        # As of right now, I don't think we ever do this. We always discard ALL proposals.
        if proposer_id >= 1:
            self.log.debug(
                f"Discarding proposals with attempt number < {latest_attempt_number}, excluding proposals from node {proposer_id}.")
        else:
            self.log.debug(f"Discarding proposals from ALL nodes with attempt number < {latest_attempt_number}.")

        num_discarded: int = 0
        # Iterate over all the proposals, checking which (if any) need to be discarded.
        # We do this even if the proposal we just received did not overwrite an existing proposal (in case a proposal was lost).
        to_remove: List[int] = []
        for prop_id, prop in self._proposals.items():
            # Skip/ignore the proposal for the node whose proposal we're adding.
            if prop_id == proposer_id:
                continue

            # If the attempt number is smaller, then mark the proposal for removal.
            if prop.attempt_number < latest_attempt_number:
                to_remove.append(prop_id)

                # If we're about to discard the first 'LEAD' proposal that we received, then set `self._first_lead_proposal` to None.
                if prop == self._first_lead_proposal:
                    self._first_lead_proposal = None
                    self._discard_after = -1

                    # Cancel this future if it exists. (It should probably exist, if the first 'LEAD' proposal also exists.)
                    if self._pick_and_propose_winner_future is not None:
                        self._pick_and_propose_winner_future.cancel()
                        self._pick_and_propose_winner_future = None

        # Remove proposals with smaller attempt numbers.
        for prop_id in to_remove:
            # Get the proposal that we're about to discard.
            proposal_to_discard: LeaderElectionProposal = self._proposals[prop_id]

            # Update the "discarded proposals" mapping.
            inner_prop_mapping: MutableMapping[int, LeaderElectionProposal] = self._discarded_proposals.get(
                self.term_number, {})
            inner_prop_mapping[proposal_to_discard.attempt_number] = proposal_to_discard

            # Delete the mapping in the main proposal dictionary.
            del self._proposals[prop_id]

            num_discarded += 1

        self.log.debug(
            f"Discarded {num_discarded} proposal(s) with term number < {latest_attempt_number} from the following nodes: {', '.join([str(prop_id) for prop_id in to_remove])}")
        num_votes_discarded: int = 0

        # Iterate over all the proposals, checking which (if any) need to be discarded.
        # We do this even if the proposal we just received did not overwrite an existing proposal (in case a proposal was lost).
        to_remove = []
        for prop_id, prop in self._vote_proposals.items():
            # Skip/ignore the proposal for the node whose proposal we're adding.
            if prop_id == proposer_id:
                continue

            # If the attempt number is smaller, then mark the proposal for removal.
            if prop.attempt_number < latest_attempt_number:
                to_remove.append(prop_id)

        # Remove proposals with smaller attempt numbers.
        for prop_id in to_remove:
            # Get the proposal that we're about to discard.
            vote_proposal_to_discard: LeaderElectionVote = self._vote_proposals[prop_id]

            # Update the "discarded 'vote' proposals" mapping.
            inner_vote_proposal_mapping: MutableMapping[int, LeaderElectionVote] = self._discarded_vote_proposals.get(
                self.term_number, {})
            inner_vote_proposal_mapping[vote_proposal_to_discard.attempt_number] = vote_proposal_to_discard

            # Delete the mapping in the main "vote" proposal dictionary.
            del self._vote_proposals[prop_id]
            num_votes_discarded += 1

        self.log.debug(
            f"Discarded {num_discarded} vote proposal(s) with term number < {latest_attempt_number} from nodes: {', '.join([str(prop_id) for prop_id in to_remove])}")

    def set_pick_and_propose_winner_future(self, future: asyncio.Future[Any]) -> None:
        self._pick_and_propose_winner_future = future

    def start(self) -> None:
        """
        Designate the election has having been started.

        If the election is already in the 'active' state, then this will raise a ValueError.

        State Transition:
        INACTIVE --> ACTIVE
        """
        if self._election_state == ElectionState.ACTIVE:
            raise ValueError(f"election for term {self.term_number} is already active")

        if self._election_state == ElectionState.VOTE_COMPLETE:
            raise ValueError(f"election for term {self.term_number} already completed successfully")

        if self._election_state == ElectionState.FAILED:
            raise ValueError(
                f"election for term {self.term_number} already failed; must be restarted (rather than started)")

        self._election_state = ElectionState.ACTIVE

        if self._current_election_timestamps is not None:
            self._current_election_timestamps.proposal_phase_start_time = time.time() * 1.0e3

    def restart(self, latest_attempt_number: int = -1) -> None:
        """
        Restart a failed election.

        :param latest_attempt_number: the new attempt number

        State Transition:
        FAILED --> ACTIVE
        """
        if not self.is_in_failed_state:
            # TODO: Is it necessarily the case that we will only ever want to restart 'FAILED' elections?
            # That's definitely the common/usual case. Perhaps with everything being locked (externally; the locking occurs in the raft log, not in here), we'll only want to restart failed elections...
            raise ValueError(
                f"election for term {self.term_number} is not in 'FAILED' state; instead, it is in state '{self._election_state}'. Cannot restart the election.")

        if latest_attempt_number <= 0:
            raise ValueError(
                f"invalid 'lastest attempt number' {latest_attempt_number} when restarting election for term {self.term_number} (must be > 0).")

        if self._election_state != ElectionState.FAILED:
            raise ValueError(
                f"election for term {self.term_number} is not in 'FAILED' state and thus cannot be restarted (current state: {self._election_state})")

        self._pick_and_propose_winner_future = None
        self._election_state = ElectionState.ACTIVE
        self._num_restarts += 1

        self._current_election_timestamps = ElectionTimestamps(proposal_phase_start_time=time.time() * 1.0e3)
        self._election_timestamps[latest_attempt_number] = self._current_election_timestamps

        self.completion_reason = "N/A"
        self.election_finished_event.clear()  # Reset the asyncio.Event so that it can be reused.

        # Repopulate the 'missing node IDs' set.
        self._missing_proposals.clear()
        for node_id in range(1, self._num_replicas + 1):
            self._missing_proposals.add(node_id)

        # Reset these counters.
        self._num_lead_proposals_received = 0
        self._num_yield_proposals_received = 0
        self._num_discarded_proposals = 0
        self._proposed_winner = 0
        self._winner_selected = False
        self._auto_failed = False
        self._first_lead_proposal = None

        # Discard old proposals.
        # We pass -1 for `proposer_id` because we want to wipe away ALL proposals from ALL nodes.
        self._discard_old_proposals(proposer_id=-1, latest_attempt_number=latest_attempt_number)

        # Re-create this Future.
        self._election_decision_future = self._future_io_loop.create_future()

        self._proposals.clear()
        self._vote_proposals.clear()

    def set_election_failed(self):
        """
        Record that the election has failed. This transitions the election to the FAILED state.

        State Transition:
        ACTIVE --> FAILED
        """
        # We should only fail active elections.
        # The exception is that the election could've automatically "failed itself" if it received 3 'YIELD' proposals.
        # Elections automatically transition to the 'FAILED' state in this case.
        # This helps avoid issues in which there may be network delays for the last 'SYNC' proposal, committing the FAILURE.
        # If a replica sees 3 'YIELD' proposals, then the election necessarily failed; there's no way for it to continue without being restarted.
        if self._election_state != ElectionState.ACTIVE and not self._auto_failed:
            raise ValueError(
                f"election is in invalid state {self._election_state} (and the election did not automatically fail itself).")

        if not self._expecting_failure:
            self.log.warning(
                f"Election failed, but we weren't expecting a failure... NumLead: {self._num_lead_proposals_received}; NumYield: {self._num_yield_proposals_received}, NumDiscarded: {self.num_discarded_proposals}.")

        self._election_state = ElectionState.FAILED

        self.completion_reason = AllReplicasProposedYield

        if self._election_finished_condition_waiter_loop is None:
            raise ValueError("Reference to EventLoop on which someone should be waiting on the "
                             "Election Finished condition is None...")

        # If (somehow) the running event loop is the same as the one in which the waiter is waiting,
        # then we can call self.election_finished_event.set() directly.
        #
        # Otherwise, we have to use call_soon_threadsafe to call the self.election_finished_event.set method.
        current_event_loop: Optional[asyncio.AbstractEventLoop] = None
        try:
            current_event_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

        if current_event_loop != self._election_finished_condition_waiter_loop:
            self._election_finished_condition_waiter_loop.call_soon_threadsafe(self.election_finished_event.set)
        else:
            self.election_finished_event.set()

        self.log.debug(f"Election {self.term_number} has failed (in attempt {self.current_attempt_number}).")

    @property
    def received_vote_future(self)->Optional[asyncio.Future]:
        return self._received_vote_future

    def get_and_clear_received_vote_future(self)->Optional[asyncio.Future]:
        """
        Return the current value of the "Received Vote" asyncio.Future, and then
        set the value of the instance variable to None.
        """
        with self._received_vote_future_lock:
            future: asyncio.Future = self._received_vote_future
            self._received_vote_future = None
            return future

    @property
    def leading_future(self)->Optional[asyncio.Future]:
        with self._leading_future_lock:
            return self._leading_future

    def get_and_clear_leading_future(self)->Optional[asyncio.Future[int]]:
        """
        Return the current value of the "Leading" asyncio.Future, and then
        set the value of the instance variable to None.
        """
        with self._leading_future_lock:
            future: asyncio.Future = self._leading_future
            self._leading_future = None
            return future

    @received_vote_future.setter
    def received_vote_future(self, future: asyncio.Future):
        self._received_vote_future = future

    @property
    def future_io_loop(self)->Optional[asyncio.AbstractEventLoop]:
        return self._future_io_loop

    @future_io_loop.setter
    def future_io_loop(self, loop: asyncio.AbstractEventLoop):
        assert loop is not None

        self._future_io_loop = loop

        if not hasattr(self, "_received_vote_future") or self._received_vote_future is None:
            self._received_vote_future = loop.create_future()
        elif self._received_vote_future.get_loop() != loop:
            raise ValueError(f"'Received Vote' future is already created on "
                             f"different loop for election {self.term_number}.")

    def set_election_finished_condition_waiter_loop(self, loop: asyncio.AbstractEventLoop):
        with self._election_waiter_mutex:
            self._election_finished_condition_waiter_loop = loop

    async def wait_for_election_to_end(self):
        """
        Wait for the election to end (or enter the failed state), either because the elected leader of this election
        successfully finished executing the user-submitted code, or because all replicas proposed YIELD.
        """
        if self._election_finished_condition_waiter_loop is not None:
            assert self._election_finished_condition_waiter_loop == asyncio.get_running_loop()
        else:
            self._election_finished_condition_waiter_loop = asyncio.get_running_loop()

        if self.code_execution_completed_successfully:
            return

        await self.election_finished_event.wait()

    def set_execution_complete(
            self,
            fast_forwarding: bool = False,
            catching_up: bool = False,
            fast_forwarded_winner_id: int = -1
    ):
        """
        Records that the elected leader of this election successfully finished executing the user-submitted code.

        If fast_forwarding is True, then we don't care if the election_finished_condition_waiter_loop instance
        variable is None, as we're presumably just creating and completing elections one-after-another in order to
        catch up to whatever term number was specified in a "execution complete" notification that we just received
        "out of the blue" (i.e., before receiving the "execute_request" or "yield_request" for that election from
        our Local Daemon). If we're fast-forwarding, then we expect the condition to be None.

        State Transition:
        VOTE_COMPLETE --> EXECUTION_COMPLETE
        """
        if self._election_state != ElectionState.VOTE_COMPLETE:
            self.log.warning(f"Election for term {self.term_number} is not in voting-complete state "
                             f"(current state: {self._election_state}), and yet we're transitioning "
                             f"to execution-complete state")

        if fast_forwarding:
            self.log.warning(f"Skipping election {self.term_number}.")

            self._election_state = ElectionState.SKIPPED
            self._winner_id = fast_forwarded_winner_id
            self.completion_reason = ElectionSkipped
        else:
            self._election_state = ElectionState.EXECUTION_COMPLETE
            self.completion_reason = ExecutionCompleted

        if self._current_election_timestamps is not None:
            self._current_election_timestamps.end_time = time.time() * 1.0e3

        # As mentioned above, we only care if the condition is None if fast_forwarding and catching_up are False.
        # If we're fast-forwarding or catching up after a migration, then we expect the condition to be None.
        if self._election_finished_condition_waiter_loop is None and not fast_forwarding and not catching_up:
            raise ValueError("Reference to EventLoop on which someone should be waiting on the "
                             "Election Finished condition is None...")

        current_event_loop: Optional[asyncio.AbstractEventLoop] = None
        try:
            current_event_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

        # If (somehow) the running event loop is the same as the one in which the waiter is waiting,
        # then we can call self.election_finished_event.set() directly.
        #
        # Otherwise, we have to use call_soon_threadsafe to call the self.election_finished_event.set method.
        if self._election_finished_condition_waiter_loop is not None and current_event_loop != self._election_finished_condition_waiter_loop:
            self._election_finished_condition_waiter_loop.call_soon_threadsafe(self.election_finished_event.set)
        elif self.election_finished_event is not None:
            self.election_finished_event.set()

        if fast_forwarding:
            self.log.debug(f"Election {self.term_number} has successfully been skipped.")
        else:
            self.log.debug(f"The code execution phase for election {self.term_number} has completed.")

    def set_election_vote_completed(self, winner_id: int):
        """
        Take note that the voting phase of the election has been completed successfully.

        State Transition:
        ACTIVE --> VOTE_COMPLETE
        """
        if self._election_state != ElectionState.ACTIVE:
            raise ValueError(f"Election for term {self.term_number} is not active "
                             f"(current state: {self._election_state.get_name()}); cannot complete election")

        self._winner_id = winner_id
        self._election_state = ElectionState.VOTE_COMPLETE

        if self._current_election_timestamps is not None:
            self._current_election_timestamps.execution_phase_start_time = time.time() * 1.0e3

        self.log.debug(f"The voting phase for election {self.term_number} "
                       f"has completed successfully with winner: node {winner_id}.")

    def format_accepted_proposals(self) -> str:
        """

        Returns:

        """
        # TODO: Should this method be locked?

        if len(self._proposals) == 0:
            return ""

        formatted_proposals: list[str] = []
        for node_id, proposal in self._proposals.items():
            formatted_proposal: str = f"Node {node_id}: \"{proposal.election_proposal_key}\""
            formatted_proposals.append(formatted_proposal)

        return ", ".join(formatted_proposals)

    def format_discarded_proposals(self) -> str:
        """

        Returns:

        """
        # TODO: Should this method be locked?

        if len(self._discarded_proposals) == 0:
            return ""

        _discarded_proposals: Dict[int, LeaderElectionProposal] = self._discarded_proposals.get(
            self._current_attempt_number, {})
        if len(_discarded_proposals) == 0:
            return ""

        formatted_proposals: list[str] = []
        for node_id, proposal in _discarded_proposals.items():
            formatted_proposal: str = f"Node {node_id}: \"{proposal.election_proposal_key}\""
            formatted_proposals.append(formatted_proposal)

        return ", ".join(formatted_proposals)

    def pick_winner_to_propose(self, last_winner_id: int = -1) -> int:
        """
        Determine who should be proposed as the winner of the election based on the current proposals that have been received.

        Return the SMR node ID of the node that should be proposed as the winner.

        Raises ValueError if no winner should be proposed at this time, such as due to an insufficient number of proposals having been received.
        """
        current_time: float = time.time()

        if self._election_state == ElectionState.INACTIVE:
            self.log.warning(f"election for term {self._term_number} "
                             "has not yet been started; cannot identify winner to propose")
            raise ElectionNotStartedError(f"election for term {self._term_number} "
                                          "has not yet been started; cannot identify winner to propose")

        if self._election_state == ElectionState.VOTE_COMPLETE:
            self.log.warning(f"election for term {self._term_number} "
                             "has already completed successfully; cannot identify winner to propose")
            raise ElectionAlreadyDecidedError(f"election for term {self._term_number} "
                                              "has already completed successfully; cannot identify winner to propose")

        if self._winner_selected:
            self.log.warning(f"election for term {self._term_number} "
                             f"already selected a node to propose as winner: node {self._proposed_winner}")
            raise ElectionAlreadyDecidedError(f"election for term {self._term_number} "
                                              f"already selected a node to propose as winner: "
                                              f"node {self._proposed_winner}")

        # If the election isn't active, then we shouldn't be proposing anybody.
        # if self._election_state == ElectionState.FAILED and not self._winner_selected:
        #     self._try_set_pick_and_propose_winner_future_result()
        #     self._winner_selected = True
        #     return -1

        # If `self._discard_after` hasn't even been set yet, then we should keep waiting for more proposals before
        # making a decision.
        #
        # Likewise, if `self._discard_after` has been set already, but there's still time to receive more proposals,
        # then we should wait before making a decision.
        #
        # Note that `self._discard_after` is reset when an election is reset.
        should_wait: bool = self._discard_after == -1 or (self._discard_after > 0 and self._discard_after > current_time)

        # If we've not yet received all proposals AND we should keep waiting, then we'll raise a ValueError,
        # indicating that we should not yet select a node to propose as winner.
        #
        # If we have received all proposals, or if we'll be discarding any future proposals that we receive,
        # then we should go ahead and try to decide.
        if (len(self._proposals) + self._num_discarded_proposals < self._num_replicas) and should_wait:
            self.log.debug(f"Cannot pick winner for election {self.term_number} yet. "
                           f"Received: {len(self._proposals)} ({self.format_accepted_proposals()}, "
                           f"discarded: {self._num_discarded_proposals} ({self.format_discarded_proposals()}), "
                           f"number of replicas: {self._num_replicas}.")
            raise ValueError(f"insufficient number of proposals received to select a winner to propose "
                             f"(received: {len(self._proposals)}, discarded: {self._num_discarded_proposals}, "
                             f"number of replicas: {self._num_replicas})")

        # By default, we'll return -1, indicating that we should propose FAILURE.
        # But if we have someone else to propose -- either the first 'LEAD' proposal from this term,
        # or the winner of the last election if they proposed 'LEAD' again this term,
        # then we'll return that someone else instead of returning -1.
        self._proposed_winner = -1

        # If we know the last winner, and we have a proposal from them,
        # and the winner of the last election proposed 'LEAD' again,
        # then we'll propose that they lead this election.
        if last_winner_id > 0 and self._received_proposal_from_node(last_winner_id) and self._proposals[last_winner_id].is_lead:
            self._proposed_winner = last_winner_id
            self.log.debug(f"Will propose node {self._proposed_winner}; node {self._proposed_winner} won last time, "
                           f"and they proposed 'LEAD' again during this election (term {self._term_number}).")
        # If we've received at least one 'LEAD' proposal, then return the node ID of whoever proposed 'LEAD' first.
        elif self._first_lead_proposal is not None:
            self._proposed_winner = self._first_lead_proposal.proposer_id
            self.log.debug(f"Will propose node {self._proposed_winner}; first 'LEAD' proposal for election term "
                           f"{self._term_number} came from node {self._proposed_winner}.")
        else:
            self.log.warning("We have nobody to propose as the winner of the election. Proposing 'FAILURE'")

        # If this future is non-nil, then set its result so that it won't be repeated later.
        self._try_set_pick_and_propose_winner_future_result()
        self._winner_selected = True
        return self._proposed_winner

    def add_vote_proposal(self, vote: LeaderElectionVote, overwrite: bool = False, received_at=time.time()) -> bool:
        """
        Register a proposed LeaderElectionProposal.

        If `overwrite` is False and there is already a LeaderElectionProposal registered for the associated node, then a ValueError is raised.
        If `overwrite` is True, then the existing/already-registered LeaderElectionProposal is simply overwritten.

        Let P' be a new proposal from Node N. Let P be the existing proposal from Node N.
        If TERM_NUMBER(P') > TERM_NUMBER(P) and `overwrite` is True, then P will be replaced with P'.

        Let X be the set of proposals from the other nodes (i.e., nodes whose ID != N). For each proposal x in X, if TERM_NUMBER(x) < TERM_NUMBER(P'), then x will be thrown out/discarded.

        That is, when overwriting an existing proposal with a new proposal, any other proposals (from other nodes) whose attempt numbers are lower than the new proposal will be discarded.

        Returns:
            (bool) True if this is the first vote proposal (of whatever attempt number the proposal has) that has been received during this election, otherwise False
        """
        if vote.election_term != self.term_number:
            self.log.error(
                f"Attempting to add VOTE from term {vote.election_term} to election {self.term_number}: {vote}")
            raise ValueError(
                f"vote proposal's term {vote.election_term} differs from target election with term {self.term_number}")

        proposer_id: int = vote.proposer_id
        current_attempt_number: int = vote.attempt_number

        # Update the current attempt number if the newly-received proposal has a greater attempt number than any of the other proposals that we've seen so far.
        if current_attempt_number > self._current_attempt_number:
            self.log.debug(f"Received vote for node {vote.proposed_node_id} from node "
                           f"{vote.proposer_id} with attempt number {vote.attempt_number}. "
                           f"Current attempt number is {self._current_attempt_number}. "
                           f"Setting attempt number to new value of {vote.attempt_number}.")
            self._current_attempt_number = current_attempt_number

        # Check if there's already an existing proposal.
        # If so, and if overwrite is False, then we raise a ValueError.
        existing_proposal: Optional[LeaderElectionVote] = self._vote_proposals.get(proposer_id, None)
        if existing_proposal is not None:
            prev_attempt_number: int = existing_proposal.attempt_number

            # Raise an error if we've not been instructed to overwrite.
            # If we have been instructed to overwrite, then we'll end up storing the new proposal at the end of this method.
            if prev_attempt_number >= current_attempt_number and not overwrite:
                raise ValueError(
                    f"new proposal has attempt number {current_attempt_number}, whereas previous/existing proposal has attempt number {prev_attempt_number}")

        # TODO: Modified this to only discard proposals if we've received at least one so far. If we haven't received any yet, then we still accept proposals after the delay.
        if 0 < self._discard_after < received_at and len(
                self._vote_proposals) >= 1:  # `self._discard_after` is initially equal to -1, so it isn't valid until it is set to a positive value.
            self._num_discarded_vote_proposals += 1
            self.log.debug(
                f"Discarding 'VOTE' proposal from node {vote.proposer_id}, as it was received after deadline. (Deadline: {datetime.datetime.fromtimestamp(self._discard_after).strftime('%Y-%m-%d %H:%M:%S.%f')}, Received at: {datetime.datetime.fromtimestamp(received_at).strftime('%Y-%m-%d %H:%M:%S.%f')})")
            return False

        # Store the new proposal.
        self._vote_proposals[proposer_id] = vote

        # If the length is 1, then this is the first "vote" proposal received.
        # If the length is not 1, then this is not the first.
        return len(self._vote_proposals) == 1

    def add_proposal(self, proposal: LeaderElectionProposal, future_io_loop: asyncio.AbstractEventLoop,
                     received_at: float = time.time()) -> Optional[tuple[asyncio.Future[Any], float]]:
        """
        Register a proposed LeaderElectionProposal.

        Let P' be a new proposal from Node N. Let P be the existing proposal from Node N.
        If TERM_NUMBER(P') > TERM_NUMBER(P) and `overwrite` is True, then P will be replaced with P'.

        Let X be the set of proposals from the other nodes (i.e., nodes whose ID != N). For each proposal x in X, if TERM_NUMBER(x) < TERM_NUMBER(P'), then x will be thrown out/discarded.

        That is, when overwriting an existing proposal with a new proposal, any other proposals (from other nodes) whose attempt numbers are lower than the new proposal will be discarded.

        Returns:
            tuple:
                When the proposal being added is the first proposal to be received, then this method will return a tuple containing a Future and a timeout.
                Note that if the election is restarted, then the 'first lead proposal' is reset to None, so the next lead proposal received will trigger this case.
                The future that is returned should be resolved after the timeout.

                When the proposal being added is NOT the first proposal to be received, then this method will return None.

        """
        if proposal.election_term != self.term_number:
            error_msg: str = (f"\"{proposal.key}\" proposal from node {proposal.proposer_id} "
                              f"(ts={datetime.datetime.fromtimestamp(proposal.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')}) "
                              f"is from term {proposal.election_term}, whereas this election is for term {self.term_number}.")
            self.log.error(error_msg)
            raise ValueError(error_msg)

        # Update the current attempt number if the newly-received proposal has a greater attempt number than any of the other proposals that we've seen so far.
        if proposal.attempt_number > self._current_attempt_number:
            self.log.debug(f"Election {self._term_number} received \"{proposal.key}\" proposal "
                           f"from node {proposal.proposer_id} with attempt number {proposal.attempt_number}. "
                           f"Current attempt number is {self._current_attempt_number}. "
                           f"Setting attempt number to new value of {proposal.attempt_number}.")

            old_largest: int = self._current_attempt_number
            self._current_attempt_number = proposal.attempt_number

            # If we previously failed and just got a new proposal with a new highest attempt number, then restart!
            # We'll purge/discard the old proposals down below.
            if self._election_state == ElectionState.FAILED:
                self.log.debug(
                    f"Election {self.term_number} restarting. "
                    f"Received new highest attempt number ({proposal.attempt_number}; prev: {old_largest}).")
                self.restart(latest_attempt_number=proposal.attempt_number)

        # Check if we need to discard the proposal due to there already being an existing proposal from that node with an attempt number >= the attempt number of the proposal we just received.
        # If the new proposal has an attempt number equal to or lower than the last proposal we received from this node, then that's a problem.
        existing_proposal: Optional[LeaderElectionProposal] = self._proposals.get(proposal.proposer_id, None)
        if existing_proposal is not None and existing_proposal.attempt_number >= proposal.attempt_number:
            self.log.warning(
                f"Discarding proposal from node {proposal.proposer_id} during election term {proposal.election_term} "
                f"because new proposal has attempt number ({proposal.attempt_number}) <= existing proposal "
                f"from same node's attempt number ({existing_proposal.attempt_number}).")
            return None

        # Check if we need to discard the proposal due to it being received late.
        if 0 < self._discard_after < received_at:  # `self._discard_after` is initially equal to -1, so it isn't valid until it is set to a positive value.
            self._num_discarded_proposals += 1
            self._lifetime_num_discarded_proposals += 1
            self.log.warning(
                f"Discarding proposal from node {proposal.proposer_id} during election term {proposal.election_term} because it was received too late.")
            return None

        # We're not discarding the proposal, so let's officially store the new proposal.
        self._proposals[proposal.proposer_id] = proposal
        self._missing_proposals.remove(proposal.proposer_id)

        if proposal.is_lead:
            self._num_lead_proposals_received += 1
            self.log.debug(
                f"Accepted \"LEAD\" proposal from node {proposal.proposer_id} with attempt number {proposal.attempt_number}. "
                f"NumLead: {self._num_lead_proposals_received}; NumYield: {self._num_yield_proposals_received}.")
        else:
            self._num_yield_proposals_received += 1
            self.log.debug(
                f"Accepted \"YIELD\" proposal from node {proposal.proposer_id} with attempt number {proposal.attempt_number}. "
                f"NumLead: {self._num_lead_proposals_received}; NumYield: {self._num_yield_proposals_received}.")

            if self._num_yield_proposals_received >= 3:
                self._expecting_failure = True
                self.log.warning(
                    "Received third 'YIELD' proposal. Election is doomed to fail! Automatically transitioning to the 'FAILED' state.")
                self.set_election_failed()
                self._auto_failed = True

        self.log.debug(
            f"Received proposals from node(s): {', '.join([str(k) for k in self._proposals.keys()])}. "
            f"Missing proposals from node(s): {', '.join([str(k) for k in self._missing_proposals])}")

        # Check if this is the first 'LEAD' proposal that we've received.
        # If so, we'll return a future that can be used to ensure we make a decision after the timeout period, even if we've not received all other proposals.
        if self._first_lead_proposal is None and proposal.is_lead:
            self._first_lead_proposal = proposal
            self._discard_after = time.time() + self._timeout

            self._pick_and_propose_winner_future = asyncio.Future(loop=future_io_loop)

            return self._pick_and_propose_winner_future, self._discard_after

        # It wasn't the first 'LEAD' proposal, so we just return None.
        return None
