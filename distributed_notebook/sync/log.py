import datetime
import time
import uuid
from enum import Enum
from json import JSONEncoder
from typing import Optional, Any, List, Dict

from typing_extensions import Protocol, runtime_checkable

KEY_FAILURE = "_failure_"  # We cannot execute this request...
KEY_NONE: str = ""
KEY_SYNC_END: str = "_end_"
KEY_CATCHUP: str = "_catchup_"
OP_SYNC_ADD: str = "add"
OP_SYNC_PUT: str = "put"
OP_SYNC_DEL: str = "del"
OP_NONE: str = ""


class ElectionProposalKey(Enum):
    YIELD = "_yield_"  # Propose to yield the execution to another replica.
    LEAD = "_lead_"  # Propose to lead the execution term (i.e., execute the user's code).
    SYNC = "_sync_"  # Synchronize to confirm decision about who is executing the code.


class SynchronizedValueJsonEncoder(JSONEncoder):
    def default(self, o):
        print(f"SynchronizedValueJsonEncoder called with obj o={o}")
        return o.__dict__


class SynchronizedValue(object):
    """
    Base class for a value for log proposal.

    This is an updated/rewritten version of the SyncValue class.
    """

    def __init__(
            self,
            tag: Any,
            data: Any,  # The value/data attached to the proposal.
            proposer_id: int = -1,  # The SMR node ID of the node proposing this value.
            attempt_number: int = -1,
            # Serves as a sort of "sub-term", as elections can be re-tried if they fail (i.e., if everyone proposes "YIELD")
            election_term: int = -1,  # The election term on which this value is intended to be proposed.
            prmap: Optional[list[str]] = None,
            should_end_execution: bool = False,
            jupyter_message_id: str = "",
            key: str = KEY_NONE,
            operation: str = OP_NONE,
    ):
        self._tag: Any = tag
        self._proposer_id: int = proposer_id
        self._election_term: int = election_term
        self._attempt_number: int = attempt_number
        self._data: Any = data
        self._id: str = str(uuid.uuid4())
        self._timestamp: float = time.time()
        self._operation: str = operation
        self._jupyter_message_id: str = jupyter_message_id

        self._should_end_execution: bool = should_end_execution
        self._prmap: Optional[list[str]] = prmap
        self._key: str = key

    @property
    def jupyter_message_id(self)->Optional[str]:
        if hasattr(self, "_jupyter_message_id"):
            return self._jupyter_message_id

        return None

    @jupyter_message_id.setter
    def jupyter_message_id(self, _jupyter_message_id: str):
        self._jupyter_message_id = _jupyter_message_id

    def get_metadata(self) -> Dict[str, Any]:
        """
        Returns a dictionary containing this SynchronizedValue's fields, suitable for JSON serialization.
        """
        metadata: Dict[str, Any] = {
            "key": self._key,
            "op": self._operation,
            "end": self._should_end_execution,
            "tag": self._tag,
            "proposer": self.proposer_id,
            "election_term": self.election_term,
            "attempt_number": self._attempt_number,
            "timestamp": datetime.datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f'),
            "id": self._id
        }

        if hasattr(self, "_jupyter_message_id"):
            metadata["jupyter_message_id"] = getattr(self, "_jupyter_message_id")

        return metadata

    def __str__(self):
        string: str = (f"SynchronizedValue[Key={self._key},Op={self._operation},End={self._should_end_execution},"
                       f"Tag={self._tag},ProposerID={self.proposer_id},ElectionTerm={self.election_term},"
                       f"AttemptNumber={self._attempt_number},"
                       f"Timestamp={datetime.datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')},"
                       f"ID={self._id}")

        if hasattr(self, "_jupyter_message_id"):
            jupyter_message_id: Optional[str] = getattr(self, "_jupyter_message_id")
            string += f",JupyterMsgId={jupyter_message_id}]"
        else:
            string += "]"

        return string

    @property
    def tag(self) -> Any:
        return self._tag

    @property
    def key(self) -> str:
        return self._key

    def set_key(self, key: str) -> None:
        self._key = key

    def set_proposer_id(self, id: int) -> None:
        self._proposer_id = id

    @property
    def should_end_execution(self) -> bool:
        return self._should_end_execution

    def set_should_end_execution(self, should_end_execution: bool) -> None:
        self._should_end_execution = should_end_execution

    @property
    def prmap(self) -> Optional[list[str]]:
        return self._prmap

    def set_prmap(self, prmap: Optional[list[str]]) -> None:
        self._prmap = prmap

    @property
    def has_operation(self) -> bool:
        """
        True if the SynchronizedValue has an explicitly-defined operation (i.e., the `self._operation` field is not the empty string).
        """
        return len(self.operation) > 0

    @property
    def op(self) -> str:
        """
        Alias of `self.operation`

        The named operation of the synchronized value, which may be the empty string.
        """
        return self.operation

    @property
    def operation(self) -> str:
        """
        The named operation of the synchronized value, which may be the empty string.
        """
        return self._operation

    @property
    def proposer_id(self) -> int:
        """
        # The SMR node ID of the node proposing this value.
        """
        return self._proposer_id

    @property
    def timestamp(self) -> float:
        """
        The timestamp at which this proposal was created.
        """
        return self._timestamp

    @property
    def ts(self) -> float:
        """
        Alias of `self.timestamp`

        The timestamp at which this proposal was created.
        """
        return self.timestamp

    @property
    def id(self) -> str:
        """
        Return the UUID v4 uniquely identifying this proposed value.
        """
        return self._id

    @property
    def data(self) -> Any:
        return self._data

    def set_data(self, data: Any) -> None:
        self._data = data

    @property
    def election_term(self) -> int:
        return self._election_term

    def set_election_term(self, term) -> None:
        self._election_term = term

    def set_attempt_number(self, attempt_number)->None:
        self._attempt_number = attempt_number

    @property
    def attempt_number(self) -> int:
        return self._attempt_number


class ExecutionCompleteNotification(SynchronizedValue):
    """
    A special type of SynchronizedValue used to notify peer replicas that the execution of user-submitted
    code by the leader replica has completed for a particular election term.
    """

    def __init__(self, jupyter_message_id: str, **kwargs):
        super().__init__(None, None, **kwargs)

        self._jupyter_message_id: str = jupyter_message_id

    @property
    def jupyter_message_id(self):
        return self._jupyter_message_id


class LeaderElectionProposal(SynchronizedValue):
    """
    A special type of SynchronizedValue encapsulating a "LEAD" or "YIELD" proposal during a leader election.
    """

    def __init__(self, jupyter_message_id: str = "", **kwargs):
        if "key" not in kwargs:
            raise ValueError(
                "Must specify a \"key\" keyword argument when creating an instance of `LeaderElectionProposal`")

        # LeaderElectionProposals cannot have data.
        super().__init__(None, None, **kwargs)

        self._jupyter_message_id: str = jupyter_message_id

    @property
    def jupyter_message_id(self):
        return self._jupyter_message_id

    @property
    def is_lead(self) -> bool:
        return self._key == str(ElectionProposalKey.LEAD)

    @property
    def is_yield(self) -> bool:
        return self._key == str(ElectionProposalKey.YIELD)

    @property
    def election_proposal_key(self) -> str:
        """
        Alias for `self.key`.

        The "key" of this proposal, which indicates whether it is a LEAD or a YIELD proposal.
        """
        return self._key


class BufferedLeaderElectionProposal(object):
    """
    This simply wraps a LeaderElectionProposal along with the timestamp at which the proposal was originally received.
    The purpose is so we can pass the correct timestamp when we eventually process the buffered proposal.
    """

    def __init__(self, proposal: LeaderElectionProposal, received_at: float = time.time(), jupyter_message_id: str = ""):
        self._proposal: LeaderElectionProposal = proposal
        self._received_at: float = received_at
        self._jupyter_message_id: str = jupyter_message_id

    @property
    def jupyter_message_id(self):
        return self._jupyter_message_id

    @property
    def proposal(self) -> LeaderElectionProposal:
        """
        :return: the `LeaderElectionProposal` that is wrapped by this `BufferedLeaderElectionProposal` instance.
        """
        return self._proposal

    @property
    def received_at(self) -> float:
        return self._received_at


class LeaderElectionVote(SynchronizedValue):
    """
    A special type of SynchronizedValue encapsulating a vote for a leader during a leader election.

    These elections occur when code is submitted for execution by the user.

    This used to be 'SYNC' in the original RaftLog implementation.
    """

    def __init__(self, proposed_node_id: int, jupyter_message_id:str = "", **kwargs):
        if "key" in kwargs:
            _key = kwargs.pop("key")

            if _key != str(ElectionProposalKey.SYNC):
                raise ValueError(
                    f"the \"key\" keyword argument must be equal to \"{ElectionProposalKey.SYNC}\" for `LeaderElectionVote`. This is handled automatically; no need to pass a \"key\" keyword argument.")
        else:
            kwargs["key"] = str(ElectionProposalKey.SYNC)

        super().__init__(None, None, **kwargs)

        # The SMR node ID of the node being voted for
        self._proposed_node_id: int = proposed_node_id
        self._jupyter_message_id: str = jupyter_message_id

    @property
    def jupyter_message_id(self):
        return self._jupyter_message_id

    def get_metadata(self) -> dict[str, any]:
        """
        Returns a dictionary containing this SynchronizedValue's fields, suitable for JSON serialization.
        """
        metadata: dict[str, any] = super().get_metadata()
        metadata["proposed_node_id"] = self._proposed_node_id
        metadata["jupyter_message_id"] = self._jupyter_message_id

        return metadata

    def __str__(self):
        return f"LeaderElectionVote[Proposer=Node{self.proposer_id},Proposed=Node{self.proposed_node_id},Timestamp={datetime.datetime.fromtimestamp(self.timestamp).strftime('%c')},ElectionTerm={self.election_term},AttemptNumber={self._attempt_number},JupyterMessageId={self._jupyter_message_id}]"

    @property
    def proposed_node_id(self) -> int:
        """
        The SMR node ID of the node being voted for
        """
        return self._proposed_node_id

    @property
    def election_succeeded(self) -> bool:
        """
        True if a particular node was selected/voted for (rather than all nodes proposing 'YIELD', which would constitute a failure/failed election.)
        Basically, True if at least one node proposed 'LEAD', and False if all nodes proposed 'YIELD'.
        """
        return self._proposed_node_id != -1

    @property
    def election_failed(self) -> bool:
        """
        True if all nodes proposed 'YIELD'.
        False if at least one node proposed 'LEAD'.
        """
        return self._proposed_node_id == -1


class BufferedLeaderElectionVote(object):
    """
    This simply wraps a LeaderElectionVote along with the timestamp at which the vote was originally received.
    The purpose is so we can pass the correct timestamp when we eventually process the buffered vote.
    """

    def __init__(self, vote: LeaderElectionVote, received_at: float = time.time(), jupyter_message_id:str = ""):
        self._vote: LeaderElectionVote = vote
        self._received_at: float = received_at
        self._jupyter_message_id = jupyter_message_id

    @property
    def jupyter_message_id(self):
        return self._jupyter_message_id

    @property
    def vote(self) -> LeaderElectionVote:
        return self._vote

    @property
    def received_at(self) -> float:
        return self._received_at


class SyncValue:
    """A value for log proposal."""

    def __init__(self, tag, val: Any, term: int = 0, proposed_node: Optional[int] = -1,
                 timestamp: Optional[float] = time.time(), key: Optional[str] = None, op: Optional[str] = None,
                 prmap: Optional[list[str]] = None, end: bool = False, attempt_number: int = -1):
        self.term: int = term
        self.key: str = key
        self.prmap = prmap
        self.tag = tag
        self.val = val
        self.attempt_number = attempt_number
        self.proposed_node = proposed_node  # Only used by 'SYNC' proposals to specify the node that should serve as the leader.
        self.end: bool = end
        self.op: str = op
        self.timestamp: float = timestamp  # The time at which the proposal/value was issued.

        self._reset: bool = False

    def __str__(self) -> str:
        ts: str = datetime.datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
        return "SyncValue[Key='%s',Term=%d,Timestamp='%s',Tag='%s',Val='%s']" % (
            self.key, self.term, ts, str(self.tag), str(self.val)[0:25])

    @property
    def reset(self):
        return self._reset


@runtime_checkable
class SyncLog(Protocol):
    @property
    def num_changes(self) -> int:  # type: ignore
        """The number of incremental changes since first set or the latest checkpoint."""

    @property
    def term(self) -> int:  # type: ignore
        """Current term."""

    @property
    def current_election(self)->Any:
        """
        :return: the current election, if one exists
        """

    @property
    def leader_id(self)->int:
        """
        Return the ID of the last/current leader node.
        """

    @property
    def leader_term(self)->int:
        """
        Return the term number of the last/current leader node.
        """

    async def remove_node(self, node_id):
        """Remove a node from the etcd-raft cluster."""
        pass

    def close_remote_storage_client(self) -> None:
        """
        Close the LogNode's RemoteStorage client.
        """

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

    async def update_node(self, node_id, address):
        """Add a node to the etcd-raft  cluster."""

    async def add_node(self, node_id, address):
        """
        Add a node to the etcd-raft cluster.

        NOTE: As of right now (5:39pm EST, Oct 11, 2024), this method is not actually used/called.

        Args:
            node_id: the ID of the node being added.
            address: the IP address of the node being added.
        """

    @property
    def created_first_election(self)->bool:
        """
        :return: return a boolean indicating whether we've created the first election yet.
        """

    def get_election(self, term_number: int, jupyter_msg_id: Optional[str] = None)->Any:
        """
        Returns the election with the specified term number, if one exists.

        If the term number is given as -1, then resolution via the JupyterMessageID is attempted.
        """

    def get_known_election_terms(self)->Optional[list[int]]:
        """
        :return: a list of term numbers for which we have an associated Election object
        """

    @property
    def needs_to_catch_up(self)->bool:
        """
        :return: true if we need to catch up because a migration just occurred
        """

    async def catchup_with_peers(self)->None:
        """
        Propose a new value and wait for it to be commited to know that we're "caught up".
        """
        pass

    @property
    def restoration_time_seconds(self)->float:
        """
        Return the time spent on restoring previous state from remote storage.
        """

    def clear_restoration_time(self):
        """
        Clear the 'restoration_time_seconds' metric.
        """

    @property
    def restore_namespace_time_seconds(self)->float:
        """
        Return the time spent restoring the user namespace.
        """

    def clear_restore_namespace_time_seconds(self):
        """
        Clear the 'restore_namespace_time_seconds' metric.
        """

    def start(self, handler):
        """Register change handler, restore internal states, and start monitoring changes.
          handler will be in the form listener(key, val: SyncValue)"""

    def set_should_checkpoint_callback(self, callback):
        """Set the callback that will be called when the SyncLog decides if to checkpoint or not.
          callback will be in the form callback(SyncLog) bool"""

    def set_checkpoint_callback(self, callback):
        """Set the callback that will be called when the SyncLog decides to checkpoint.
          callback will be in the form callback(Checkpointer)."""

    async def try_yield_execution(self, jupyter_message_id: str, term_number: int, target_replica_id: int = -1) -> bool:
        """Request yield the update of a term to another replica."""

    async def try_lead_execution(self, jupyter_message_id: str, term_number: int, target_replica_id: int = -1) -> bool:
        """Request to lead the update of a term. A following append call
           without leading status will fail."""

    async def wait_for_election_to_end(self, term_number: int):
        """
        Wait until the leader of the specified election finishes executing the code,
        or until we know that all replicas yielded.

        :param term_number: the term number of the election
        """

    async def does_election_already_exist(self, jupyter_msg_id:str)->bool:
        """
        Check if an election for the given Jupyter msg ID already exists.

        The Jupyter msg id would come from an "execute_request" or a
        "yield_request" message.
        """

    async def is_election_voting_complete(self, jupyter_msg_id:str)->bool:
        """
        Check if an election for the given Jupyter msg ID already exists.

        If so, return True if the voting phase of the election is complete.

        The Jupyter msg id would come from an "execute_request" or a
        "yield_request" message.
        """

    async def is_election_execution_complete(self, jupyter_msg_id:str)->bool:
        """
        Check if an election for the given Jupyter msg ID already exists.

        If so, return True if the execution phase election is complete.

        The Jupyter msg id would come from an "execute_request" or a
        "yield_request" message.
        """

    async def notify_execution_complete(self, term_number: int):
        """
        Notify our peer replicas that we have finished executing the code for the specified election.

        :param term_number: the term of the election for which we served as leader and executed
        the user-submitted code.
        """

    async def append(self, val: SynchronizedValue):
        """Append the difference of the value of specified key to the synchronization queue."""

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
