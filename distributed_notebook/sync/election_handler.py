import asyncio
import logging
import sys
import threading
import time
from collections import OrderedDict
from typing import Callable, Optional, Dict, Any, List, Tuple, Awaitable

from distributed_notebook.kernel.iopub_notifier import IOPubNotification
from distributed_notebook.logs import ColoredLogFormatter
from distributed_notebook.sync.election import Election
from distributed_notebook.sync.errors import GoNilError
from distributed_notebook.sync.log import LeaderElectionVote, LeaderElectionProposal, ExecutionCompleteNotification, \
    ElectionProposalKey, SynchronizedValue


def flush_streams():
    sys.stderr.flush()
    sys.stdout.flush()


class ElectionHandler(object):
    def __init__(
            self,
            kernel_id: str = None,
            node_id: int = 1,
            num_replicas: int = 3,
            election_timeout_sec: float = 60.0,
            io_loop: Optional[asyncio.AbstractEventLoop] = None,
            send_iopub_notification: Callable[[IOPubNotification, Optional[Dict[str, Any]]], None] = None,
            serialize_and_append_callback: Callable[[SynchronizedValue], Awaitable[None]] = None,
    ):
        assert kernel_id is not None
        assert node_id is not None and node_id >= 1
        assert serialize_and_append_callback is not None

        self.log: logging.Logger = logging.getLogger(__class__.__name__ + str(node_id))
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        self._leader_term: int = 0
        self._leader_id: int = 0
        self._node_id: int = node_id
        self._kernel_id: str = kernel_id
        self._num_replicas: int = num_replicas
        self._election_timeout_sec: float = election_timeout_sec
        self._serialize_and_append_callback: Callable[
            [SynchronizedValue], Awaitable[None]] = serialize_and_append_callback
        self._send_iopub_notification: Callable[
            [IOPubNotification, Optional[Dict[str, Any]]], None] = send_iopub_notification

        if io_loop is None:
            self._io_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        else:
            self._io_loop: asyncio.AbstractEventLoop = io_loop

        self._term_to_jupyter_id: Dict[int, str] = {}
        self._jupyter_id_to_term: Dict[str, int] = {}
        self._term_lock: threading.Lock = threading.Lock()

        self._needs_to_catch_up: bool = False

        self._proposed_values: OrderedDict[int, OrderedDict[int, LeaderElectionProposal]] = OrderedDict()
        """ Mapping from term number -> Dict. The inner map is attempt number -> proposal. """

        self._elections: Dict[int, Election] = {}
        self._elections_lock: threading.Lock = threading.Lock()

        self._buffered_proposals: dict[int, List[LeaderElectionProposal]] = {}
        """
        If we receive a proposal with a larger term number than our current election, then it is possible
        that we simply received the proposal before receiving the associated "execute_request" or "yield_request" message
        that would've prompted us to start the election locally. So, we'll just buffer the proposal for now, and when
        we receive the "execute_request" or "yield_request" message, we'll process any buffered proposals at that point.
        
        This map maintains the buffered proposals. The mapping is from term number to a list of buffered proposals for that term.        
        """

        self._buffered_proposals_lock: threading.Lock = threading.Lock()
        """
        Ensures atomic access to the _buffered_proposals dictionary (required because we may be switching between
        multiple Python threads/goroutines that are accessing the _buffered_proposals dictionary).
        """

    @property
    def needs_to_catch_up(self) -> bool:
        return self._needs_to_catch_up

    @needs_to_catch_up.setter
    def needs_to_catch_up(self, value: bool) -> None:
        self._needs_to_catch_up = value

    async def handle_election(
            self,
            key: ElectionProposalKey,
            jupyter_message_id: str,
            term_number: int,
            target_replica_id: int = -1,
    ) -> bool:
        """
        Request to serve as the leader for the update of a term (and therefore to be the
        replica to execute user-submitted code).

        A subsequent call to append (without successfully being elected as leader) will fail.

        :param key: indicates whether this replica is YIELD-ing or attempting to LEAD.
        :param jupyter_message_id: the msg_id of the associated Jupyter "execute_request" or "yield_request" message.
        :param term_number: the election/execution term number.
        :param target_replica_id: the SMR node ID of the replica pre-specified by the scheduler to become the executor.

        :return: True if this replica won the election and is now the Executor replica; otherwise, False.
        """
        self.log.debug(f'RaftLog {self._node_id}: handle election {term_number}: "{key.name}" '
                       f'[TargetReplicaId={target_replica_id}, JupyterMsgId="{jupyter_message_id}"]')

        # Sanity check.
        if target_replica_id >= 1 and target_replica_id != self._node_id:
            raise ValueError(f"Target replica ID specified as {target_replica_id} "
                             f"but we're still proposing 'LEAD' as node {self._node_id}.")

        # Create the value that we'll propose during the election.
        proposal_or_vote: LeaderElectionProposal | LeaderElectionVote = await self._create_election_proposal_or_vote(
            key=key,
            term_number=term_number,
            jupyter_message_id=jupyter_message_id,
            target_replica_id=target_replica_id
        )

        # Sanity check.
        self._validate_proposal(proposal_or_vote, term_number)

        # Get or create the election for the specified term number.
        with self._elections_lock:
            election: Election = self._get_or_create_election(
                term_number, jupyter_message_id, proposal_or_vote.attempt_number)

        if election is None:  # Sanity check.
            raise ValueError(f"Election {term_number} is unexpectedly null.")

        if election.is_inactive:
            election.start()

        # Do some additional sanity checks:
        # The proposal_or_vote must already be registered.
        # This means that there will be at least one proposal_or_vote for the specified
        # target term number (which matches the proposal_or_vote's term number; we
        # already checked verified that above).
        if isinstance(proposal_or_vote, LeaderElectionProposal):
            # At least one proposal_or_vote for the specified term?
            assert term_number in self._proposed_values

            # The proposal_or_vote is registered under its attempt number?
            assert proposal_or_vote.attempt_number in self._proposed_values[term_number]

            # Equality check for ultimate sanity check.
            assert self._proposed_values[term_number][proposal_or_vote.attempt_number] == proposal_or_vote

        # This is the future that we'll use to inform the local kernel replica if
        # it has been selected to "lead" the election (and therefore execute the user-submitted code).
        # Create local references.
        leading_future: Optional[asyncio.Future[int]] = election.leading_future
        if leading_future is None:
            self.log.warning(f'"Leading" future for election {election.term_number} is already None. '
                             f'Election state: "{election.state.name}"')

            assert election.voting_phase_completed_successfully

            return self._node_id == election.winner_id

        # If skip_proposals is True, then we'll skip both any buffered proposals, and we'll just elect not to
        # propose something ourselves. skip_proposals is set to True if we have a buffered vote that decides
        # the election for us.
        skip_proposals: bool = False

        num_buffered_votes_processed: int = 0

        self.log.debug(f"There are {len(election.buffered_proposals)} buffered proposal_or_vote(s) and "
                       f"{len(election.buffered_votes)} buffered vote(s) for election {term_number}.")

        if len(election.buffered_votes) > 0:
            skip_proposals, num_buffered_votes_processed = await self._process_buffered_votes(election)

        if skip_proposals:
            self.log.debug(f"Skipping the {len(election.buffered_proposals)} buffered proposal(s) as well as our "
                           f"own proposal for election {term_number}.")

            return await self._wait_for_election_to_end(election, leading_future)

        done, is_leading = await self._process_proposals(
            election,
            num_buffered_votes_processed,
            proposal_or_vote,
            target_replica_id
        )

        if done:
            self.log.debug(f"Finished handling election {term_number} while processing "
                           f"{len(election.buffered_proposals)} buffered proposal(s). is_leading={is_leading}")
            return is_leading

        self.log.debug(f"Not yet finished handling election {term_number} after processing "
                       f"{len(election.buffered_proposals)} buffered proposal(s). is_leading={is_leading}")

        return await self._wait_for_election_to_end(election, leading_future)

    async def _wait_for_election_to_end(self, election: Election, leading_future: asyncio.Future[int]):
        # Validate the term
        wait, is_leading = self._is_leading(election.term_number)
        if not wait:
            self.log.debug(f"RaftLog {self._node_id}: returning for term {election.term_number} "
                           f"without waiting, is_leading={is_leading}")
            return is_leading

        # Wait for the future to be set.
        self.log.debug("ElectionHandler::handle_election: Waiting on _leading_future Future to be resolved.")
        await leading_future
        self.log.debug("ElectionHandler::handle_election: Successfully waited for resolution of _leading_future.")

        # Validate the term
        wait, is_leading = self._is_leading(election.term_number)
        assert wait == False
        return is_leading

    def _is_leading(self, term: int) -> Tuple[bool, bool]:
        """Check if the current node is leading, return (wait, is_leading)"""
        if self._leader_term > term:
            return False, False
        elif self._leader_term == term:
            return False, self._leader_id == self._node_id
        else:
            return True, False

    async def _process_buffered_proposals(self, election: Election) -> int:
        """
        Process any buffered proposals for the specified Election.

        :returns: the number of buffered proposals that were processed.
        """
        if len(election.buffered_proposals) == 0:
            return 0

        self.log.debug(f"Processing {len(election.buffered_proposals)} "
                       f"buffered proposal(s) for election {election.term_number}.")

        num_buffered_proposals_processed: int = 0

        for i, buffered_proposal in enumerate(election.buffered_proposals):
            self.log.debug(f"Handling buffered proposal {i + 1}/{len(election.buffered_proposals)} "
                           f"during election term {election.term_number}: {buffered_proposal}")

            self._handle_proposal(election, buffered_proposal.proposal, received_at=buffered_proposal.received_at)

            self.log.debug(f"Handled buffered proposal {i + 1}/{len(election.buffered_proposals)} "
                           f"during election term {election.term_number}.")
            num_buffered_proposals_processed += 1

        return num_buffered_proposals_processed

    async def _process_proposals(
            self,
            election: Election,
            num_buffered_votes_processed: int,
            proposalOrVote: LeaderElectionProposal | LeaderElectionVote,
            target_replica_id: int = -1,
    ) -> Tuple[bool, bool]:
        """
        Process any buffered proposals, and propose our own value if necessary.

        :param election: the target election.
        :param num_buffered_votes_processed: the number of buffered votes that were processed.
        :param proposalOrVote: the proposal or vote that we will propose.
        :param target_replica_id: the ID of the target replica, as specified by the kernel scheduler(s).

        :return: a tuple where 1st element indicates if we're done processing the election, and 2nd is result if so.

        :raises ValueError: if the specified Election is None.
        """
        if election is None:
            raise ValueError("Election cannot be None.")

        num_buffered_proposals_processed: int = await self._process_buffered_proposals(election)

        if isinstance(proposalOrVote, LeaderElectionProposal):
            isDone, isLeading, voteProposal = await self._propose_election_proposal(
                proposalOrVote, election.term_number,
                num_buffered_proposals_processed=num_buffered_proposals_processed,
                num_buffered_votes_processed=num_buffered_votes_processed)

            if voteProposal is None or isDone:
                return isDone, isLeading

            assert isinstance(voteProposal, LeaderElectionVote)
        else:
            assert isinstance(proposalOrVote, LeaderElectionVote)
            voteProposal: LeaderElectionVote = proposalOrVote

        # Validate that the term number matches the current election.
        if voteProposal.election_term != election.term_number:
            raise ValueError(f"Received LeaderElectionVote with mis-matched term number ({voteProposal.election_term}) "
                             f"compared to current election term number ({election.term_number})")

        # Are we proposing that the election failed?
        if voteProposal.election_failed:
            self.log.debug(f"RaftLog {self._node_id}: Got decision to propose: election failed. "
                           f"No replicas proposed 'LEAD'.")

            with self._elections_lock:
                election.set_election_failed()

            # None of the replicas proposed 'LEAD'
            # It is likely that a migration of some sort will be triggered as a result, leading to another election round for this term.
            return True, False

        self.log.debug(f"RaftLog {self._node_id}: Appending vote proposal "
                       f"for term {voteProposal.election_term} now.")

        await self._append_election_vote(voteProposal)

        self.log.debug(f"RaftLog {self._node_id}: Successfully appended vote "
                       f"proposal for term {voteProposal.election_term} now.")

        return False, False

    async def _propose_election_proposal(
            self,
            proposal: LeaderElectionProposal,
            election_term: int,
            num_buffered_proposals_processed: int = 0,
            num_buffered_votes_processed: int = 0,
    ) -> Tuple[bool, bool, Optional[LeaderElectionVote]]:
        """

        :param proposal:
        :param election_term:
        :param num_buffered_proposals_processed:
        :param num_buffered_votes_processed:
        :return:
        """
        # TODO: Implement me.
        raise NotImplementedError("_propose_election_proposal has not been implemented yet")

    def _handle_proposal(
            self,
            election: Election,
            proposal: LeaderElectionProposal,
            received_at: float = time.time(),
    ) -> bytes:
        """
        Handle a committed LEAD/YIELD proposal.

        Args
            election: The Election for which we're handling the proposal.
            proposal (LeaderElectionProposal): the committed proposal.
            received_at (float): the time at which we received this proposal.
        """
        if election is None:
            raise ValueError("Election cannot be None")

        if proposal is None:
            raise ValueError("LeaderElectionProposal cannot be None")

        self.log.debug(f'Received "{proposal.key}" proposal from node {proposal.proposer_id} '
                       f'for election {election.term_number}.')

        if self.needs_to_catch_up:
            self.log.debug(f"Buffering proposal received for election {election.term_number} "
                           f"while catching up: {proposal}")

            return self._buffer_proposal(proposal=proposal, received_at=received_at, election=election)

        with self._elections_lock:
            if not election.is_active:
                self.log.debug(f"Buffering proposal for inactive election {election.term_number}: {proposal}")

                return self._buffer_proposal(proposal=proposal, received_at=received_at, election=election)

            # val will only be non-None if this is the first LEAD proposal we're receiving for this election term.
            was_first: bool = election.add_proposal(proposal, self._io_loop, received_at=received_at)

        if not was_first:
            fut: Optional[asyncio.Future[Any]] = asyncio.run_coroutine_threadsafe(
                election.try_decide_election(last_winner_id=0), loop=self._io_loop)
            fut.result()

        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError()

    async def _process_buffered_votes(self, election: Election) -> Tuple[bool, int]:
        """
        :param election: the term of the election for which the buffered votes are to be processed.

        :return: a tuple in which the first element is a boolean indicating whether proposals should be processed,
                 and the second element is an integer encoding the number of buffered votes that were processed.

        :raises ValueError: if the election argument is None.
        """
        if election is None:
            raise ValueError("Received null election.")

        self.log.debug(f"ElectionHandler::_process_buffered_votes: processing {len(election.buffered_votes)} "
                       f"buffered vote(s) for election {election.term_number}.")

        if len(election.buffered_votes) == 0:
            return False, 0

        skip_proposals: bool = False
        num_buffered_votes_processed: int = 0

        for i, buffered_vote in enumerate(election.buffered_votes):
            self.log.debug(f"ElectionHandler::_process_buffered_votes: processing vote "
                           f"{i + 1}/{len(election.buffered_votes)} for election {election.term_number}: {buffered_vote}")

            self._handle_vote(
                election=election,
                vote=buffered_vote.vote,
                received_at=buffered_vote.received_at,
                buffered=True
            )

            self.log.debug(f"ElectionHandler::_process_buffered_votes: handled buffered vote "
                           f"{i + 1}/{len(election.buffered_votes)} election {election.term_number}.")

            num_buffered_votes_processed += 1

            if election.voting_phase_completed_successfully:
                self.log.debug(f"ElectionHandler::_process_buffered_votes: "
                               f"voting phase completed for election ({election.term_number}).")
                skip_proposals = True
                break

        self.log.debug(f"Finished processing buffered votes for election {election.term_number}. "
                       f"Processed {num_buffered_votes_processed}/{len(election.buffered_votes)} buffered vote(s).")

        return skip_proposals, num_buffered_votes_processed

    def _handle_vote(
            self,
            election: Election,
            vote: LeaderElectionVote,
            received_at: float = time.time(),
            buffered: bool = False
    ) -> bytes:
        """
        Process a LeaderElectionVote that was appended to the RaftLog.

        :param vote: the vote that was appended to the RaftLog.
        :param received_at: the time at which we received the LeaderElectionVote.
        :param buffered: indicates whether the specified LeaderElectionVote was buffered.

        :raises ValueError: if the term number of the specified Election does not match
                            the election term  field of the specified LeaderElectionVote.
        """
        self.log.debug(f"Handling committed LeaderElectionVote: {vote}, buffered={buffered}")

        if election.term_number != vote.election_term:
            raise ValueError(f"Inconsistent term number between election ({election.term_number}) "
                             f"and LeaderElectionVote({vote.election_term}).")

        if self.needs_to_catch_up:
            return self._handle_vote_while_catching_up(election=election, vote=vote, received_at=received_at)

        was_first_vote_proposal: bool = election.add_vote_proposal(vote, overwrite=True, received_at=received_at)

        if not was_first_vote_proposal:
            self.log.debug(f"Discarding vote for node {vote.proposed_node_id} from node {vote.proposer_id} "
                           f"during term {election.term_number}, as it is not the first committed vote.")

        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError()

    def _handle_vote_while_catching_up(
            self,
            election: Election,
            vote: LeaderElectionVote,
            received_at: float = time.time(),
    ) -> bytes:
        """
        Handle a vote received while catching up.

        :param election: the associated Election.
        :param vote: the vote that was received.
        :param received_at: the time at which the vote was received.

        :raises ValueError: if the term number of the specified Election does not match the election term field of
                            the specified LeaderElectionVote.
        """
        self.log.debug(f"Vote for election {election.term_number} was received while catching up: {vote}")

        if election.term_number != vote.election_term:
            raise ValueError(f"Inconsistent term number between election ({election.term_number}) "
                             f"and LeaderElectionVote({vote.election_term}).")

        self._buffer_vote(vote=vote, election=election, received_at=received_at)

        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError()

    def _validate_proposal(
            self,
            proposal_or_vote: LeaderElectionProposal | LeaderElectionVote,
            target_term_number: int
    ):
        """
        Validate that the specified proposal or vote is either a LeaderElectionProposal object or a
        LeaderElectionVote object.

        If it isn't, then this will raise a ValueError.

        :param proposal_or_vote: the proposal/vote.
        :param target_term_number: the election term.

        :raises ValueError: if the proposal_or_vote argument is None, or if proposal_or_vote is neither a
        LeaderElectionProposal object nor a LeaderElectionVote object.
        """
        if proposal_or_vote is None:
            raise ValueError(f"Received null proposal/vote object for election term {target_term_number}.")

        if isinstance(proposal_or_vote, LeaderElectionVote):
            self.log.debug(f"RaftLog {self._node_id}: short-circuiting election {target_term_number} with vote for "
                           f"node {proposal_or_vote.proposed_node_id} [AttemptNum={proposal_or_vote.attempt_number}]")
            return

        if not isinstance(proposal_or_vote, LeaderElectionProposal):
            raise ValueError(
                f"Illegal type of proposal/vote passed to 'handle election': {type(proposal_or_vote).__name__}")

    def _received_vote(self, vote: LeaderElectionVote, received_at: float = time.time()) -> bytes:
        self.log.debug(f"Received Vote: {vote}")

        with self._elections_lock:
            election: Optional[Election] = self._get_or_create_election(
                vote.election_term, vote.jupyter_message_id, vote.attempt_number)

            if not election.has_been_started:
                self._buffer_vote(vote)

                sys.stderr.flush()
                sys.stdout.flush()
                return GoNilError()

        was_first_vote_proposal: bool = election.add_vote_proposal(vote, overwrite=True, received_at=received_at)

        if not was_first_vote_proposal or election.voting_phase_completed_successfully:
            self.log.debug(f"Ignoring vote [term={vote.election_term}, proposer={vote.proposer_id}, "
                           f"proposed={vote.proposed_node_id}, jupyterId={vote.jupyter_message_id}].")

        sys.stderr.flush()
        sys.stdout.flush()
        return GoNilError()

    def _received_notification(self, notification: ExecutionCompleteNotification) -> bytes:
        self.log.debug(f"Received Notification: {notification}")

        with self._elections_lock:
            election: Optional[Election] = self._get_or_create_election(
                notification.election_term, notification.jupyter_message_id, notification.attempt_number)

            self._complete_election(election, notification)

        return GoNilError()

    async def _create_election_proposal_or_vote(
            self,
            key: ElectionProposalKey,
            term_number: int,
            jupyter_message_id: str,
            target_replica_id: int = -1,
    ) -> LeaderElectionProposal | LeaderElectionVote:
        """
        Create and register a proposal for the current term.

        This updates the `self._proposed_values` field.

        The attempt number for the new proposal is "calculated" based on whether there
        already exists a previous proposal for this election term.

        :param key: the key to propose (YIELD, LEAD, VOTE, etc.)
        :param term_number: the election/execution term number
        :param jupyter_message_id: the msg_id of the associated Jupyter "execute_request" or "yield_request" message.
        :param target_replica_id: the SMR node ID of the replica pre-specified by the scheduler to become the executor.

        :return: the newly-created LeaderElectionProposal or LeaderElectionVote
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
        # if target_replica_id >= 1:
        #     vote: LeaderElectionVote = LeaderElectionVote(
        #         proposed_node_id=target_replica_id,
        #         jupyter_message_id=jupyter_message_id,
        #         proposer_id=self._node_id,
        #         election_term=term_number,
        #         attempt_number=attempt_num,
        #     )
        #     return vote

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

    def _complete_election(self, election: Election, notification: ExecutionCompleteNotification) -> None:
        """
        Record that the specified Election has completed, as informed by the specified ExecutionCompleteNotification.

        IMPORTANT: This method must be called with the elections lock already held.
        """
        if election.term_number != notification.election_term:
            raise ValueError(f"Term mismatch between Election and ExecutionCompleteNotification")

        if not election.has_been_started:
            self.log.warning(f"Election {election.term_number} has finished before starting.")
        else:
            self.log.debug(f"Election {election.term_number} has finished.")

        # First, check if we know that the voting phase has completed.
        # If not, then we'll update that first.
        if not election.voting_phase_completed_successfully:
            election.set_election_vote_completed(notification.proposer_id)

        # Now, check if we know that the code execution completed successfully.
        # If we know about it already, then we'll just return.
        if election.code_execution_completed_successfully:
            self.log.debug(f"Discarding ExecutionCompleteNotification [term={notification.election_term}, "
                           f"attemptNumber={notification.attempt_number}, notification={notification}]")
            return

        # Record that the code execution phase completed successfully.
        election.set_execution_complete(
            catching_up=True,
            fast_forwarding=False,
            fast_forwarded_winner_id=notification.proposer_id
        )

    def _get_or_create_election(self, election_term: int, jupyter_message_id: str, attempt_number: int) -> Election:
        """
        Return the existing Election object for the specified term number, if one exists.

        If no Election exists for the specified term number, then create and return a new Election instance.

        IMPORTANT: This method must be called with the elections lock already held.

        :param election_term: the target term number.
        :param jupyter_message_id: the associated Jupyter message ID.
        :param attempt_number: the expected attempt number of the election to be returned or created.

        :return: the Election associated with the specified term number, which may or may not have already existed.

        :raises ValueError: if election_term < 0 or attempt_number <= 0 or jupyter_message_id is None
                            or len(jupyter_message_id) == 0.
        """
        if election_term < 0:
            self.log.error(f"ElectionHandler::_get_or_create_election: invalid term number: {election_term}")
            raise ValueError(f"Invalid election term number: {election_term}")

        if attempt_number <= 0:
            self.log.error(f"ElectionHandler::_get_or_create_election: invalid attempt number: {attempt_number}")
            raise ValueError(f"Invalid attempt number: {attempt_number}")

        if jupyter_message_id is None or len(jupyter_message_id) == 0:
            self.log.error(f"ElectionHandler::_get_or_create_election: "
                           f"invalid Jupyter message ID: {jupyter_message_id}")

            raise ValueError(f'Invalid Jupyter message ID: "{jupyter_message_id}"')

        if election_term in self._elections:
            election: Election = self._elections[election_term]
            self._validate_or_restart_election(election)
            return election

        election: Election = Election(
            term_number=election_term,
            num_replicas=self._num_replicas,
            jupyter_message_id=jupyter_message_id,
            timeout_seconds=self._election_timeout_sec,
            local_node_id=self._node_id,
            io_loop=self._io_loop,
        )

        self._update_term_jupyter_id_mapping(election_term, jupyter_message_id)

        self._elections[election_term] = election

        return election

    def _validate_or_restart_election(
            self,
            election: Election,
            jupyter_message_id: str = "",
            expected_attempt_number: int = -1,
    ) -> None:
        """
        Validate the state of the current active election. This should be called by the handle_election method.
        We make sure that the term number and Jupyter message IDs are consistent with the proposal we just received.

        If the local election is in the 'failed' state, then we restart it.

        Args:
            election: the election to be validated and possibly restarted.
            jupyter_message_id: the expected jupyter message ID of the current election
            expected_attempt_number: the expected attempt number of the current election

        :raises ValueError: if election is None or expected_attempt_number <= 0 or jupyter_message_id is None
                            or len(jupyter_message_id) == 0.
        """
        if election is None:
            self.log.error("ElectionHandler::_validate_or_restart_election: election argument is None.")
            raise ValueError("Election is null.")

        if expected_attempt_number <= 0:
            self.log.error(f"ElectionHandler::_validate_or_restart_election: "
                           f"invalid attempt number: {expected_attempt_number}")

            raise ValueError(f"Invalid attempt number: {expected_attempt_number}")

        if jupyter_message_id is None or len(jupyter_message_id) == 0:
            self.log.error(f"ElectionHandler::_validate_or_restart_election: "
                           f"invalid Jupyter message ID: {jupyter_message_id}")

        # If the Jupyter message IDs do not match, then that is problematic.
        if election.jupyter_message_id != jupyter_message_id:
            raise ValueError(f"Attempting to get or retrieve election for term {election.term_number} with "
                             f'JupyterMessageID={election.jupyter_message_id}, which does not match the specified '
                             f"JupyterMessageID {election.jupyter_message_id}.")

        if not election.is_active:
            assert election.is_in_failed_state
            self.log.debug(f"Restarting existing election {election.term_number}. "
                           f"Current state: {election.election_state.get_name()}.")
            election.restart(latest_attempt_number=expected_attempt_number)
            return

        # If we have an election with the same term number, then there may have just been some delay in us receiving
        # the 'execute_request' (or 'yield_request') ZMQ message.
        #
        # During this delay, we may have received a committed proposal from another replica for this election,
        # which prompted us to either create or restart the election at that point.
        #
        # So, if we have a current election already, and that election is in a non-active state, then we restart it.
        # If we have a current election that is already active, then we should have at least one proposal already
        # (otherwise, why would the election be active already?)
        self.log.debug(f"Reusing active election {election.term_number} with "
                       f"{election.num_proposals_received} received proposal(s).")

        # Sanity check.
        # If the election is already active, then we necessarily should have received a proposal from a peer, which
        # triggered either the creation of this election, or the restarting of the election if it had already
        # existed and was in the failed state.
        if election.num_proposals_received == 0:
            self.log.error(f"ElectionHandler::_validate_or_restart_election: Existing election for term "
                           f"{election.term_number} is already active; however, it has no registered proposals, "
                           f"so it should not be active already")

            raise ValueError(f"Attempted reuse of existing election {election.term_number} "
                             f"with no registered proposals.")

    def _update_term_jupyter_id_mapping(self, term_number: int, jupyter_msg_id: str) -> None:
        """
        Update the mapping between term numbers and Jupyter message IDs.
        """
        with self._term_lock:
            if term_number not in self._term_to_jupyter_id:
                self._term_to_jupyter_id[term_number] = jupyter_msg_id

            if jupyter_msg_id not in self._jupyter_id_to_term:
                self._jupyter_id_to_term[jupyter_msg_id] = term_number

    def _buffer_vote(
            self,
            vote: LeaderElectionVote,
            received_at: float = time.time(),
            election: Optional[Election] = None,
    ) -> bytes:
        """
        Buffer a LeaderElectionVote received before the associated Election was started.
        """
        self.log.debug(f'Buffering vote. Term={vote.election_term}. Proposer={vote.proposer_id}. '
                       f'Target={vote.proposed_node_id}. JupyterId="{vote.jupyter_message_id}".')

        if election is None:
            with self._election_timeout_sec:
                election: Election = self._get_or_create_election(
                    vote.election_term, vote.jupyter_message_id, vote.attempt_number)

        election.buffer_vote(vote=vote, received_at=received_at)

        return GoNilError()

    def _buffer_proposal(
            self,
            proposal: LeaderElectionProposal,
            received_at: float = time.time(),
            election: Optional[Election] = None,
    ) -> bytes:
        """
        Buffer a LeaderElectionProposal received before the associated Election was started.
        """
        self.log.debug(f'Buffering proposal. Term={proposal.election_term}. Proposer={proposal.proposer_id}. '
                       f'Key="{proposal.key}". JupyterId="{proposal.jupyter_message_id}".')

        if election is None:
            with self._election_timeout_sec:
                election: Election = self._get_or_create_election(
                    proposal.election_term, proposal.jupyter_message_id, proposal.attempt_number)

        election.buffer_proposal(proposal=proposal, received_at=received_at)

        return GoNilError()

    async def _append_election_vote(self, vote: LeaderElectionVote):
        """
        Explicitly propose and append (to the synchronized Raft log) a vote for the winner of the current election.

        This function exists so that we can mock proposals of LeaderElectionProposal objects specifically,
        rather than mocking the more generic _serialize_and_append_value method.
        """
        self.log.debug(f"Serializing and appending election vote: {vote}")
        await self._serialize_and_append_callback(vote)
