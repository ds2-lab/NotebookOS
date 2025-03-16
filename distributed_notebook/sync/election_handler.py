import logging
import time
import threading
import asyncio
import sys
import typing

from collections import OrderedDict

from typing import Callable, Optional, Dict, Any

from distributed_notebook.kernel.iopub_notifier import IOPubNotification
from distributed_notebook.logs import ColoredLogFormatter

from distributed_notebook.sync.election import Election, ElectionAlreadyDecidedError, ElectionNotStartedError
from distributed_notebook.sync.errors import GoNilError
from distributed_notebook.sync.log import LeaderElectionVote, LeaderElectionProposal, ExecutionCompleteNotification, \
    BufferedLeaderElectionProposal, BufferedLeaderElectionVote

def flush_streams():
    sys.stderr.flush()
    sys.stdout.flush()

class ElectionHandler(object):
    def __init__(
            self,
            kernel_id: str = None,
            node_id: int = 1,
            send_iopub_notification: Callable[[IOPubNotification, typing.Optional[typing.Dict[str, typing.Any]]], None] = None,
    ):
        assert kernel_id is not None
        assert node_id is not None and node_id >= 1

        self.log: logging.Logger = logging.getLogger(__class__.__name__ + str(node_id))
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        self._node_id: int = node_id
        self._kernel_id: str = kernel_id

        self._term_to_jupyter_id: typing.Dict[int, str] = {}
        self._jupyter_id_to_term: typing.Dict[str, int] = {}

        self.elections: typing.Dict[int, Election] = {}
        self.elections_lock: threading.Lock = threading.Lock()

        self.latest_election_term: int = -1

        self._leader_term_before_migration: int = -1

        self.decide_election_future: typing.Optional[asyncio.Future] = None

        self._last_winner_id: int = -1

        self._send_iopub_notification: typing.Callable[
            [IOPubNotification, typing.Optional[typing.Dict[str, typing.Any]]], None] = send_iopub_notification

        # Future that is resolved when we propose that somebody win the current election.
        # This future returns the `LeaderElectionVote` that we will propose to nominate/synchronize
        #  the winner of the election with our peers.
        self._election_decision_future: typing.Optional[asyncio.Future[LeaderElectionVote]] = None

        # The IO loop on which the `_election_decision_future` is/was created.
        self._future_io_loop: typing.Optional[asyncio.AbstractEventLoop] = None

        # The _fallback_future_io_loop is used if self._future_io_loop is None when we receive a proposal.
        # This can occur if we receive a proposal from a peer before receiving the "execute_request" message
        # (or equivalently the "yield_request" message) from our Local Daemon.
        #
        # The _fallback_future_io_loop is set to the Shell handler's IO loop, and should be set shortly after the
        # kernel is created.
        self._fallback_future_io_loop: typing.Optional[asyncio.AbstractEventLoop] = None

        # If we receive a proposal with a larger term number than our current election, then it is possible
        # that we simply received the proposal before receiving the associated "execute_request" or "yield_request" message
        # that would've prompted us to start the election locally. So, we'll just buffer the proposal for now, and when
        # we receive the "execute_request" or "yield_request" message, we'll process any buffered proposals at that point.
        #
        # This map maintains the buffered proposals. The mapping is from term number to a list of buffered proposals for that term.
        self._buffered_proposals: dict[int, typing.List[BufferedLeaderElectionProposal]] = {}

        # Ensures atomic access to the _buffered_proposals dictionary (required because we may be switching between
        # multiple Python threads/goroutines that are accessing the _buffered_proposals dictionary).
        self._buffered_proposals_lock: threading.Lock = threading.Lock()

        # _buffered_votes serves the same purpose as _buffered_proposals, but _buffered_votes is for LeaderElectionVote
        # objects, whereas _buffered_proposals is for LeaderElectionProposal objects.
        self._buffered_votes: dict[int, typing.List[BufferedLeaderElectionVote]] = {}

        # Ensures atomic access to the _buffered_votes dictionary (required because we may be switching between
        # multiple Python threads/goroutines that are accessing the _buffered_votes dictionary).
        self._buffered_votes_lock: threading.Lock = threading.Lock()

        # Mapping from term number -> Dict. The inner map is attempt number -> proposal.
        self._proposed_values: OrderedDict[int, OrderedDict[int, LeaderElectionProposal]] = OrderedDict()

        self._is_catching_up: bool = False

    @property
    def is_catching_up(self) -> bool:
        return self._is_catching_up

    @is_catching_up.setter
    def is_catching_up(self, is_catching_up: bool):
        self._is_catching_up = is_catching_up

    @property
    def future_io_loop(self) -> typing.Optional[asyncio.AbstractEventLoop]:
        return self._future_io_loop

    @property
    def fallback_future_io_loop(self) -> typing.Optional[asyncio.AbstractEventLoop]:
        return self._fallback_future_io_loop

    @fallback_future_io_loop.setter
    def fallback_future_io_loop(self, loop: typing.Optional[asyncio.AbstractEventLoop]):
        self._fallback_future_io_loop = loop

    def get_election(self, term_number: int) -> typing.Optional[Election]:
        with self.elections_lock:
            return self.elections.get(term_number, None)

    def has_election(self, term_number: int) -> bool:
        """
        Return True if there exists an election registered for the specified term number.
        """
        with self.elections_lock:
            return term_number in self.elections

    def buffer_vote(
            self, vote: LeaderElectionVote, received_at: float = time.time()
    ) -> bytes:
        if vote.jupyter_message_id not in self._jupyter_id_to_term:
            self._jupyter_id_to_term[vote.jupyter_message_id] = vote.election_term

        if vote.election_term not in self._term_to_jupyter_id:
            self._term_to_jupyter_id[vote.election_term] = vote.jupyter_message_id

        # Save the vote in the "buffered votes" dictionary.
        with self._buffered_votes_lock:
            buffered_votes: typing.List[BufferedLeaderElectionVote] = self._buffered_votes.get(vote.election_term, [])
            buffered_votes.append(BufferedLeaderElectionVote(vote=vote, received_at=received_at))
            self._buffered_votes[vote.election_term] = buffered_votes
            sys.stderr.flush()
            sys.stdout.flush()
            return GoNilError()

    def buffer_proposal(self, proposal: LeaderElectionProposal, received_at: float = time.time()) -> bytes:
        # Save the proposal in the "buffered proposals" mapping.
        with self._buffered_proposals_lock:
            buffered_proposals: typing.List[BufferedLeaderElectionProposal] = (
                self._buffered_proposals.get(proposal.election_term, [])
            )
            buffered_proposals.append(BufferedLeaderElectionProposal(proposal=proposal, received_at=received_at))
            self._buffered_proposals[proposal.election_term] = buffered_proposals
            flush_streams()
            return GoNilError()

    def update_state(self, committedValue: LeaderElectionVote | LeaderElectionProposal | ExecutionCompleteNotification):
        """
        Update state is called upon receiving any committed value (LeaderElectionVote, LeaderElectionProposal, or
        ExecutionCompleteNotification).
        """
        if committedValue.jupyter_message_id not in self._jupyter_id_to_term:
            self._jupyter_id_to_term[committedValue.jupyter_message_id] = committedValue.election_term

        if committedValue.election_term not in self._term_to_jupyter_id:
            self._term_to_jupyter_id[committedValue.election_term] = committedValue.jupyter_message_id

        if committedValue.election_term > self.latest_election_term:
            self.log.debug(f"largest election term: {self.latest_election_term} --> {committedValue.election_term}")

            self.latest_election_term = committedValue.election_term

    def received_vote(
            self,
            vote: LeaderElectionVote = None,
            buffered_vote: bool = True,
            received_at: float = -1,
    ) -> bytes:
        """
        Handle a vote proposal.

        :param vote: the vote proposal that we've received.
        :param received_at: the time at which we received the vote proposal.
        :param buffered_vote: if True, then we're handling a buffered vote proposal,
                              and thus we should not buffer it again.
        """
        assert vote is not None
        self.log.debug(f"RaftLog committed LeaderElectionVote: {vote} [buffered={buffered_vote}]")
        self.update_state(vote)

        if not self.has_election(vote.election_term):
            self.log.debug(f"received_vote: no election for term {vote.election_term}. "
                           f"buffering vote: {vote}.")

            return self.buffer_vote(vote, received_at = received_at)

        return GoNilError()

    def received_proposal_while_catching_up(
            self,
            proposal: LeaderElectionProposal = None,
            received_at: float = -1
    ) -> bytes:
        if proposal.election_term <= self._leader_term_before_migration:
            self.log.debug(f"Discarding old LeaderElectionProposal from term {proposal.election_term} "
                           f"with attempt number {proposal.attempt_number}, "
                           f"as we need to catch-up: {proposal}")
            flush_streams()
            return GoNilError()

        election: typing.Optional[Election] = self.get_election(proposal.election_term)
        if election is None or election.current_attempt_number < proposal.attempt_number:
            self.log.warning(f"Received proposal from term {proposal.election_term} "
                             f"(with attempt number {proposal.attempt_number})."
                             f"The proposal's term is > the election term prior to our migration "
                             f"(i.e., {self._leader_term_before_migration}). Buffering proposal now: {proposal}.")
            self.buffer_proposal(proposal, received_at=received_at)
            flush_streams()
            return GoNilError()

        self.log.debug(f"Discarding LeaderElectionProposal from term {proposal.election_term} "
                       f"with attempt number {proposal.attempt_number}, "
                       f"as we need to catch-up: {proposal}")
        flush_streams()
        return GoNilError()

    def received_proposal(self, proposal: LeaderElectionProposal = None, received_at: float = -1) -> bytes:
        assert proposal is not None
        self.log.debug(f"RaftLog committed LeaderElectionProposal: {proposal}")
        self.update_state(proposal)

        if not self.has_election(proposal.election_term):
            self.log.debug(f"received_proposal: no election for term {proposal.election_term}. "
                           f"buffering proposal: {proposal}.")

            return self.buffer_proposal(proposal, received_at = received_at)

        if self.is_catching_up:
            return self.received_proposal_while_catching_up(proposal, received_at = received_at)

        self.__set_future_io_loop(proposal.election_term)

        election: typing.Optional[Election] = self.get_election(term_number = proposal.election_term)
        if election is None:
            # TODO: Just create election here, I guess.
            pass

        with self.elections_lock:
            # val will only be non-None if this is the first LEAD proposal we're receiving for this election term.
            val: typing.Optional[tuple[asyncio.Future[typing.Any], float]] = election.add_proposal(
                proposal,
                self._future_io_loop,
                received_at=received_at,
            )

        if val is None:
            self.__try_pick_winner_to_propose(proposal.election_term, election)
            flush_streams()
            return GoNilError()

        if self._send_iopub_notification is not None:
            self._send_iopub_notification(
                IOPubNotification.ElectionFirstLeadProposalCommitted,
                {
                    "term_number": proposal.election_term,
                    "proposer_id": proposal.proposer_id,
                    "kernel_id": self._kernel_id,
                    "node_id": self._node_id
                }
            )

        # Future to decide the result of the election by a certain time limit.
        pick_and_propose_winner_future, discard_after = val

        self.__set_future_io_loop(proposal.election_term)

        # Schedule `decide_election` to be called.
        # It will sleep until the discardAt time expires, at which point a decision needs to be made.
        # If a decision was already made for that election, then the `decide_election` function will simply return.
        self.decide_election_future: asyncio.Future = (
            asyncio.run_coroutine_threadsafe(
                self.decide_election(election, discard_after, pick_and_propose_winner_future), self._future_io_loop)
        )

        self.log.debug(f"Received {election.num_proposals_received} proposal(s) "
                       f"and discarded {election.num_discarded_proposals} proposal(s) "
                       f"so far during term {election.term_number}.")

        self.__try_pick_winner_to_propose(proposal.election_term, election)

        flush_streams()
        return GoNilError()

    async def decide_election(
            self,
            election: Election,
            discard_after: float = 0,
            pick_and_propose_winner_future: asyncio.Future = None,
    ):
        if election is None:
            self.log.error("decide_election called, but current election is None...")
            raise ValueError("Current election is None in `decide_election` callback.")

        current_term: int = election.term_number

        sleep_duration: float = discard_after - time.time()
        assert sleep_duration > 0
        self.log.debug(f"decide_election called for election {current_term}. "
                       f"Sleeping for {sleep_duration} seconds in decide_election coroutine for election {current_term}.")
        await asyncio.sleep(sleep_duration)
        self.log.debug(f"Woke up in decide_election call for election {current_term}.")

        if pick_and_propose_winner_future.done():
            self.log.debug(f"Election {current_term} has already been decided; "
                           f"returning from decide_election coroutine now.")
            return

        if election.term_number != current_term:
            self.log.warning(f"Election term has changed in resolve(). "
                             f"Was {current_term}, is now {election.term_number}.")
            return

        try:
            picked_a_winner: bool = self.__try_pick_winner_to_propose(current_term, election)

            if not picked_a_winner:
                if election.is_active:
                    self.log.error(f"Could not select a winner for election term {current_term} "
                                   f"after timeout period elapsed...")
                    self.log.error(f"Received proposals: {election.proposals}")
                    # Note: the timeout period is not set until we receive our first lead proposal,
                    # so we should necessarily be able to select a winner
                    raise ValueError(f"Could not decide election term {current_term} "
                                     f"despite timeout period elapsing")
        except asyncio.InvalidStateError as ex:
            self.log.error(f"Future for picking and proposing a winner of election term {current_term} "
                           f"has already been resolved...: {ex}")

    def received_execution_complete_notification(self, notification: ExecutionCompleteNotification) -> bytes:
        assert notification is not None
        self.log.debug(f"RaftLog committed ExecutionCompleteNotification: {notification}")
        self.update_state(notification)

        return GoNilError()

    def __set_future_io_loop(self, term_number:int):
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

    def __try_pick_winner_to_propose(self, term_number: int, election: Election) -> bool:
        """
        Try to select a winner to propose for the current election.

        Returns:
            True if a winner was selected for proposal (including just proposing 'FAILURE' due to all nodes
            proposing 'YIELD'); otherwise, return False.
        """
        self.log.debug(f"Trying to pick winner for election {term_number}.")

        if election is None:
            raise ValueError(f"cannot try to pick winner for election {term_number}; "
                             f"current election field is null.")

        if election.voting_phase_completed_successfully:
            self.log.debug(f"Voting phase has already completed for election {term_number}.")
            return False

        self.__set_future_io_loop(term_number)

        try:
            # Select a winner.
            with self.elections_lock:
                id_of_winner_to_propose: int = election.pick_winner_to_propose(last_winner_id=self._last_winner_id)

            if id_of_winner_to_propose > 0:
                assert self._election_decision_future is not None
                self.log.debug(f"Will propose that node {id_of_winner_to_propose} "
                               f"win the election in term {election.term_number}.")
                self._future_io_loop.call_soon_threadsafe(
                    self._election_decision_future.set_result,
                    LeaderElectionVote(
                        proposed_node_id=id_of_winner_to_propose,
                        jupyter_message_id=election.jupyter_message_id,
                        proposer_id=self._node_id,
                        election_term=term_number,
                        attempt_number=election.current_attempt_number,
                    ),
                )
                return True
            else:
                assert self._election_decision_future is not None
                self.log.debug(f"Will propose 'FAILURE' for election in term {election.term_number}.")
                self._future_io_loop.call_soon_threadsafe(
                    self._election_decision_future.set_result,
                    LeaderElectionVote(
                        proposed_node_id=-1,
                        jupyter_message_id=election.jupyter_message_id,
                        proposer_id=self._node_id,
                        election_term=term_number,
                        attempt_number=election.current_attempt_number,
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
                           f"{election.term_number} because: {ex}")

        return False