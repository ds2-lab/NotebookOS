import asyncio
import logging 
import time 

from typing import Dict, Optional, List, Type, MutableMapping, Any
from enum import Enum

from .log import LeaderElectionVote, LeaderElectionProposal

class ElectionState(Enum):
    INACTIVE = 1    # Created, but not yet started.
    ACTIVE = 2      # Active, in progress
    COMPLETE = 3    # Finished successfully, as in some node proposed 'LEAD' and was elected.
    FAILED = 4      # Failed, as in all nodes proposed 'YIELD', and the election has not been restarted yet.

class Election(object):
    """
    Encapsulates the information about the current election term.
    """
    def __init__(
            self, 
            term_number: int,
            num_replicas: int,
    ):
        # The term number of the election.
        # Each election has a unique term number.
        # Term numbers monotonically increase over time. 
        self._term_number = term_number

        # The number of replicas in the SMR cluster, so we know how many proposals to expect.
        self._num_replicas = num_replicas

        # Mapping from SMR Node ID to the LeaderElectionProposal that it proposed during this election term.
        self._proposals: Dict[int, LeaderElectionProposal] = {} 
        # Mapping from SMR Node ID to the LeaderElectionVote that it proposed during this election term.
        self._vote_proposals: Dict[int, LeaderElectionVote] = {} 

        # Mapping from SMR node ID to an inner mapping.
        # The inner mapping is a mapping from attempt number to the associated LeaderElectionProposal proposed by that node (during that attempt) during this election term.
        self._discarded_proposals: Dict[int, Dict[int, LeaderElectionProposal]] = {}
        # Mapping from SMR node ID to an inner mapping.
        # The inner mapping is a mapping from attempt number to the associated LeaderElectionVote proposed by that node (during that attempt) during this election term.
        self._discarded_vote_proposals: Dict[int, Dict[int, LeaderElectionVote]] = {}

        # The current state/status of the election.
        self._election_state: ElectionState = ElectionState.INACTIVE

        # The first 'LEAD' proposal we received during this election.
        self._first_lead_proposal: Optional[LeaderElectionProposal] = None  

        # The number of proposals that have been discarded (due to being received after the timeout period).
        self._num_discarded_proposals: int = 0

        # The number of 'vote' proposals that have been discarded (due to being received after the timeout period).
        self._num_discarded_vote_proposals: int = 0

        # The time at which we will start discarding new proposals.
        self._discard_after: float = -1

        # The duration of time that must elapse before we start discarding new proposals.
        self._timeout: float = -1 

        # The SMR node ID Of the winner of the last election.
        # A value of -1 indicates that the winner has not been chosen/identified yet. 
        self._winner_id: int = -1 

        # The number of times that the election has been restarted.
        self._num_restarts:int = 0

        # The current attempt number of proposals (i.e., the largest attempt number we've received).
        self._current_attempt_number:int = 0

        # Future created when the first 'LEAD' proposal for the current election is received.
        self._pick_and_propose_winner_future: Optional[asyncio.Future[Any]] = None 

        self.logger: logging.Logger = logging.getLogger(__class__.__name__ + str(id))

    def set_pick_and_propose_winner_future(self, future: asyncio.Future[Any])->None:
        self._pick_and_propose_winner_future = future

    @property 
    def num_discarded_vote_proposals(self)->int:
        """
        The number of 'vote' proposals that have been discarded (due to being received after the timeout period).
        """
        return self._num_discarded_vote_proposals

    @property 
    def num_discarded_proposals(self)->int:
        """
        The number of proposals that have been discarded (due to being received after the timeout period).
        """
        return self._num_discarded_proposals

    @property 
    def current_attempt_number(self)->int:
        """
        The current attempt number of proposals (i.e., the largest attempt number we've received).
        """
        return self._current_attempt_number

    @property 
    def num_restarts(self)->int:
        """
        The number of times that the election has been restarted.
        """
        return self._num_restarts

    @property 
    def winner(self)->int:
        """
        Alias of `winner_id`. 

        The SMR node ID Of the winner of the last election.
        A value of -1 indicates that the winner has not been chosen/identified yet. 
        """
        return self._winner_id
    
    @property 
    def first_lead_proposal(self)->Optional[LeaderElectionProposal]:
        return self._first_lead_proposal

    @property 
    def first_lead_proposal_at(self)->float:
        """
        The time at which we received the first 'LEAD' proposal.

        -1 indicates that we've not yet received a 'LEAD' proposal.
        """
        if self._first_lead_proposal == None:
            return -1 
        
        return self._first_lead_proposal.timestamp
    
    @property 
    def winner_id(self)->int:
        """
        The SMR node ID Of the winner of the last election.
        A value of -1 indicates that the winner has not been chosen/identified yet. 
        """
        return self._winner_id

    @property
    def state(self)->ElectionState:
        """
        Alias of `election_state`

        The current state/status of the election.
        """
        return self._election_state
    
    @property
    def election_state(self)->ElectionState:
        """
        The current state/status of the election.
        """
        return self._election_state

    @property 
    def did_fail(self)->bool:
        return self._election_state == ElectionState.FAILED

    @property
    def completed_successfully(self)->bool:
        """
        Designate the election as no longer being active.
        """
        return self._election_state == ElectionState.COMPLETE

    @property
    def has_been_started(self)->bool:
        return self._election_state != ElectionState.INACTIVE

    @property
    def is_active(self)->bool:
        return self._election_state == ElectionState.ACTIVE

    @property 
    def term_number(self)->int:
        """
        Return the term of this election.

        Each election has a unique term number. Term numbers monotonically increase over time. 
        """
        return self._term_number 
    
    @property 
    def num_proposals_accepted(self)->int:
        """
        The number of proposals we've received, NOT including discarded proposals.
        """
        return len(self._proposals)

    @property 
    def num_proposals_received(self)->int:
        """
        The number of proposals we've received, including discarded proposals.
        """
        return len(self._proposals) + self._num_discarded_proposals

    @property 
    def num_vote_proposals_accepted(self)->int:
        """
        The number of 'vote' proposals we've received, NOT including discarded 'vote' proposals.
        """
        return len(self._vote_proposals)

    @property 
    def num_vote_proposals_received(self)->int:
        """
        The number of 'vote' proposals we've received, including discarded 'vote' proposals.
        """
        return len(self._vote_proposals) + self._num_discarded_vote_proposals
    
    @property 
    def proposals(self)->MutableMapping[int, LeaderElectionProposal]:
        """
        Return a mapping from SMR Node ID to the LeaderElectionProposal proposed by that node during this election.
        """
        return self._proposals
    
    @property 
    def all_proposals_received(self)->bool:
        """
        True if we've received a proposal from every node.
        """
        return len(self._proposals) == self._num_replicas

    def _received_proposal_from_node(self, node_id: int) -> bool:
        return node_id in self._proposals

    def _discard_old_proposals(self, proposer_id: int = -1, latest_attempt_number: int = -1):
        """
        Discard all proposals and 'vote' proposals with attempt numbers < `latest_attempt_number`.

        If we discard the first-received 'LEAD' proposal from this term, then we'll also cancel and clear the `_pick_and_propose_winner_future` Future.
        """
        # Iterate over all of the proposals, checking which (if any) need to be discarded.
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
                    self._timeout = -1 
                    
                    # Cancel this future if it exists. (It should probably exist, if the first 'LEAD' proposal also exists.)
                    if self._pick_and_propose_winner_future != None:
                        self._pick_and_propose_winner_future.cancel()
                        self._pick_and_propose_winner_future = None 
            
            # Remove proposals with smaller attempt numbers.
            for prop_id in to_remove:
                del self._proposals[prop_id]
        
        # Iterate over all of the proposals, checking which (if any) need to be discarded.
        # We do this even if the proposal we just received did not overwrite an existing proposal (in case a proposal was lost).
        to_remove: List[int] = [] 
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
                proposal_to_discard: LeaderElectionVote = self._vote_proposals[prop_id]
                
                # Update the "discarded proposals" mapping.
                inner_mapping: MutableMapping[int, LeaderElectionVote] = self._discarded_vote_proposals.get(self.term_number, {})
                inner_mapping[proposal_to_discard.attempt_number] = proposal_to_discard

                # Delete the mapping.
                del self._vote_proposals[prop_id]

    def start(self)->None:
        """
        Designate the election has having been started. 

        If the election is already in the 'active' state, then this will raise a ValueError.
        """
        if self._election_state == ElectionState.ACTIVE:
            raise ValueError(f"election for term {self.term_number} is already active")
        
        if self._election_state == ElectionState.COMPLETE:
            raise ValueError(f"election for term {self.term_number} already completed successfully")

        if self._election_state == ElectionState.FAILED:
            raise ValueError(f"election for term {self.term_number} already failed; must be restarted (rather than started)")

        self._election_state = ElectionState.ACTIVE
    
    def restart(self)->None:
        """
        Restart a failed election.
        """
        if self._election_state != ElectionState.FAILED:
            raise ValueError(f"election for term {self.term_number} is not in 'FAILED' state and thus cannot be restarted (current state: {self._election_state})")
        
        self._pick_and_propose_winner_future = None 
        self._election_state = ElectionState.ACTIVE
        self._num_restarts += 1

    def complete_election(self, winner_id: int):
        """
        Mark the election as having completed successfully. 
        """
        if self._election_state != ElectionState.ACTIVE:
            raise ValueError(f"election for term {self.term_number} is not active (current state: {self._election_state}); cannot complete election")

        self._winner_id = winner_id 
        self._election_state = ElectionState.COMPLETE

    def pick_winner_to_propose(self, last_winner_id: int = -1)->int:
        """
        Determine who should be proposed as the winner of the election based on the current proposals that have been received.

        Return the SMR node ID of the node that should be proposed as the winner. 
        
        Raises ValueError if no winner should be proposed at this time, such as due to an insufficient number of proposals having been received.
        """
        current_time: float = time.time() 

        # If the election isn't active, then we shouldn't be proposing anybody.
        if self._election_state != ElectionState.ACTIVE:
            raise ValueError(f"election for term {self._term_number} is in invalid state ({self._election_state}) to be trying to identify a winner to propose")

        # If `self._discard_after` hasn't even been set yet, then we should keep waiting for more proposals before making a decision.
        # Likewise, if `self._discard_after` has been set already, but there's still time to receive more proposals, then we should wait before making a decision.
        should_wait:bool = self._discard_after == -1 or (self._discard_after > 0 and self._discard_after > current_time)

        # If we've not yet received all proposals AND we should keep waiting, then we'll raise a ValueError, indicating that we should not yet select a node to propose as winner.
        # If we have received all proposals, or if we'll be discarding any future proposals that we receive, then we should go ahead and try to decide. 
        if (len(self._proposals) + self._num_discarded_proposals < self._num_replicas) and should_wait: 
            raise ValueError(f"insufficient number of proposals received to select a winner to propose (received: {len(self._proposals)}, discarded: {self._num_discarded_proposals}, number of replicas: {self._num_replicas})")
        
        # If we know the last winner and we have a proposal from them...
        if last_winner_id > 0 and self._received_proposal_from_node(last_winner_id):
            last_winners_proposal: LeaderElectionProposal = self._proposals[last_winner_id]

            # And the winner of the last election proposed 'LEAD' again, then we'll propose that they lead this election.
            if last_winners_proposal.is_lead:
                # If this future is non-nil, then set its result so that it won't be repeated again later.
                self._try_set_pick_and_propose_winner_future_result()
                return last_winner_id
            
        # If we've received at least one 'LEAD' proposal, then return the node ID of whoever proposed 'LEAD' first.
        if self._first_lead_proposal != None:
            # If this future is non-nil, then set its result so that it won't be repeated again later.
            self._try_set_pick_and_propose_winner_future_result()
            return self._first_lead_proposal.proposer_id

        self._try_set_pick_and_propose_winner_future_result()

        # Return -1, indicating that we should propose FAILURE.
        return -1 

    def _try_set_pick_and_propose_winner_future_result(self):
        """
        Set the result on the `self._pick_and_propose_winner_future` asyncio.Future object, if it exists and has not already been completed.
        """
        # If this future is non-nil, then set its result so that it won't be repeated again later.
        if self._pick_and_propose_winner_future != None:
            assert not self._pick_and_propose_winner_future.done()
            self._pick_and_propose_winner_future.set_result(1)

    def add_vote_proposal(self, vote: LeaderElectionVote, overwrite: bool = False, received_at = time.time()) -> bool:
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
        proposer_id:int = vote.proposer_id
        current_attempt_number:int = vote.attempt_number

        # Update the current attempt number if the newly-received proposal has a greater attempt number than any of the other proposals that we've seen so far. 
        if current_attempt_number > self._current_attempt_number:
            self._current_attempt_number = current_attempt_number

        # Check if there's already an existing proposal. 
        # If so, and if overwrite is False, then we raise a ValueError.
        existing_proposal: Optional[LeaderElectionVote] = self._vote_proposals.get(proposer_id, None) 
        if existing_proposal != None: 
            prev_attempt_number:int = existing_proposal.attempt_number

            # Raise an error if we've not been instructed to overwrite. 
            # If we have been instructed to overwrite, then we'll end up storing the new proposal at the end of this method.
            if prev_attempt_number >= current_attempt_number and not overwrite:
                raise ValueError(f"new proposal has attempt number {current_attempt_number}, whereas previous/existing proposal has attempt number {prev_attempt_number}")

        if self._discard_after > 0 and received_at > self._discard_after: # `self._discard_after` is initially equal to -1, so it isn't valid until it is set to a positive value.
            self._num_discarded_vote_proposals += 1
            return False   

        # Store the new proposal. 
        self._vote_proposals[proposer_id] = vote

        # If the length is 1, then this is the first "vote" proposal received.
        # If the length is not 1, then this is not the first.
        return len(self._vote_proposals) == 1 

    def add_proposal(self, proposal: LeaderElectionProposal, future_io_loop: asyncio.AbstractEventLoop, received_at:float = time.time())->Optional[tuple[asyncio.Future[Any], float]]:
        """
        Register a proposed LeaderElectionProposal.

        Let P' be a new proposal from Node N. Let P be the existing proposal from Node N.
        If TERM_NUMBER(P') > TERM_NUMBER(P) and `overwrite` is True, then P will be replaced with P'.

        Let X be the set of proposals from the other nodes (i.e., nodes whose ID != N). For each proposal x in X, if TERM_NUMBER(x) < TERM_NUMBER(P'), then x will be thrown out/discarded.

        That is, when overwriting an existing proposal with a new proposal, any other proposals (from other nodes) whose attempt numbers are lower than the new proposal will be discarded.

        Return a tuple containing a Future and a timeout. The future should be resolved after the timeout.
        """
        proposer_id:int = proposal.proposer_id
        latest_attempt_number:int = proposal.attempt_number

        # Update the current attempt number if the newly-received proposal has a greater attempt number than any of the other proposals that we've seen so far. 
        if latest_attempt_number > self._current_attempt_number:
            old_largest:int = self._current_attempt_number 
            self._current_attempt_number = latest_attempt_number

            # If we previously failed and just got a new proposal with a new highest attempt number, then restart! 
            # We'll purge/discard the old proposals down below.
            if self._election_state == ElectionState.FAILED:
                self.logger.debug(f"Election {self.term_number} restarting. Received new highest attempt number ({latest_attempt_number}; prev: {old_largest}).")
                self.restart()

        # Check if we need to discard the proposal due to there already being an existing proposal from that node with an attempt number >= the attempt number of the proposal we just received.
        # If the new proposal has an attempt number equal to or lower than the last proposal we received from this node, then that's a problem.
        existing_proposal: Optional[LeaderElectionProposal] = self._proposals.get(proposer_id, None) 
        if existing_proposal != None and existing_proposal.attempt_number >= latest_attempt_number:
            self.logger.warn(f"Discarding proposal from node {proposal.proposer_id} during election term {proposal.election_term} because new proposal has attempt number ({latest_attempt_number}) <= existing proposal from same node's attempt number ({existing_proposal.attempt_number}).")
            return None 

        # Check if we need to discard the proposal due to it being received late.
        if self._discard_after > 0 and received_at > self._discard_after: # `self._discard_after` is initially equal to -1, so it isn't valid until it is set to a positive value.
            self._num_discarded_proposals += 1
            self.logger.warn(f"Discarding proposal from node {proposal.proposer_id} during election term {proposal.election_term} because it was received too late.")
            return None   
        
        # We're not discarding the proposal, so let's officially store the new proposal. 
        self._proposals[proposer_id] = proposal

        # Check if this is the first 'LEAD' proposal that we've received. 
        # If so, we'll return a future that can be used to ensure we make a decision after the timeout period, even if we've not received all other proposals.
        if self._first_lead_proposal == None and proposal.is_lead:
            self._first_lead_proposal = proposal 
            self._timeout = 10 
            self._discard_after = time.time() + self._timeout

            self._pick_and_propose_winner_future = asyncio.Future(loop = future_io_loop)
            
            return self._pick_and_propose_winner_future, self._discard_after        

        # It wasn't the first 'LEAD' proposal, so we just return None. 
        return None  