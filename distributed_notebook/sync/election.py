from typing import Dict, Optional, List 
from enum import Enum

from .log import SyncValue, SynchronizedValue, LeaderElectionVote

class ElectionState(Enum):
    INACTIVE = 1    # Created, but not yet started.
    ACTIVE = 2      # Active, in progress
    COMPLETE = 3    # Finished
    ERRED = 4       # Created, started, but then entered some sort of error state and could not complete successfully 

class Election(object):
    """
    Encapsulates the information about the current election term.
    """
    def __init__(
            self, 
            term_number: int,
    ):
        # The term number of the election.
        # Each election has a unique term number.
        # Term numbers monotonically increase over time. 
        self._term_number = term_number

        # Mapping from SMR Node ID to the SynchronizedValue that it proposed during this election term.
        self._proposals: Dict[int, SynchronizedValue] = {} 

        # Mapping from SMR node ID to an inner mapping.
        # The inner mapping is a mapping from attempt number to the associated SynchronizedValue proposed by that node (during that attempt) during this election term.
        self._discarded_proposals: Dict[int, Dict[int, SynchronizedValue]] = {}

        # The current state/status of the election.
        self._election_state: ElectionState = ElectionState.INACTIVE

        # The SMR node ID Of the winner of the last election.
        # A value of -1 indicates that the winner has not been chosen/identified yet. 
        self._winner_id: int = -1 

    @property 
    def winner(self)->int:
        """
        Alias of `winner_id`. 

        The SMR node ID Of the winner of the last election.
        A value of -1 indicates that the winner has not been chosen/identified yet. 
        """
        return self._winner_id
    
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
    def is_erred(self)->bool:
        return self._election_state == ElectionState.ERRED

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
    def proposals(self)->Dict[int, SynchronizedValue]:
        """
        Return a mapping from SMR Node ID to the SynchronizedValue proposed by that node during this election.
        """
        return self._proposals
    
    def start(self)->None:
        """
        Designate the election has having been started. 

        If the election is already in the 'active' state, then this will raise a ValueError.
        """
        if self._active:
            raise ValueError("election is already active")
        
        self._active = True 
    
    def complete_election(self, winner_id: int):
        """
        Mark the election as having completed successfully. 
        """
        if not self._active:
            raise ValueError(f"election for term {self.term_number} is not active (current state: {self.election_state}); cannot complete election")

        self._winner_id = winner_id 
        self._election_state = ElectionState.COMPLETE

    def add_proposal(self, proposal: SynchronizedValue, overwrite: bool = False)->None:
        """
        Register a proposed SynchronizedValue.
        
        If `overwrite` is False and there is already a SynchronizedValue registered for the associated node, then a ValueError is raised.
        If `overwrite` is True, then the existing/already-registered SynchronizedValue is simply overwritten.

        Let P' be a new proposal from Node N. Let P be the existing proposal from Node N.
        If TERM_NUMBER(P') > TERM_NUMBER(P) and `overwrite` is True, then P will be replaced with P'.

        Let X be the set of proposals from the other nodes (i.e., nodes whose ID != N). For each proposal x in X, if TERM_NUMBER(x) < TERM_NUMBER(P'), then x will be thrown out/discarded.

        That is, when overwriting an existing proposal with a new proposal, any other proposals (from other nodes) whose attempt numbers are lower than the new proposal will be discarded.
        """
        proposer_id:int = proposal.proposer_id
        current_attempt_number:int = proposal.attempt_number

        # Check if there's already an existing proposal. 
        # If so, and if overwrite is False, then we raise a ValueError.
        existing_proposal: Optional[SynchronizedValue] = self._proposals.get(proposer_id, None) 
        if existing_proposal != None: 
            prev_attempt_number:int = existing_proposal.attempt_number

            # Raise an error if we've not been instructed to overwrite. 
            # If we have been instructed to overwrite, then we'll end up storing the new proposal at the end of this method.
            if prev_attempt_number >= current_attempt_number and not overwrite:
                raise ValueError(f"new proposal has attempt number {current_attempt_number}, whereas previous/existing proposal has attempt number {prev_attempt_number}")
            
        # Iterate over all of the proposals, checking which (if any) need to be discarded.
        # We do this even if the proposal we just received did not overwrite an existing proposal (in case a proposal was lost).
        to_remove: List[int] = [] 
        for prop_id, prop in self._proposals.items():
            # Skip/ignore the proposal for the node whose proposal we're adding.
            if prop_id == proposer_id:
                continue
            
            # If the attempt number is smaller, then mark the proposal for removal.
            if prop.attempt_number < current_attempt_number:
                to_remove.append(prop_id)
            
            # Remove proposals with smaller attempt numbers.
            for prop_id in to_remove:
                del self._proposals[prop_id]

        # Store the new proposal. 
        self._proposals[proposer_id] = proposal