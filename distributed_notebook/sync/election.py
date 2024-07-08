from typing import Dict
from enum import Enum

from .log import SyncValue, ProposedValue, LeaderElectionVote

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

        # Mapping from SMR Node ID to the SyncValue that it proposed during this election.
        self._proposals: Dict[int, SyncValue] = {} 

        # The current state/status of the election.
        self._election_state: ElectionState = ElectionState.INACTIVE
    
    def start(self)->None:
        """
        Designate the election has having been started. 

        If the election is already in the 'active' state, then this will raise a ValueError.
        """
        if (self._active):
            raise ValueError("election is already active")
        
        self._active = True 

    @property
    def state(self)->ElectionState:
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
    def proposals(self)->Dict[int, SyncValue]:
        """
        Return a mapping from SMR Node ID to the SyncValue proposed by that node during this election.
        """
        return self._proposals

    def add_proposal(self, proposal: SyncValue, overwrite: bool = False)->None:
        """
        Register a proposed SyncValue.
        
        If `overwrite` is False and there is already a SyncValue registered for the associated node, then a ValueError is raised.
        If `overwrite` is True, then the existing/already-registered SyncValue is simply overwritten.
        """
        pass 