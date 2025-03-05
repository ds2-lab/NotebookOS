from enum import StrEnum 

from typing import Dict, Any, Optional

class IOPubNotification(StrEnum):
    """
    These are embedded in IOPub messages to report on the status of an election.
    """
    ElectionStatus = "election_status"
    
    # Sent by the primary replica upon being elected.
    SmrLeadTask = "smr_lead_task"
    
    # Sent by follower replicas upon officially yielding a request.
    SmrYieldTask = "smr_yield_task"

    ExecuteStatistics = "execute_statistics"
    
    # Sent by the first replica to see one of its proposals get committed.
    # For example, if Replica 2 proposes "LEAD" and Replica 3 proposes "LEAD",
    # and Replica 2's proposal is the first to be committed, then Replica 2 will
    # send an IOPub notification about this.
    ElectionFirstLeadProposalCommitted = "election_first_lead_committed"
    
    # Sent by the first replica to see its vote get committed to the RaftLog.
    ElectionFirstVoteCommitted = "election_first_vote_committed"
    
    # Sent by the primary replica when it begins downloading a model
    # while executing user-submitted code.
    DownloadingModel = "downloading_model"
    # Sent by the primary replica when it finishes downloading a model
    # while executing user-submitted code.
    DownloadedModel = "downloaded_model"
   
    # Sent by the primary replica when it begins downloading a dataset
    # while executing user-submitted code. 
    DownloadingDataset = "downloading_dataset"
    # Sent by the primary replica when it finishes downloading a dataset
    # while executing user-submitted code.
    DownloadedDataset = "downloaded_dataset"