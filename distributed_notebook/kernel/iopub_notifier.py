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
    
    # Sent by the first replica to see one of its proposals get committed.
    # For example, if Replica 2 proposes "LEAD" and Replica 3 proposes "LEAD",
    # and Replica 2's proposal is the first to be committed, then Replica 2 will
    # send an IOPub notification about this.
    ElectionFirstLeadProposed = "election_first_lead_proposed"
    
    # Sent by the first replica to see its vote get committed to the RaftLog.
    ElectionFirstVoteProposed = "election_first_vote_proposed"
    
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

class IOPubNotifier(object):
    def __init__(
        self,
        ident: str = "",
        session: Optional[] = None,
        iopub_socket: Optional[] = None,
    ):
        self._iopub_socket: Optional[] = iopub_socket
        self._ident = ident 
        self._session = session
    
        # Initialize logging
        self.log = logging.getLogger(__class__.__name__)
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)
    
    @property 
    def ident(self)->str:
        return self._ident 
    
    def _topic(self, topic):
        """prefixed topic for IOPub messages"""
        return (f"kernel.{self.ident}.{topic}").encode()    
    
    def send_notification(
        self,
        status: IOPubNotification,
        content: Dict[str, Any] = {},
        metadata: Dict[str, Any] = {},
    ):
        self.session.send(
            self._iopub_socket,
            content = content,
            metadata = metadata,
            ident=self._topic(str(status)),
        )