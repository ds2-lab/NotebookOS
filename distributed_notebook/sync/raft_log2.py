import io
import os
import base64
import pickle
import asyncio
import json
import logging
import time
from typing import Tuple, Callable, Optional, Any, Iterable, Dict, List
import datetime

from ..smr.smr import NewLogNode, NewConfig, NewBytes, WriteCloser, ReadCloser, PrintTestMessage
from ..smr.go import Slice_string, Slice_int, Slice_byte
from .log import SyncLog, SyncValue
from .checkpoint import Checkpoint
from .future import Future
from .errors import print_trace, SyncError, GoError, GoNilError
from .reader import readCloser
from .file_log import FileLog

KEY_LEAD = "_lead_" # Propose to lead the execution term (i.e., execute the user's code).
KEY_YIELD = "_yield_" # Propose to yield the execution to another replica.
KEY_SYNC = "_sync_" # Synchronize to confirm decision about who is executing the code.
KEY_FAILURE = "_failure_" # We cannot execute this request... 

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
        id: int,
        base_path: str = "/store", 
        hdfs_hostname: str = "172.17.0.1:9000",
        data_directory: str = "/storage",
        peer_addrs: Iterable[str] = [], 
        peer_ids: Iterable[int] = [], 
        join: bool = False, 
        debug_port:int = 8464
    ):
        if len(hdfs_hostname) == 0:
            raise ValueError("HDFS hostname is empty.")
        
        if debug_port <= 1023 or debug_port >= 65535:
           raise ValueError("Invalid debug port specified.")

        self.logger: logging.Logger = logging.getLogger(__class__.__name__ + str(id))
        self.logger.info("Creating RaftNode %d now." % id)

        self.persistent_store_path:str = base_path
        self.node_id = id 
        self.offloader: FileLog = FileLog(self.persistent_store_path)

        self.create_persistent_store_path(base_path)

        self.logger.info("persistent store path: %s" % self.persistent_store_path)
        self.logger.info("hdfs_hostname: \"%s\"" % hdfs_hostname)
        self.logger.info("data_directory: \"%s\"" % data_directory)
        self.logger.info("peer_addrs: %s" % peer_addrs)
        self.logger.info("peer_ids: %s" % peer_ids)
        self.logger.info("join: %s" % join)
        self.logger.info("debug_port: %d" % debug_port)

        self.log_node = NewLogNode(self.persistent_store_path, id, hdfs_hostname, data_directory, Slice_string(peer_addrs), Slice_int(peer_ids), join, debug_port)
        if self.log_node == None:
            raise RuntimeError("Failed to create LogNode.")
        elif not self.log_node.ConnectedToHDFS():
            self.logger.error("The LogNode failed to connect to HDFS.")
            raise RuntimeError("The LogNode failed to connect to HDFS")
        
        self.logger.info(f"Successfully created LogNode {id}.")
        
        self.my_current_attempt_number : int = 1 # Attached to proposals. Sort of an ID within an election term. 
        self.winners_per_term: Dict[int, int] = {} # Mapping from term number -> SMR node ID of the winner of that term.
        self.my_proposals: Dict[int, Dict[int, SyncValue]] = {} # Mapping from term number -> Dict. The inner map is attempt number -> proposal.
        self.my_current_attempt_number:int = 1 # The current attempt number for the current term. 
        self.largest_peer_attempt_number:Dict[int, int] = {0:0} # The largest attempt number received from a peer's proposal.
        self.proposals_per_term: Dict[int, Dict[int, SyncValue]] = {} # Mapping from term number -> dict. Inner dict is map from SMR node ID -> proposal.
        self.own_proposal_times: Dict[int, float] = {} # Mapping from term number -> the time at which we proposed LEAD/YIELD in that term.
        self.first_lead_proposal_received_per_term: Dict[int, SyncValue] = {} # Mapping from term number -> the first 'LEAD' proposal received in that term.
        self.first_proposal_received_per_term: Dict[int, SyncValue] = {} # Mapping from term number -> the first proposal received in that term.
        self.timeout_durations: Dict[int, float] = {} # Mapping from term number -> the timeout (in seconds) for that term.
        self.discard_after: Dict[int, float] = {} # Mapping from term number -> the time after which received proposals will be discarded.
        self.num_proposals_discarded: Dict[int, int] = {} # Mapping from term number -> the number of proposals that were discarded in that term.
        self.sync_proposals_per_term: Dict[int, SyncValue] = {} # Mapping from term number -> the first SYNC proposal committed during that term.
        self.decisions_proposed: Dict[int, bool] = {} # Mapping from term number -> boolean flag indicating whether we've proposed (but not necessarily committed) a decision for the given term yet.

        self.ignore_changes: int = 0
        self.closed = None 

        # This will just do nothing if there's no serialized state to be loaded.
        self.load_and_apply_serialized_state()

    def create_persistent_store_path(self, path: str):
        """
        Create a directory at the specified path if it does not already exist.
        """
        if path != "" and not os.path.exists(path):
            self.logger.debug(f"Creating persistent store directory: \"{path}\"")
            os.makedirs(path, 0o750, exist_ok = True) # It's OK if it already exists.
            self.logger.debug(f"Created persistent store directory \"{path}\" (or it already exists).")

    def get_serialized_state(self) -> bytes:
        """
        Serialize important state so that it can be written to HDFS (for recovery purposes).
        
        This return value of this function should be passed to the `self._node.WriteDataDirectoryToHDFS` function.
        """        
        data_dict:dict = {
            "winners_per_term": self.winners_per_term,
            "my_proposals": self.my_proposals,
            "my_current_attempt_number": self.my_current_attempt_number,
            "largest_peer_attempt_number": self.largest_peer_attempt_number,
            "proposals_per_term": self.proposals_per_term,
            "own_proposal_times": self.own_proposal_times,
            "first_lead_proposal_received_per_term": self.first_lead_proposal_received_per_term,
            "first_proposal_received_per_term": self.first_proposal_received_per_term,
            "timeout_durations": self.timeout_durations,
            "discard_after": self.discard_after,
            "num_proposals_discarded": self.num_proposals_discarded,
            "sync_proposals_per_term": self.sync_proposals_per_term,
            "decisions_proposed": self.decisions_proposed,
            "_leader_term": self._leader_term,
            "_leader_id": self._leader_id,
            "_expected_term": self._expected_term,
        }
        
        return pickle.dumps(data_dict)

    def load_and_apply_serialized_state(self) -> None:
        """
        Retrieve the serialized state read by the Go-level LogNode. 
        This state is read from HDFS during migration/error recovery.
        Update our local state with the state retrieved from HDFS.
        """
        serialized_state_bytes:bytes = bytes(self._node.GetSerializedState()) # Convert the Go bytes (Slice_byte) to Python bytes.
        
        if len(serialized_state_bytes) == 0:
            self._log.debug("No serialized state found. Nothing to load and apply.")
            return 
        
        data_dict:dict = pickle.loads(serialized_state_bytes) # json.loads(serialized_state_json)
        if len(data_dict) == 0:
            self._log.debug("No serialized state found. Nothing to apply.")
            return 
        
        for key, entry in data_dict.items():
            self._log.debug(f"Retrived state \"{key}\": {str(entry)}")
        
        # TODO: 
        # There may be some bugs that arrise from these values being somewhat old or outdated, potentially.
        self.winners_per_term = data_dict["winners_per_term"]
        self.my_proposals = data_dict["my_proposals"]
        self.my_current_attempt_number = data_dict["my_current_attempt_number"]
        self.largest_peer_attempt_number = data_dict["largest_peer_attempt_number"]
        self.proposals_per_term = data_dict["proposals_per_term"]
        self.own_proposal_times = data_dict["own_proposal_times"]
        self.first_lead_proposal_received_per_term = data_dict["first_lead_proposal_received_per_term"]
        self.first_proposal_received_per_term = data_dict["first_proposal_received_per_term"]
        self.timeout_durations = data_dict["timeout_durations"]
        self.discard_after = data_dict["discard_after"]
        self.num_proposals_discarded = data_dict["num_proposals_discarded"]
        self.sync_proposals_per_term = data_dict["sync_proposals_per_term"]
        self.decisions_proposed = data_dict["decisions_proposed"]
        
        # If true, then the "remote updates" that we're receiving are us catching up to where we were before a migration/eviction was triggered.
        self._catchingUpAfterMigration = True 

        # The value of _leader_term before a migration/eviction was triggered.
        self.leader_term_before_migration: int = data_dict["_leader_term"]

        # Commenting these out for now; it's not clear if we should set these in this way yet.
        # self._leader_term = data_dict["_leader_term"]
        # self._leader_id = data_dict["_leader_id"]
        # self._expected_term = data_dict["_expected_term"]