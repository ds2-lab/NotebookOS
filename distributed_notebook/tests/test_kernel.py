import asyncio
import os
import sys
from collections import OrderedDict

import pytest
import pytest_asyncio

import distributed_notebook.sync.raft_log2
from distributed_notebook.kernel.kernel import DistributedKernel
from distributed_notebook.smr.smr import LogNode
from distributed_notebook.sync import Synchronizer, CHECKPOINT_AUTO, RaftLog
from distributed_notebook.sync.election import Election
from distributed_notebook.sync.log import SynchronizedValue, ElectionProposalKey, LeaderElectionProposal
from distributed_notebook.sync.raft_log import KEY_LEAD
from distributed_notebook.tests.utils.session import TestSession

from unittest import mock

DefaultKernelId: str = "8a275c45-52fc-4390-8a79-9d8e86066a65"
DefaultDate: str = "2024-11-01T15:32:45.123456789Z"

@pytest_asyncio.fixture
async def kernel() -> DistributedKernel:
    kwargs = {
        "hdfs_namenode_hostname": "127.0.0.1:10000",
        "kernel_id": DefaultKernelId,
        "smr_port": 8000,
        "smr_node_id": 1,
        "smr_nodes": [],
        "smr_join": False,
        "should_register_with_local_daemon": False,
        "pod_name": "TestPod",
        "node_name": "TestNode",
        "debug_port": -1,
    }

    os.environ.setdefault("PROMETHEUS_METRICS_PORT", "-1")
    kernel: DistributedKernel = DistributedKernel(**kwargs)
    kernel.num_replicas = 3
    kernel.should_read_data_from_hdfs = False
    kernel.deployment_mode = "DOCKER_SWARM"
    kernel.session = TestSession()
    kernel.store = "/"
    kernel.prometheus_port = -1
    kernel.debug_port = -1

    kernel.synclog = RaftLog(kernel.smr_node_id,
                             base_path=kernel.store,
                             kernel_id=kernel.kernel_id,
                             num_replicas=kernel.num_replicas,
                             hdfs_hostname=kernel.hdfs_namenode_hostname,
                             should_read_data_from_hdfs=kernel.should_read_data_from_hdfs,
                             peer_addrs=[],
                             peer_ids=[],
                             join=kernel.smr_join,
                             debug_port=kernel.debug_port,
                             report_error_callback=kernel.report_error,
                             send_notification_func=kernel.send_notification,
                             hdfs_read_latency_callback=kernel.hdfs_read_latency_callback,
                             deploymentMode=kernel.deployment_mode)

    kernel.synchronizer = Synchronizer(kernel.synclog, module=None, opts=CHECKPOINT_AUTO)

    return kernel

# async def mocked_serialize_and_append_value(*args, **kwargs):
#     print(f"Mocked RaftLog::_serialize_and_append_value called with args {args} and kwargs {kwargs}.")

@pytest.fixture
def execute_request():
    return {
        "header": {
            "msg_id": "70d1412e-f937-416e-99fe-a48eed8dc8a4",
            "msg_type": "execute_request",
            "date": DefaultDate,
            "username": DefaultKernelId,
            "session": DefaultKernelId,
            "version": 1
        },
        "parent_header": {},
        "content": {
            "code": "a = 1"
        },
        "metadata": {}
    }

@pytest.mark.asyncio
async def test_election_created(kernel, execute_request):
    print(f"Testing execute request with kernel {kernel} and execute request {execute_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert(synchronizer is not None)
    assert(raftLog is not None)

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    proposal_future: asyncio.Future[any] = loop.create_future()
    async def mocked_serialize_and_append_value(*args, **kwargs):
        print(f"Mocked RaftLog::_serialize_and_append_value called with args {args} and kwargs {kwargs}.")
        proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log2.RaftLog, "_serialize_and_append_value", mocked_serialize_and_append_value):
        execute_request_task: asyncio.Task[any] = loop.create_task(kernel.execute_request(None, [], execute_request))
        proposedValue: SynchronizedValue = await proposal_future

    # Check that the kernel proposed a LEAD value.
    assert(proposedValue.key == str(ElectionProposalKey.LEAD))
    assert(proposedValue.proposer_id == kernel.smr_node_id)
    assert(proposedValue.election_term == 1)

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election =kernel.synclog.get_election(1)
    assert(election is not None)
    assert(election.term_number == 1)
    assert(election.num_proposals_received == 0)
    assert(raftLog._future_io_loop is not None)
    assert(raftLog._election_decision_future is not None)
    assert(raftLog._leading_future is not None)
    assert(raftLog._election_decision_future.done() == False)
    assert(raftLog._leading_future.done() == False)

    # We've proposed it, so the RaftLog knows about it, even though the value hasn't been committed yet.
    assert(len(raftLog._proposed_values) == 1)

    innerMap: OrderedDict[int, LeaderElectionProposal] = raftLog._proposed_values.get(election.term_number)
    assert(innerMap is not None)
    assert(len(innerMap) == 1)
    assert(1 in innerMap)
    assert(innerMap[1] == proposedValue)

    print("Calling RaftLog::_valueCommittedCallback now.")

    raftLog._valueCommittedCallback(proposedValue, sys.getsizeof(proposedValue), proposedValue.id)

    print("Called RaftLog::_valueCommittedCallback.")

    # Check that everything is updated correctly now that we've received a proposal.
    assert(raftLog.decide_election_future is not None)
    assert(election.num_proposals_received == 1)
    assert(election.proposals.get(kernel.smr_node_id) is not None)
    assert(election.proposals.get(kernel.smr_node_id) == proposedValue)
    assert(len(raftLog._proposed_values) == 1)

    assert(election.term_number in raftLog._proposed_values)
    assert(raftLog._proposed_values.get(election.term_number) is not None)

    innerMap: OrderedDict[int, LeaderElectionProposal] = raftLog._proposed_values.get(election.term_number)
    assert(innerMap is not None)
    assert(len(innerMap) == 1)
    assert(1 in innerMap)
    assert(innerMap[1] == proposedValue)
    
    assert(raftLog._future_io_loop is not None)
    assert(raftLog._election_decision_future is not None)
    assert(raftLog._leading_future is not None)
    assert(raftLog._election_decision_future.done() == False)
    assert(raftLog._leading_future.done() == False)
