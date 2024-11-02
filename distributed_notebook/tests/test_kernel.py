import asyncio
import os
import sys
from collections import OrderedDict
from typing import Optional
from unittest import mock

import pytest
import pytest_asyncio

import distributed_notebook.sync.raft_log
from distributed_notebook.kernel.kernel import DistributedKernel
from distributed_notebook.sync import Synchronizer, CHECKPOINT_AUTO, RaftLog
from distributed_notebook.sync.election import Election
from distributed_notebook.sync.log import SynchronizedValue, ElectionProposalKey, LeaderElectionProposal, \
    LeaderElectionVote, Checkpointer, ExecutionCompleteNotification
from distributed_notebook.tests.utils.session import TestSession

DefaultKernelId: str = "8a275c45-52fc-4390-8a79-9d8e86066a65"
DefaultDate: str = "2024-11-01T15:32:45.123456789Z"


@pytest_asyncio.fixture
async def kernel() -> DistributedKernel:
    # def mocked_raftlog_propose(*args, **kwargs):
    #     print(f"\n\nMocked RaftLog::Propose called with args {args} and kwargs {kwargs}.")
    #
    # with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "propose", mocked_raftlog_propose):
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

    await kernel.override_shell()

    # Need to yield here rather than return, or else we'll go out-of-scope,
    # and the mock that happens above won't work.
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


async def mocked_sync(execution_ast, source: Optional[str] = None, checkpointer: Optional[Checkpointer] = None) -> bool:
    print(
        f"\nMocked Synchronizer::sync called with execution_ast={execution_ast}, source={source}, checkpointer={checkpointer}")
    await asyncio.sleep(0.25)
    return True


def propose(raftLog: RaftLog, proposedValue: LeaderElectionProposal, election: Election,
            execute_request_task: asyncio.Task[any], expectedNumProposals: int = -1):
    if expectedNumProposals == -1:
        expectedNumProposals = election.num_proposals_received + 1

    raftLog._valueCommittedCallback(proposedValue, sys.getsizeof(proposedValue), proposedValue.id)

    # Check that everything is updated correctly now that we've received a proposal.
    assert (raftLog.decide_election_future is not None)
    assert (election.num_proposals_received == expectedNumProposals)
    assert (election.proposals.get(proposedValue.proposer_id) is not None)
    assert (election.proposals.get(proposedValue.proposer_id) == proposedValue)
    assert (len(raftLog._proposed_values) == 1)

    assert (election.term_number in raftLog._proposed_values)
    assert (raftLog._proposed_values.get(election.term_number) is not None)

    if raftLog._node_id == proposedValue.proposer_id:
        innerMap: OrderedDict[int, LeaderElectionProposal] = raftLog._proposed_values.get(election.term_number)
        assert (innerMap is not None)
        assert (len(innerMap) == 1)
        assert (innerMap[proposedValue.proposer_id] == proposedValue)

    assert (raftLog._future_io_loop is not None)
    assert (raftLog._election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog._election_decision_future.done() == False)
    assert (raftLog._leading_future.done() == False)

    leading_future: asyncio.Future[int] = raftLog._leading_future
    assert leading_future is not None
    assert leading_future.done() == False

    assert execute_request_task.done() == False
    assert leading_future is not None
    assert leading_future.done() == False


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_basic_election(kernel, execute_request):
    print(f"Testing execute request with kernel {kernel} and execute request {execute_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert (synchronizer is not None)
    assert (raftLog is not None)

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        print(f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(kernel.execute_request(None, [], execute_request))
        proposedValue: LeaderElectionProposal = await election_proposal_future

    # Check that the kernel proposed a LEAD value.
    assert (proposedValue.key == str(ElectionProposalKey.LEAD))
    assert (proposedValue.proposer_id == kernel.smr_node_id)
    assert (proposedValue.election_term == 1)

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)
    assert (election is not None)
    assert (election.term_number == 1)
    assert (election.num_proposals_received == 0)
    assert (raftLog._future_io_loop is not None)
    assert (raftLog._election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog._election_decision_future.done() == False)
    assert (raftLog._leading_future.done() == False)

    # We've proposed it, so the RaftLog knows about it, even though the value hasn't been committed yet.
    assert (len(raftLog._proposed_values) == 1)

    innerMap: OrderedDict[int, LeaderElectionProposal] = raftLog._proposed_values.get(election.term_number)
    assert (innerMap is not None)
    assert (len(innerMap) == 1)
    assert (1 in innerMap)
    assert (innerMap[1] == proposedValue)

    leading_future: asyncio.Future[int] = raftLog._leading_future
    assert leading_future is not None
    assert leading_future.done() == False

    propose(raftLog, proposedValue, election, execute_request_task)

    # Call "value committed" handler again for the 2nd proposal.
    leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                           proposer_id=2,
                                                                           election_term=1,
                                                                           attempt_number=1)
    propose(raftLog, leadProposalFromNode2, election, execute_request_task)

    vote_proposal_future: asyncio.Future[LeaderElectionVote] = loop.create_future()

    async def mocked_append_election_vote(*args, **kwargs):
        print(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog._election_decision_future
    assert (election_decision_future is not None)

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        print(f"\n\nMocked RaftLog::_append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             _append_execution_end_notification=mocked_raftlog_append_execution_end_notification):
        print(f"\n\n\n\nraftLog._append_execution_end_notification: {raftLog._append_execution_end_notification}")
        print(f"kernel.synclog._append_execution_end_notification: {kernel.synclog._append_execution_end_notification}")
        print(
            f"kernel.synchronizer._synclog._append_execution_end_notification: {kernel.synchronizer._synclog._append_execution_end_notification}")

        assert raftLog._append_execution_end_notification is not None
        assert kernel.synclog._append_execution_end_notification is not None
        assert kernel.synchronizer._synclog._append_execution_end_notification is not None

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=1,
                                                                               attempt_number=1)
        raftLog._valueCommittedCallback(leadProposalFromNode3, sys.getsizeof(proposedValue), proposedValue.id)

        try:
            proposedVote: LeaderElectionVote = await asyncio.wait_for(vote_proposal_future, 5)
        except TimeoutError:
            print("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                print("\n\n\n")

            assert False

        print(f"Got proposed vote: {proposedVote}")

        assert (raftLog._leading_future.done() == False)
        assert (election.num_proposals_received == 3)
        assert (election.proposals.get(1) == proposedValue)
        assert (election.proposals.get(2) == leadProposalFromNode2)
        assert (election.proposals.get(3) == leadProposalFromNode3)
        assert (len(raftLog._proposed_values) == 1)
        assert election.is_active
        assert election.voting_phase_completed_successfully == False

        assert election_decision_future.done()
        vote: LeaderElectionVote = election_decision_future.result()
        assert vote is not None
        assert vote == proposedVote
        assert vote.election_term == 1
        assert vote.proposer_id == 1
        assert vote.proposed_node_id == 1
        assert vote.attempt_number == 1

        assert election.is_active
        assert election.voting_phase_completed_successfully == False
        assert leading_future is not None
        assert leading_future.done() == False

        raftLog._valueCommittedCallback(vote, sys.getsizeof(proposedValue), vote.id)

        assert election.voting_phase_completed_successfully == True
        assert election.winner == 1

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(leading_future, 5)
        except TimeoutError:
            print("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        print("\"Leading\" future should be done now.")
        assert leading_future.done() == True
        assert raftLog.leader_id == 1
        assert raftLog.leader_term == 1
        wait, leading = raftLog._is_leading(1)
        assert wait == False
        assert leading == True

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execution_done_future, 5)
        except TimeoutError:
            print("[ERROR] \"execution_done\" future was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                print("\n\n\n")

            assert False

        assert execution_done_future.done()

        notification: ExecutionCompleteNotification = execution_done_future.result()
        print(f"Got ExecutionCompleteNotification: {notification}")
        assert notification is not None
        assert notification.proposer_id == 1
        assert notification.election_term == 1

        raftLog._valueCommittedCallback(notification, sys.getsizeof(notification), notification.id)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            print("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                print("\n\n\n")

            assert False

        assert execute_request_task.done()
