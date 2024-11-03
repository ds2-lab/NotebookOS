import asyncio
import os
import pickle
import sys
from collections import OrderedDict
from typing import Optional
from unittest import mock

import pytest
import pytest_asyncio

import distributed_notebook.sync.raft_log
from distributed_notebook.kernel.kernel import DistributedKernel
from distributed_notebook.sync import Synchronizer, CHECKPOINT_AUTO, RaftLog
from distributed_notebook.sync.election import Election, ExecutionCompleted, AllReplicasProposedYield
from distributed_notebook.sync.log import ElectionProposalKey, LeaderElectionProposal, \
    LeaderElectionVote, Checkpointer, ExecutionCompleteNotification
from distributed_notebook.tests.utils.session import TestSession

DefaultKernelId: str = "8a275c45-52fc-4390-8a79-9d8e86066a65"
DefaultDate: str = "2024-11-01T15:32:45.123456789Z"


# TODO (create the following unit tests):
# - Receiving vote(s) for future election before receiving own call to yield_request or execute_request
# - Receiving additional vote(s) doesn't cause anything to break (after the first vote is received)
# - Migration-related unit tests

@pytest_asyncio.fixture
async def kernel(
        hdfs_namenode_hostname: str = "127.0.0.1:10000",
        kernel_id: str= DefaultKernelId,
        smr_port: int = 8000,
        smr_node_id: int = 1,
        smr_nodes: int = [],
        smr_join: bool = False,
        should_register_with_local_daemon: bool = False,
        pod_name: str = "TestPod",
        node_name: str = "TestNode",
        debug_port: int = -1,
        **kwargs
) -> DistributedKernel:
    keyword_args = {
        "hdfs_namenode_hostname": hdfs_namenode_hostname,
        "kernel_id": kernel_id,
        "smr_port": smr_port,
        "smr_node_id": smr_node_id,
        "smr_nodes": smr_nodes,
        "smr_join": smr_join,
        "should_register_with_local_daemon": should_register_with_local_daemon,
        "pod_name": pod_name,
        "node_name": node_name,
        "debug_port": debug_port,
    }

    keyword_args.update(kwargs)

    os.environ.setdefault("PROMETHEUS_METRICS_PORT", "-1")
    kernel: DistributedKernel = DistributedKernel(**keyword_args)
    kernel.num_replicas = 3
    kernel.should_read_data_from_hdfs = False
    kernel.deployment_mode = "DOCKER_SWARM"
    kernel.session = TestSession()
    kernel.store = "/"
    kernel.prometheus_port = -1
    kernel.debug_port = -1
    kernel.kernel_id = DefaultKernelId

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


def assert_election_success(election: Election):
    assert election.code_execution_completed_successfully
    assert election.voting_phase_completed_successfully
    assert election.election_finished_event.is_set()
    assert election.completion_reason == ExecutionCompleted


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
            "code": "a = 1",
            "stop_on_error": False,
        },
        "metadata": {}
    }


async def mocked_sync(execution_ast, source: Optional[str] = None, checkpointer: Optional[Checkpointer] = None) -> bool:
    print(
        f"\nMocked Synchronizer::sync called with execution_ast={execution_ast}, source={source}, checkpointer={checkpointer}")
    await asyncio.sleep(0.25)
    return True


def propose(raftLog: RaftLog, proposedValue: LeaderElectionProposal, election: Election,
            execute_request_task: asyncio.Task[any], expected_num_proposals: int = -1,
            expected_attempt_number: int = 1):
    if expected_num_proposals == -1:
        expected_num_proposals = election.num_proposals_received + 1

    valueId: str = ""
    if proposedValue.proposer_id == 1:
        valueId = proposedValue.id

    raftLog._valueCommittedCallback(proposedValue, sys.getsizeof(proposedValue), valueId)

    if election.num_lead_proposals_received > 0:
        assert (raftLog.decide_election_future is not None)

    # Check that everything is updated correctly now that we've received a proposal.
    assert (election.num_proposals_received == expected_num_proposals)
    assert (election.proposals.get(proposedValue.proposer_id) is not None)
    assert (election.proposals.get(proposedValue.proposer_id) == proposedValue)
    assert (len(raftLog._proposed_values) == 1)

    assert (election.term_number in raftLog._proposed_values)
    assert (raftLog._proposed_values.get(election.term_number) is not None)

    # Make sure proposals from all attempts are in there
    if raftLog.node_id == proposedValue.proposer_id:
        innerMap: OrderedDict[int, LeaderElectionProposal] = raftLog._proposed_values.get(election.term_number)
        assert (innerMap is not None)
        assert (len(innerMap) == expected_attempt_number)

        for attempt_num in range(1, expected_attempt_number + 1):
            assert (attempt_num in innerMap)

        assert (innerMap[expected_attempt_number] == proposedValue)

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


def propose_vote(
        raftLog: RaftLog,
        vote: LeaderElectionVote,
        leading_future: asyncio.Future[int],
        election: Election,
        election_decision_future: asyncio.Future[LeaderElectionVote],
        expected_winner_id: int = -1,
        expected_attempt_number: int = -1,
        expected_election_term: int = -1
):
    if expected_winner_id == -1:
        expected_winner_id = vote.proposed_node_id

    if expected_attempt_number == -1:
        expected_attempt_number = 1

    if expected_election_term == -1:
        expected_election_term = election.term_number

    assert election_decision_future.done()
    proposedVote: LeaderElectionVote = election_decision_future.result()
    assert vote is not None
    assert vote == proposedVote
    assert vote.election_term == expected_election_term
    assert vote.proposer_id == 1
    assert vote.proposed_node_id == expected_winner_id
    assert vote.attempt_number == expected_attempt_number

    assert election.is_active
    assert election.voting_phase_completed_successfully == False
    assert leading_future is not None
    assert leading_future.done() == False

    voteId: str = ""
    if vote.proposer_id == 1:
        voteId = vote.id

    raftLog._valueCommittedCallback(vote, sys.getsizeof(vote), voteId)

    assert election.voting_phase_completed_successfully == True
    assert election.winner == expected_winner_id


async def test_propose_lead_and_win_no_asserts(kernel: DistributedKernel, execute_request: dict[str, any]):
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

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)
    leading_future: asyncio.Future[int] = raftLog._leading_future

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

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        print(f"\n\nMocked RaftLog::_append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             _append_execution_end_notification=mocked_raftlog_append_execution_end_notification):

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=1,
                                                                               attempt_number=1)
        propose(raftLog, leadProposalFromNode3, election, execute_request_task)

        try:
            proposedVote: LeaderElectionVote = await asyncio.wait_for(vote_proposal_future, 5)
        except TimeoutError:
            print("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                print("\n\n\n")

            assert False

        print(f"Got proposed vote: {proposedVote}")

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(leading_future, 5)
        except TimeoutError:
            print("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")

        print("\"Leading\" future should be done now.")
        wait, leading = raftLog._is_leading(1)

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
        assert_election_success(election)


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_propose_lead_and_win(kernel: DistributedKernel, execute_request: dict[str, any]):
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

        assert raftLog._append_execution_end_notification is not None
        assert kernel.synclog._append_execution_end_notification is not None
        assert kernel.synchronizer._synclog._append_execution_end_notification is not None

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=1,
                                                                               attempt_number=1)
        propose(raftLog, leadProposalFromNode3, election, execute_request_task)

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
        assert (election.num_lead_proposals_received == 3)
        assert (election.num_yield_proposals_received == 0)
        assert (election.proposals.get(1) == proposedValue)
        assert (election.proposals.get(2) == leadProposalFromNode2)
        assert (election.proposals.get(3) == leadProposalFromNode3)
        assert (len(raftLog._proposed_values) == 1)
        assert election.is_active
        assert election.voting_phase_completed_successfully == False

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future)

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

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == 1
            assert proposal.is_lead

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
        assert_election_success(election)


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_propose_lead_and_lose(kernel: DistributedKernel, execute_request: dict[str, any]):
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

    # Call "value committed" handler again for the 2nd proposal.
    leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                           proposer_id=2,
                                                                           election_term=1,
                                                                           attempt_number=1)
    propose(raftLog, leadProposalFromNode2, election, execute_request_task)

    propose(raftLog, proposedValue, election, execute_request_task)

    vote_proposal_future: asyncio.Future[LeaderElectionVote] = loop.create_future()

    async def mocked_append_election_vote(*args, **kwargs):
        print(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog._election_decision_future
    assert (election_decision_future is not None)

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote):

        assert raftLog._append_execution_end_notification is not None
        assert kernel.synclog._append_execution_end_notification is not None
        assert kernel.synchronizer._synclog._append_execution_end_notification is not None

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=1,
                                                                               attempt_number=1)
        propose(raftLog, leadProposalFromNode3, election, execute_request_task)

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
        assert (election.num_lead_proposals_received == 3)
        assert (election.num_yield_proposals_received == 0)
        assert (election.proposals.get(1) == proposedValue)
        assert (election.proposals.get(2) == leadProposalFromNode2)
        assert (election.proposals.get(3) == leadProposalFromNode3)
        assert (len(raftLog._proposed_values) == 1)
        assert election.is_active
        assert election.voting_phase_completed_successfully == False

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(leading_future, 5)
        except TimeoutError:
            print("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        print("\"Leading\" future should be done now.")
        assert leading_future.done() == True
        assert raftLog.leader_id == 2
        assert raftLog.leader_term == 1
        wait, leading = raftLog._is_leading(1)
        assert wait == False
        assert leading == False

        await asyncio.sleep(0.5)

        notification: ExecutionCompleteNotification = ExecutionCompleteNotification(proposer_id=2, election_term=1)

        raftLog._valueCommittedCallback(notification, sys.getsizeof(notification), "")

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            print("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                print("\n\n\n")

            assert False

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == 1
            assert proposal.is_lead

        assert execute_request_task.done()
        assert_election_success(election)


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_propose_yield_and_lose(kernel: DistributedKernel, execute_request: dict[str, any]):
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
        execute_request_task: asyncio.Task[any] = loop.create_task(kernel.yield_request(None, [], execute_request))
        proposedValue: LeaderElectionProposal = await election_proposal_future

    # Check that the kernel proposed a LEAD value.
    assert (proposedValue.key == str(ElectionProposalKey.YIELD))
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

    # Call "value committed" handler again for the 2nd proposal.
    leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                           proposer_id=2,
                                                                           election_term=1,
                                                                           attempt_number=1)
    propose(raftLog, leadProposalFromNode2, election, execute_request_task)

    propose(raftLog, proposedValue, election, execute_request_task)

    vote_proposal_future: asyncio.Future[LeaderElectionVote] = loop.create_future()

    async def mocked_append_election_vote(*args, **kwargs):
        print(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog._election_decision_future
    assert (election_decision_future is not None)

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote):

        assert raftLog._append_execution_end_notification is not None
        assert kernel.synclog._append_execution_end_notification is not None
        assert kernel.synchronizer._synclog._append_execution_end_notification is not None

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=1,
                                                                               attempt_number=1)
        propose(raftLog, leadProposalFromNode3, election, execute_request_task)

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
        assert (election.num_lead_proposals_received == 2)
        assert (election.num_yield_proposals_received == 1)
        assert (election.proposals.get(1) == proposedValue)
        assert (election.proposals.get(2) == leadProposalFromNode2)
        assert (election.proposals.get(3) == leadProposalFromNode3)
        assert (len(raftLog._proposed_values) == 1)
        assert election.is_active
        assert election.voting_phase_completed_successfully == False

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(leading_future, 5)
        except TimeoutError:
            print("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        print("\"Leading\" future should be done now.")
        assert leading_future.done() == True
        assert raftLog.leader_id == 2
        assert raftLog.leader_term == 1
        wait, leading = raftLog._is_leading(1)
        assert wait == False
        assert leading == False

        await asyncio.sleep(0.5)

        notification: ExecutionCompleteNotification = ExecutionCompleteNotification(proposer_id=2, election_term=1)

        raftLog._valueCommittedCallback(notification, sys.getsizeof(notification), "")

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


def assert_election_failed(
        election: Election,
        execute_request_task: asyncio.Task[any],
        election_decision_future: asyncio.Future[LeaderElectionVote],
        expected_attempt_number: int = 1,
        expected_term_number: int = 1,
        expected_proposer_id: int = 1,
        expected_proposals_received: int = 3,
):
    """
    Checks the election for valid failure state.
    """
    assert election_decision_future.done()
    assert election.is_in_failed_state
    assert election.num_proposals_received == expected_proposals_received
    assert not election.code_execution_completed_successfully
    assert not election.voting_phase_completed_successfully
    assert election.election_finished_event.is_set()
    assert election.is_active == False
    assert election.is_inactive == False
    assert election.winner_id == -1
    assert election.completion_reason == AllReplicasProposedYield

    print(f"Election {election.term_number} has current attempt number = {election.current_attempt_number}.")
    print(f"Election proposals (quantity: {len(election.proposals)}):")
    for _, proposal in election.proposals.items():
        print(f"Proposal: {proposal}")
        assert proposal.election_term == expected_term_number
        assert proposal.attempt_number == expected_attempt_number
        assert proposal.is_yield

    failure_vote: LeaderElectionVote = election_decision_future.result()
    assert failure_vote is not None
    assert failure_vote.proposed_node_id == -1
    assert failure_vote.proposer_id == expected_proposer_id
    assert failure_vote.election_term == expected_term_number
    assert failure_vote.attempt_number == expected_attempt_number

    assert execute_request_task.done()


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_election_fails_when_all_propose_yield(kernel: DistributedKernel, execute_request: dict[str, any]):
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
        execute_request_task: asyncio.Task[any] = loop.create_task(kernel.yield_request(None, [], execute_request))
        proposedValue: LeaderElectionProposal = await election_proposal_future

    # Check that the kernel proposed a LEAD value.
    assert (proposedValue.key == str(ElectionProposalKey.YIELD))
    assert (proposedValue.proposer_id == kernel.smr_node_id)
    assert (proposedValue.election_term == 1)

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)
    assert (election is not None)
    assert (election.term_number == 1)
    assert (election.current_attempt_number == 0)
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

    # Call "value committed" handler again for the 2nd proposal.
    yieldProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                            proposer_id=2,
                                                                            election_term=1,
                                                                            attempt_number=1)

    propose(raftLog, proposedValue, election, execute_request_task)

    propose(raftLog, yieldProposalFromNode2, election, execute_request_task)

    assert raftLog._append_execution_end_notification is not None
    assert kernel.synclog._append_execution_end_notification is not None
    assert kernel.synchronizer._synclog._append_execution_end_notification is not None

    yieldProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                            proposer_id=3,
                                                                            election_term=1,
                                                                            attempt_number=1)
    propose(raftLog, yieldProposalFromNode3, election, execute_request_task)

    # TODO: There's actually another proposal that needs to happen -- a node proposing "FAILURE"
    assert election.num_proposals_accepted == 3
    assert election.num_yield_proposals_received == 3
    assert election.num_lead_proposals_received == 0

    election_decision_future: asyncio.Future[LeaderElectionVote] = raftLog._election_decision_future
    assert election_decision_future is not None

    try:
        await asyncio.wait_for(election_decision_future, 5)
    except TimeoutError:
        print("[ERROR] \"election_decision\" future was not resolved.")

        for task in asyncio.all_tasks():
            asyncio.Task.print_stack(task)
            print("\n\n\n")

        assert False

    assert_election_failed(election, execute_request_task, election_decision_future, expected_proposer_id=1,
                           expected_term_number=1, expected_attempt_number=1, expected_proposals_received=3)

    assert election.election_finished_event.is_set()
    assert election.is_active == False
    assert election.is_inactive == False
    assert election.winner_id == -1

    assert not election.code_execution_completed_successfully
    assert not election.voting_phase_completed_successfully
    assert election.election_finished_event.is_set()
    assert election.completion_reason == AllReplicasProposedYield


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_all_propose_yield_and_win_second_round(kernel: DistributedKernel, execute_request: dict[str, any]):
    """
    Simulate a first-round election where all replicas proposed YIELD, followed by a second round where
    replica #1 proposes 'LEAD'.

    Note: we are NOT simulating the actual migration in this unit test
    """
    print(f"Testing execute request with kernel {kernel} and execute request {execute_request}")

    ###############
    # FIRST ROUND #
    ###############

    raftLog: RaftLog = kernel.synclog

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        print(f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(kernel.yield_request(None, [], execute_request))
        proposedValue: LeaderElectionProposal = await election_proposal_future

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)

    # Call "value committed" handler again for the 2nd proposal.
    yieldProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                            proposer_id=2,
                                                                            election_term=1,
                                                                            attempt_number=1)

    propose(raftLog, proposedValue, election, execute_request_task)

    propose(raftLog, yieldProposalFromNode2, election, execute_request_task)

    yieldProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                            proposer_id=3,
                                                                            election_term=1,
                                                                            attempt_number=1)
    propose(raftLog, yieldProposalFromNode3, election, execute_request_task, expected_num_proposals=3,
            expected_attempt_number=1)

    election_decision_future: asyncio.Future[LeaderElectionVote] = raftLog._election_decision_future

    try:
        await asyncio.wait_for(election_decision_future, 5)
    except TimeoutError:
        print("[ERROR] \"election_decision\" future was not resolved.")

        for task in asyncio.all_tasks():
            asyncio.Task.print_stack(task)
            print("\n\n\n")

        assert False

    assert_election_failed(election, execute_request_task, election_decision_future, expected_proposer_id=1,
                           expected_term_number=1, expected_attempt_number=1, expected_proposals_received=3)

    ################
    # SECOND ROUND #
    ################

    assert election.election_finished_event.is_set()

    election_proposal_future = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        print(f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task = loop.create_task(kernel.execute_request(None, [], execute_request))
        proposedValue = await election_proposal_future

    # Check that the kernel proposed a LEAD value.
    assert (proposedValue.key == str(ElectionProposalKey.LEAD))
    assert (proposedValue.proposer_id == kernel.smr_node_id)
    assert (proposedValue.election_term == 1)
    assert (proposedValue.attempt_number == 2)

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)
    assert (election is not None)
    assert (election.term_number == 1)
    assert (election.current_attempt_number == 1)  # Hasn't been committed yet, so still 1
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
    assert (len(innerMap) == 2)
    assert (1 in innerMap)
    assert (2 in innerMap)
    assert (innerMap[2] == proposedValue)

    leading_future: asyncio.Future[int] = raftLog._leading_future
    assert leading_future is not None
    assert leading_future.done() == False

    propose(raftLog, proposedValue, election, execute_request_task, expected_attempt_number=2)

    assert (election.current_attempt_number == 2)

    # Call "value committed" handler again for the 2nd proposal.
    leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                           proposer_id=2,
                                                                           election_term=1,
                                                                           attempt_number=2)
    propose(raftLog, leadProposalFromNode2, election, execute_request_task, expected_attempt_number=2)

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

        assert raftLog._append_execution_end_notification is not None
        assert kernel.synclog._append_execution_end_notification is not None
        assert kernel.synchronizer._synclog._append_execution_end_notification is not None

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=1,
                                                                               attempt_number=2)
        propose(raftLog, leadProposalFromNode3, election, execute_request_task, expected_attempt_number=2)

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
        assert (election.num_lead_proposals_received == 3)
        assert (election.num_yield_proposals_received == 0)
        assert (election.proposals.get(1) == proposedValue)
        assert (election.proposals.get(2) == leadProposalFromNode2)
        assert (election.proposals.get(3) == leadProposalFromNode3)
        assert (len(raftLog._proposed_values) == 1)
        assert election.is_active
        assert not election.voting_phase_completed_successfully

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future,
                     expected_attempt_number=2)

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

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == 2
            assert proposal.is_lead

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
        assert_election_success(election)


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_fail_election_nine_times_then_win(kernel: DistributedKernel, execute_request: dict[str, any]):
    raftLog: RaftLog = kernel.synclog

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    NUM_FAILURES: int = 9

    for i in range(0, NUM_FAILURES):
        print(f"\n\n\nElection Round #{i}\n")
        election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

        async def mocked_append_election_proposal(*args, **kwargs):
            print(f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
            election_proposal_future.set_result(args[1])

        with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                               mocked_append_election_proposal):
            execute_request_task: asyncio.Task[any] = loop.create_task(kernel.yield_request(None, [], execute_request))
            proposedValue: LeaderElectionProposal = await election_proposal_future

        # Check that the kernel created an election, but that no proposals were received yet.
        election: Election = kernel.synclog.get_election(1)

        # Call "value committed" handler again for the 2nd proposal.
        yieldProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                                proposer_id=2,
                                                                                election_term=1,
                                                                                attempt_number=i + 1)

        propose(raftLog, proposedValue, election, execute_request_task, expected_num_proposals=1,
                expected_attempt_number=i + 1)

        propose(raftLog, yieldProposalFromNode2, election, execute_request_task, expected_num_proposals=2,
                expected_attempt_number=i + 1)

        yieldProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                                proposer_id=3,
                                                                                election_term=1,
                                                                                attempt_number=i + 1)
        propose(raftLog, yieldProposalFromNode3, election, execute_request_task, expected_num_proposals=3,
                expected_attempt_number=i + 1)

        election_decision_future: asyncio.Future[LeaderElectionVote] = raftLog._election_decision_future

        try:
            await asyncio.wait_for(election_decision_future, 5)
        except TimeoutError:
            print("[ERROR] \"election_decision\" future was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                print("\n\n\n")

            assert False

        assert_election_failed(election, execute_request_task, election_decision_future, expected_proposer_id=1,
                               expected_term_number=1, expected_attempt_number=i + 1, expected_proposals_received=3)

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)
    assert election.election_finished_event.is_set()

    election_proposal_future = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        print(f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task = loop.create_task(kernel.execute_request(None, [], execute_request))
        proposedValue = await election_proposal_future

    # Check that the kernel proposed a LEAD value.
    assert (proposedValue.key == str(ElectionProposalKey.LEAD))
    assert (proposedValue.proposer_id == kernel.smr_node_id)
    assert (proposedValue.election_term == 1)
    assert (proposedValue.attempt_number == NUM_FAILURES + 1)

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)
    assert (election is not None)
    assert (election.term_number == 1)
    assert (election.current_attempt_number == NUM_FAILURES)  # Hasn't been committed yet, so still NUM_FAILURES
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
    assert (len(innerMap) == NUM_FAILURES + 1)
    for i in range(1, NUM_FAILURES + 2):
        assert i in innerMap
    assert (innerMap[NUM_FAILURES + 1] == proposedValue)

    leading_future: asyncio.Future[int] = raftLog._leading_future
    assert leading_future is not None
    assert leading_future.done() == False

    propose(raftLog, proposedValue, election, execute_request_task, expected_attempt_number=NUM_FAILURES + 1)

    assert (election.current_attempt_number == NUM_FAILURES + 1)

    # Call "value committed" handler again for the 2nd proposal.
    leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                           proposer_id=2,
                                                                           election_term=1,
                                                                           attempt_number=NUM_FAILURES + 1)
    propose(raftLog, leadProposalFromNode2, election, execute_request_task, expected_attempt_number=NUM_FAILURES + 1)

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

        assert raftLog._append_execution_end_notification is not None
        assert kernel.synclog._append_execution_end_notification is not None
        assert kernel.synchronizer._synclog._append_execution_end_notification is not None

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=1,
                                                                               attempt_number=NUM_FAILURES + 1)
        propose(raftLog, leadProposalFromNode3, election, execute_request_task,
                expected_attempt_number=NUM_FAILURES + 1)

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
        assert (election.num_lead_proposals_received == 3)
        assert (election.num_yield_proposals_received == 0)
        assert (election.proposals.get(1) == proposedValue)
        assert (election.proposals.get(2) == leadProposalFromNode2)
        assert (election.proposals.get(3) == leadProposalFromNode3)
        assert (len(raftLog._proposed_values) == 1)
        assert election.is_active
        assert not election.voting_phase_completed_successfully

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future,
                     expected_attempt_number=NUM_FAILURES + 1)

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

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == NUM_FAILURES + 1
            assert proposal.is_lead

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
        assert_election_success(election)


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_election_success_after_timing_out(kernel: DistributedKernel, execute_request: dict[str, any]):
    """
    Test that an election which only receives 1/3 proposals will succeed after the timeout period elapses,
    if that one proposal is a LEAD proposal.
    """
    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    kernel.election_timeout_seconds = 1.0
    raftLog._election_timeout_seconds = 1.0

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

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)
    leading_future: asyncio.Future[int] = raftLog._leading_future

    propose(raftLog, proposedValue, election, execute_request_task)

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
        assert (election.num_proposals_received == 1)
        assert (election.num_lead_proposals_received == 1)
        assert (election.num_yield_proposals_received == 0)
        assert (election.proposals.get(1) == proposedValue)
        assert (len(raftLog._proposed_values) == 1)
        assert election.is_active
        assert election.voting_phase_completed_successfully == False

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future)

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

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == 1
            assert proposal.is_lead

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
        assert_election_success(election)


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_election_loss_buffer_one_lead_proposal(kernel: DistributedKernel, execute_request: dict[str, any]):
    """
    The election succeeds like normal after buffering one of the proposals.

    Node #1 does not win the election, though. Node #2 wins the election in this test.
    """
    print(f"Testing execute request with kernel {kernel} and execute request {execute_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert (synchronizer is not None)
    assert (raftLog is not None)

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    # Call "value committed" handler again for the 2nd proposal.
    leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                           proposer_id=2,
                                                                           election_term=1,
                                                                           attempt_number=1)

    raftLog._valueCommittedCallback(leadProposalFromNode2, sys.getsizeof(leadProposalFromNode2), "")

    assert raftLog._buffered_proposals is not None
    assert len(raftLog._buffered_proposals) == 1

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
    assert (election.num_proposals_received == 1)  # We've not committed the second proposal, so still just 1
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

    # Now commit our (node 1's) proposal.
    propose(raftLog, proposedValue, election, execute_request_task)

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

        assert raftLog._append_execution_end_notification is not None
        assert kernel.synclog._append_execution_end_notification is not None
        assert kernel.synchronizer._synclog._append_execution_end_notification is not None

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=1,
                                                                               attempt_number=1)
        propose(raftLog, leadProposalFromNode3, election, execute_request_task)

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
        assert (election.num_lead_proposals_received == 3)
        assert (election.num_yield_proposals_received == 0)
        assert (election.proposals.get(1) == proposedValue)
        assert (election.proposals.get(2) == leadProposalFromNode2)
        assert (election.proposals.get(3) == leadProposalFromNode3)
        assert (len(raftLog._proposed_values) == 1)
        assert election.is_active
        assert election.voting_phase_completed_successfully == False

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(leading_future, 5)
        except TimeoutError:
            print("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        print("\"Leading\" future should be done now.")
        assert leading_future.done() == True
        assert raftLog.leader_id == 2
        assert raftLog.leader_term == 1
        wait, leading = raftLog._is_leading(1)
        assert wait == False
        assert leading == False

        await asyncio.sleep(0.5)

        notification: ExecutionCompleteNotification = ExecutionCompleteNotification(proposer_id=2, election_term=1)

        raftLog._valueCommittedCallback(notification, sys.getsizeof(notification), "")

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


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_election_win_buffer_one_yield_proposal(kernel: DistributedKernel, execute_request: dict[str, any]):
    """
    The election succeeds like normal after buffering one of the proposals.

    Node #1 wins the election, as the buffered proposal is a YIELD proposal.
    """
    print(f"Testing execute request with kernel {kernel} and execute request {execute_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert (synchronizer is not None)
    assert (raftLog is not None)

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    # Call "value committed" handler again for the 2nd proposal.
    leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                           proposer_id=2,
                                                                           election_term=1,
                                                                           attempt_number=1)

    raftLog._valueCommittedCallback(leadProposalFromNode2, sys.getsizeof(leadProposalFromNode2), "")

    assert raftLog._buffered_proposals is not None
    assert len(raftLog._buffered_proposals) == 1

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
    assert (election.num_proposals_received == 1)  # We've not committed the second proposal, so still just 1
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

    # Now commit our (node 1's) proposal.
    propose(raftLog, proposedValue, election, execute_request_task)

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

        assert raftLog._append_execution_end_notification is not None
        assert kernel.synclog._append_execution_end_notification is not None
        assert kernel.synchronizer._synclog._append_execution_end_notification is not None

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=1,
                                                                               attempt_number=1)
        propose(raftLog, leadProposalFromNode3, election, execute_request_task)

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
        assert (election.num_lead_proposals_received == 2)
        assert (election.num_yield_proposals_received == 1)
        assert (election.proposals.get(1) == proposedValue)
        assert (election.proposals.get(2) == leadProposalFromNode2)
        assert (election.proposals.get(3) == leadProposalFromNode3)
        assert (len(raftLog._proposed_values) == 1)
        assert election.is_active
        assert election.voting_phase_completed_successfully == False

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future)

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

        num_lead: int = 0
        num_yield: int = 0
        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == 1

            if proposal.is_lead:
                num_lead += 1
            elif proposal.is_yield:
                num_yield += 1

        assert num_lead == 2
        assert num_yield == 1

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
        assert_election_success(election)


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_catch_up_after_migration(kernel: DistributedKernel, execute_request: dict[str, any]):
    """
    The election succeeds like normal after buffering two of the proposals.
    """
    print(f"Testing execute request with kernel {kernel} and execute request {execute_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        print(f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(kernel.yield_request(None, [], execute_request))
        proposedValue: LeaderElectionProposal = await election_proposal_future

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)

    # Call "value committed" handler again for the 2nd proposal.
    yieldProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                            proposer_id=2,
                                                                            election_term=1,
                                                                            attempt_number=1)

    propose(raftLog, proposedValue, election, execute_request_task)
    propose(raftLog, yieldProposalFromNode2, election, execute_request_task)

    yieldProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                            proposer_id=3,
                                                                            election_term=1,
                                                                            attempt_number=1)
    propose(raftLog, yieldProposalFromNode3, election, execute_request_task)

    election_decision_future: asyncio.Future[LeaderElectionVote] = raftLog._election_decision_future
    assert election_decision_future is not None

    try:
        await asyncio.wait_for(election_decision_future, 5)
    except TimeoutError:
        print("[ERROR] \"election_decision\" future was not resolved.")

        for task in asyncio.all_tasks():
            asyncio.Task.print_stack(task)
            print("\n\n\n")

        assert False

    assert_election_failed(election, execute_request_task, election_decision_future, expected_proposer_id=1,
                           expected_term_number=1, expected_attempt_number=1, expected_proposals_received=3)

    close_future: asyncio.Future[int] = loop.create_future()
    def mocked_raftlog_close(*args, **kwargs):
        print(f"\nMocked RaftLog::close called with args {args} and kwargs {kwargs}.", flush = True)
        close_future.set_result(1)

    write_data_dir_to_hdfs_future: asyncio.Future[bytes] = loop.create_future()
    async def mocked_raftlog_write_data_dir_to_hdfs(*args, **kwargs):
        print(f"\nMocked RaftLog::write_data_dir_to_hdfs called with args {args} and kwargs {kwargs}.", flush = True)

        assert isinstance(args[0], RaftLog)

        serialized_state: bytes = args[0]._get_serialized_state()

        write_data_dir_to_hdfs_future.set_result(serialized_state)

        return "/mocked/hdfs/path/does/not/actually/exist"

    # with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "close", mocked_raftlog_close):
    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             close=mocked_raftlog_close,
                             write_data_dir_to_hdfs=mocked_raftlog_write_data_dir_to_hdfs):
        await kernel.prepare_to_migrate_request(None, [], {})

    assert close_future.done()
    assert close_future.result() == 1

    assert write_data_dir_to_hdfs_future.done()

    test_session: TestSession = kernel.session
    assert test_session is not None
    assert isinstance(test_session, TestSession)

    assert "prepare_to_migrate_reply" in test_session.message_types_sent
    assert "execute_input" in test_session.message_types_sent
    assert "execute_reply" in test_session.message_types_sent

    assert test_session.num_send_calls == 3
    assert len(test_session.message_types_sent) == 3

    serialized_state: bytes = write_data_dir_to_hdfs_future.result()
    assert serialized_state is not None
    assert isinstance(serialized_state, bytes)

    state: Optional[dict[str, any]] = None
    try:
        state = pickle.loads(serialized_state)
    except Exception as ex:
        print(f"Failed to unpickle serialized state: {ex}")
        assert ex is None # Will necessarily fail

    assert state is not None
    assert isinstance(state, dict)

    # TODO: Apply serialized state. Recreate Kernel.