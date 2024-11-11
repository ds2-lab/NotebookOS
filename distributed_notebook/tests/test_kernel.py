import asyncio
import logging
import os
import shutil
import sys
from collections import OrderedDict
from typing import Optional
from unittest import mock

import pytest
import pytest_asyncio
from ipykernel.control import ControlThread

import distributed_notebook.sync.raft_log
from distributed_notebook.kernel.kernel import DistributedKernel
from distributed_notebook.sync import Synchronizer, RaftLog
from distributed_notebook.sync.election import Election, ExecutionCompleted, AllReplicasProposedYield
from distributed_notebook.sync.log import ElectionProposalKey, LeaderElectionProposal, \
    LeaderElectionVote, Checkpointer, ExecutionCompleteNotification, SynchronizedValue, KEY_CATCHUP
from distributed_notebook.tests.utils.lognode import SpoofedLogNode
from distributed_notebook.tests.utils.session import SpoofedSession
from distributed_notebook.tests.utils.stream import SpoofedStream

unit_test_logger = logging.getLogger(__name__)
unit_test_logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
fm = logging.Formatter(fmt="%(asctime)s [%(levelname)s] %(name)s [%(threadName)s (%(thread)d)]: %(message)s ")
ch.setLevel(logging.DEBUG)
ch.setFormatter(fm)
unit_test_logger.addHandler(ch)

DefaultKernelId: str = "8a275c45-52fc-4390-8a79-9d8e86066a65"
DefaultDate: str = "2024-11-01T15:32:45.123456789Z"
FakePersistentStorePath: str = "unit-test-persistent-store"

# The /store/ prefix is automatically added by kernels and whatnot.
FullFakePersistentStorePath: str = f"./store/{FakePersistentStorePath}"

# TODO (create the following unit tests):
# - Receiving vote(s) for future election before receiving own call to yield_request or execute_request
# - Receiving additional vote(s) doesn't cause anything to break (after the first vote is received)
# - Migration-related unit tests

CommittedValues: list[SynchronizedValue] = []


def commit_value(raftLog: RaftLog, proposedValue: SynchronizedValue, value_id: str = "", record: bool = True):
    if record:
        CommittedValues.append(proposedValue)

    raftLog._valueCommittedCallback(proposedValue, sys.getsizeof(proposedValue), value_id)


@pytest.fixture(autouse=True)
def before_and_after_test_fixture():
    """
    pytest fixture that will run before and after each unit test.
    """
    # Ensure the persistent store directory does not already exist.
    global CommittedValues

    try:
        unit_test_logger.debug(f"Removing persistent store directory \"{FullFakePersistentStorePath}\".")
        shutil.rmtree(FullFakePersistentStorePath)
        unit_test_logger.debug(f"Removed persistent store directory \"{FullFakePersistentStorePath}\".")
    except FileNotFoundError:
        unit_test_logger.debug(
            f"Persistent store directory \"{FullFakePersistentStorePath}\" did not exist. Nothing to remove.")

    CommittedValues.clear()

    yield

    # Remove the persistent store directory if it was created.
    try:
        unit_test_logger.debug(f"Removing persistent store directory \"{FullFakePersistentStorePath}\".")
        shutil.rmtree(FullFakePersistentStorePath)
        unit_test_logger.debug(f"Removed persistent store directory \"{FullFakePersistentStorePath}\".")
    except FileNotFoundError:
        unit_test_logger.debug(
            f"Persistent store directory \"{FullFakePersistentStorePath}\" did not exist. Nothing to remove.")

    CommittedValues.clear()


def mock_create_log_node(*args, **mock_kwargs):
    unit_test_logger.debug(f"Mocked RaftLog::create_log_node called with args {args} and kwargs {mock_kwargs}.")

    return SpoofedLogNode(**mock_kwargs)


async def create_kernel(
        hdfs_namenode_hostname: str = "127.0.0.1:10000",
        kernel_id: str = DefaultKernelId,
        smr_port: int = 8000,
        smr_node_id: int = 1,
        smr_nodes: int = [],
        smr_join: bool = False,
        should_register_with_local_daemon: bool = False,
        pod_name: str = "TestPod",
        node_name: str = "TestNode",
        debug_port: int = -1,
        storage_base: str = "./",
        init_persistent_store: bool = True,
        call_start: bool = True,
        local_tcp_server_port: int = -1,
        persistent_id: Optional[str] = None,
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
        "storage_base": storage_base,
        "local_tcp_server_port": local_tcp_server_port,
        "persistent_id": persistent_id,
    }

    keyword_args.update(kwargs)

    os.environ.setdefault("PROMETHEUS_METRICS_PORT", "-1")
    kernel: DistributedKernel = DistributedKernel(**keyword_args)
    kernel.control_thread = ControlThread(daemon=True)
    kernel.control_stream = SpoofedStream()
    kernel.control_thread.start()
    kernel.num_replicas = 3
    kernel.should_read_data_from_hdfs = False
    kernel.deployment_mode = "DOCKER_SWARM"
    kernel.session = SpoofedSession()
    # kernel.store = FakePersistentStorePath
    kernel.prometheus_port = -1
    kernel.debug_port = -1
    kernel.kernel_id = DefaultKernelId

    # kernel.synclog = RaftLog(kernel.smr_node_id,
    #                          base_path=kernel.store,
    #                          kernel_id=kernel.kernel_id,
    #                          num_replicas=kernel.num_replicas,
    #                          hdfs_hostname=kernel.hdfs_namenode_hostname,
    #                          should_read_data_from_hdfs=kernel.should_read_data_from_hdfs,
    #                          peer_addrs=[],
    #                          peer_ids=[],
    #                          join=kernel.smr_join,
    #                          debug_port=kernel.debug_port,
    #                          report_error_callback=kernel.report_error,
    #                          send_notification_func=kernel.send_notification,
    #                          hdfs_read_latency_callback=kernel.hdfs_read_latency_callback,
    #                          deploymentMode=kernel.deployment_mode)
    #
    # kernel.synchronizer = Synchronizer(kernel.synclog, module=None, opts=CHECKPOINT_AUTO)

    if call_start:
        kernel.start()

    if init_persistent_store:
        with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "create_log_node", mock_create_log_node):
            # await kernel.init_persistent_store_with_persistent_id(FakePersistentStorePath)
            unit_test_logger.debug("Calling init_persistent_store from create_kernel fixture.")
            code = "persistent_id = \"%s\"" % FakePersistentStorePath
            await kernel.init_persistent_store(code)

    # Need to yield here rather than return, or else we'll go out-of-scope,
    # and the mock that happens above won't work.
    return kernel


@pytest_asyncio.fixture
async def kernel(
        hdfs_namenode_hostname: str = "127.0.0.1:10000",
        kernel_id: str = DefaultKernelId,
        smr_port: int = 8000,
        smr_node_id: int = 1,
        smr_nodes: list[int] = [],
        smr_join: bool = False,
        should_register_with_local_daemon: bool = False,
        pod_name: str = "TestPod",
        node_name: str = "TestNode",
        debug_port: int = -1,
        **kwargs
) -> DistributedKernel:
    return await create_kernel(
        hdfs_namenode_hostname=hdfs_namenode_hostname,
        kernel_id=kernel_id,
        smr_port=smr_port,
        smr_node_id=smr_node_id,
        smr_nodes=smr_nodes,
        smr_join=smr_join,
        should_register_with_local_daemon=should_register_with_local_daemon,
        pod_name=pod_name,
        node_name=node_name,
        debug_port=debug_port,
    )


def assert_election_success(election: Election):
    assert election.code_execution_completed_successfully
    assert election.voting_phase_completed_successfully
    assert election.election_finished_event.is_set()
    assert election.completion_reason == ExecutionCompleted


@pytest.fixture
def execution_request():
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


async def mocked_sync(execution_ast, source: Optional[str] = None,
                      checkpointer: Optional[Checkpointer] = None) -> bool:
    unit_test_logger.debug(
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

    commit_value(raftLog, proposedValue, value_id=valueId)

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
    assert (raftLog.election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog.election_decision_future.done() == False)
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

    commit_value(raftLog, vote, value_id=voteId)

    assert election.voting_phase_completed_successfully == True
    assert election.winner == expected_winner_id


async def example(kernel: DistributedKernel, execution_request: dict[str, any]):
    """
    propose_lead_and_win_no_asserts
    """
    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert (synchronizer is not None)
    assert (raftLog is not None)

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(
            kernel.execute_request(None, [], execution_request))
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
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        unit_test_logger.debug(
            f"\n\nMocked RaftLog::_append_execution_end_notification called with args {args} and kwargs {kwargs}.")
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
            unit_test_logger.debug("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        unit_test_logger.debug(f"Got proposed vote: {proposedVote}")

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(leading_future, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")

        unit_test_logger.debug("\"Leading\" future should be done now.")
        wait, leading = raftLog._is_leading(1)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execution_done_future, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execution_done\" future was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execution_done_future.done()

        notification: ExecutionCompleteNotification = execution_done_future.result()
        unit_test_logger.debug(f"Got ExecutionCompleteNotification: {notification}")

        commit_value(raftLog, notification, value_id=notification.id)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execute_request_task.done()
        assert_election_success(election)


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_propose_lead_and_win(kernel: DistributedKernel, execution_request: dict[str, any]):
    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert (synchronizer is not None)
    assert (raftLog is not None)

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(
            kernel.execute_request(None, [], execution_request))
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
    assert (raftLog.election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog.election_decision_future.done() == False)
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
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future
    assert (election_decision_future is not None)

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        unit_test_logger.debug(
            f"\n\nMocked RaftLog::_append_execution_end_notification called with args {args} and kwargs {kwargs}.")
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
            unit_test_logger.debug("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        unit_test_logger.debug(f"Got proposed vote: {proposedVote}")

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
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        unit_test_logger.debug("\"Leading\" future should be done now.")
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
            unit_test_logger.debug("[ERROR] \"execution_done\" future was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execution_done_future.done()

        notification: ExecutionCompleteNotification = execution_done_future.result()
        unit_test_logger.debug(f"Got ExecutionCompleteNotification: {notification}")
        assert notification is not None
        assert notification.proposer_id == 1
        assert notification.election_term == 1

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == 1
            assert proposal.is_lead

        commit_value(raftLog, notification, value_id=notification.id)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execute_request_task.done()
        assert_election_success(election)

    spoofed_session: SpoofedSession = kernel.session
    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert 'smr_ready' in spoofed_session.message_types_sent
    assert 'smr_lead_task' in spoofed_session.message_types_sent
    assert 'status' in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 5
    assert len(spoofed_session.message_types_sent) == 5


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_propose_lead_and_lose(kernel: DistributedKernel, execution_request: dict[str, any]):
    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert (synchronizer is not None)
    assert (raftLog is not None)

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(
            kernel.execute_request(None, [], execution_request))
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
    assert (raftLog.election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog.election_decision_future.done() == False)
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
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future
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
            unit_test_logger.debug("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        unit_test_logger.debug(f"Got proposed vote: {proposedVote}")

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
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        unit_test_logger.debug("\"Leading\" future should be done now.")
        assert leading_future.done() == True
        assert raftLog.leader_id == 2
        assert raftLog.leader_term == 1
        wait, leading = raftLog._is_leading(1)
        assert wait == False
        assert leading == False

        await asyncio.sleep(0.5)

        notification: ExecutionCompleteNotification = ExecutionCompleteNotification(proposer_id=2, election_term=1)

        commit_value(raftLog, notification)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == 1
            assert proposal.is_lead

        assert execute_request_task.done()
        assert_election_success(election)

    spoofed_session: SpoofedSession = kernel.session
    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert 'smr_ready' in spoofed_session.message_types_sent
    assert 'status' in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 4
    assert len(spoofed_session.message_types_sent) == 4


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_propose_yield_and_lose(kernel: DistributedKernel, execution_request: dict[str, any]):
    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert (synchronizer is not None)
    assert (raftLog is not None)

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(kernel.yield_request(None, [], execution_request))
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
    assert (raftLog.election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog.election_decision_future.done() == False)
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
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future
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
            unit_test_logger.debug("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        unit_test_logger.debug(f"Got proposed vote: {proposedVote}")

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
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        unit_test_logger.debug("\"Leading\" future should be done now.")
        assert leading_future.done() == True
        assert raftLog.leader_id == 2
        assert raftLog.leader_term == 1
        wait, leading = raftLog._is_leading(1)
        assert wait == False
        assert leading == False

        await asyncio.sleep(0.5)

        notification: ExecutionCompleteNotification = ExecutionCompleteNotification(proposer_id=2, election_term=1)

        commit_value(raftLog, notification)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execute_request_task.done()

    spoofed_session: SpoofedSession = kernel.session
    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert 'smr_ready' in spoofed_session.message_types_sent
    assert 'status' in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 4
    assert len(spoofed_session.message_types_sent) == 4


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

    unit_test_logger.debug(
        f"Election {election.term_number} has current attempt number = {election.current_attempt_number}.")
    unit_test_logger.debug(f"Election proposals (quantity: {len(election.proposals)}):")
    for _, proposal in election.proposals.items():
        unit_test_logger.debug(f"Proposal: {proposal}")
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
async def test_election_fails_when_all_propose_yield(kernel: DistributedKernel, execution_request: dict[str, any]):
    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert (synchronizer is not None)
    assert (raftLog is not None)

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(kernel.yield_request(None, [], execution_request))
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
    assert (raftLog.election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog.election_decision_future.done() == False)
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

    election_decision_future: asyncio.Future[LeaderElectionVote] = raftLog.election_decision_future
    assert election_decision_future is not None

    try:
        await asyncio.wait_for(election_decision_future, 5)
    except TimeoutError:
        unit_test_logger.debug("[ERROR] \"election_decision\" future was not resolved.")

        for task in asyncio.all_tasks():
            asyncio.Task.print_stack(task)
            unit_test_logger.debug("\n\n\n")

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

    spoofed_session: SpoofedSession = kernel.session
    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert 'smr_ready' in spoofed_session.message_types_sent
    assert 'status' in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 4
    assert len(spoofed_session.message_types_sent) == 4


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_all_propose_yield_and_win_second_round(kernel: DistributedKernel, execution_request: dict[str, any]):
    """
    Simulate a first-round election where all replicas proposed YIELD, followed by a second round where
    replica #1 proposes 'LEAD'.

    Note: we are NOT simulating the actual migration in this unit test
    """
    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")

    ###############
    # FIRST ROUND #
    ###############

    raftLog: RaftLog = kernel.synclog

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(kernel.yield_request(None, [], execution_request))
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

    election_decision_future: asyncio.Future[LeaderElectionVote] = raftLog.election_decision_future

    try:
        await asyncio.wait_for(election_decision_future, 5)
    except TimeoutError:
        unit_test_logger.debug("[ERROR] \"election_decision\" future was not resolved.")

        for task in asyncio.all_tasks():
            asyncio.Task.print_stack(task)
            unit_test_logger.debug("\n\n\n")

        assert False

    assert_election_failed(election, execute_request_task, election_decision_future, expected_proposer_id=1,
                           expected_term_number=1, expected_attempt_number=1, expected_proposals_received=3)

    spoofed_session: SpoofedSession = kernel.session
    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert 'smr_ready' in spoofed_session.message_types_sent
    assert 'status' in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 4
    assert len(spoofed_session.message_types_sent) == 4

    ################
    # SECOND ROUND #
    ################

    assert election.election_finished_event.is_set()

    election_proposal_future = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task = loop.create_task(kernel.execute_request(None, [], execution_request))
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
    assert (raftLog.election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog.election_decision_future.done() == False)
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
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future
    assert (election_decision_future is not None)

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        unit_test_logger.debug(
            f"\n\nMocked RaftLog::_append_execution_end_notification called with args {args} and kwargs {kwargs}.")
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
            unit_test_logger.debug("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        unit_test_logger.debug(f"Got proposed vote: {proposedVote}")

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
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        unit_test_logger.debug("\"Leading\" future should be done now.")
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
            unit_test_logger.debug("[ERROR] \"execution_done\" future was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execution_done_future.done()

        notification: ExecutionCompleteNotification = execution_done_future.result()
        unit_test_logger.debug(f"Got ExecutionCompleteNotification: {notification}")
        assert notification is not None
        assert notification.proposer_id == 1
        assert notification.election_term == 1

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == 2
            assert proposal.is_lead

        commit_value(raftLog, notification, value_id=notification.id)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execute_request_task.done()
        assert_election_success(election)

    spoofed_session: SpoofedSession = kernel.session
    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert 'smr_ready' in spoofed_session.message_types_sent
    assert 'status' in spoofed_session.message_types_sent
    assert 'smr_lead_task' in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 7
    assert len(spoofed_session.message_types_sent) == 7
    assert spoofed_session.message_types_sent_counts["execute_input"] == 2
    assert spoofed_session.message_types_sent_counts["execute_reply"] == 2
    assert spoofed_session.message_types_sent_counts["smr_ready"] == 1
    assert spoofed_session.message_types_sent_counts["status"] == 1
    assert spoofed_session.message_types_sent_counts["smr_lead_task"] == 1


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
async def fail_election(
        kernel: DistributedKernel,
        execution_request: dict[str, any],
        loop: Optional[asyncio.AbstractEventLoop] = None,
        current_election_term: int = 1,
        current_attempt_number: int = 1,
):
    """
    Run the specified Election such that it fails, with all replicas proposing YIELD.

    Args:
        kernel: the "main" kernel replica
        execution_request: the "execute_request" Jupyter kernel message
        loop: the asyncio event loop
        current_election_term: the election term
        current_attempt_number: the attempt number of the election
    """
    if loop is None:
        loop = asyncio.get_running_loop()

    raftLog: RaftLog = kernel.synclog

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        unit_test_logger.debug("Creating yield task")
        execute_request_task: asyncio.Task[any] = loop.create_task(kernel.yield_request(None, [], execution_request))
        unit_test_logger.debug("Awaiting election proposal")

        try:
            proposedValue: LeaderElectionProposal = await asyncio.wait_for(election_proposal_future, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] Timed out.")
            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")
            exit(1)

        unit_test_logger.debug("Awaited election proposal")

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)

    propose(raftLog, proposedValue, election, execute_request_task, expected_attempt_number=current_attempt_number)

    # Call "value committed" handler again for the 2nd proposal.
    yieldProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                            proposer_id=2,
                                                                            election_term=current_election_term,
                                                                            attempt_number=current_attempt_number)
    propose(raftLog, yieldProposalFromNode2, election, execute_request_task,
            expected_attempt_number=current_attempt_number)

    yieldProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.YIELD),
                                                                            proposer_id=3,
                                                                            election_term=current_election_term,
                                                                            attempt_number=current_attempt_number)
    propose(raftLog, yieldProposalFromNode3, election, execute_request_task,
            expected_attempt_number=current_attempt_number)

    election_decision_future: asyncio.Future[LeaderElectionVote] = raftLog.election_decision_future
    assert election_decision_future is not None

    try:
        await asyncio.wait_for(election_decision_future, 5)
    except TimeoutError:
        unit_test_logger.debug("[ERROR] \"election_decision\" future was not resolved.")

        for task in asyncio.all_tasks():
            asyncio.Task.print_stack(task)
            unit_test_logger.debug("\n\n\n")

        assert False

    assert_election_failed(election, execute_request_task, election_decision_future, expected_proposer_id=1,
                           expected_term_number=current_election_term, expected_attempt_number=current_attempt_number,
                           expected_proposals_received=3)


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_fail_election_nine_times_then_win(kernel: DistributedKernel, execution_request: dict[str, any]):
    raftLog: RaftLog = kernel.synclog

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    NUM_FAILURES: int = 9

    for i in range(0, NUM_FAILURES):
        unit_test_logger.debug(f"\n\n\nElection Round #{i}\n")
        await fail_election(kernel, execution_request, loop,
                            current_election_term=1,
                            current_attempt_number=1 + i)

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)
    assert election.election_finished_event.is_set()

    election_proposal_future = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task = loop.create_task(kernel.execute_request(None, [], execution_request))
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
    assert (raftLog.election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog.election_decision_future.done() == False)
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
    propose(raftLog, leadProposalFromNode2, election, execute_request_task,
            expected_attempt_number=NUM_FAILURES + 1)

    vote_proposal_future: asyncio.Future[LeaderElectionVote] = loop.create_future()

    async def mocked_append_election_vote(*args, **kwargs):
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future
    assert (election_decision_future is not None)

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        unit_test_logger.debug(
            f"\n\nMocked RaftLog::_append_execution_end_notification called with args {args} and kwargs {kwargs}.")
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
            unit_test_logger.debug("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        unit_test_logger.debug(f"Got proposed vote: {proposedVote}")

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
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        unit_test_logger.debug("\"Leading\" future should be done now.")
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
            unit_test_logger.debug("[ERROR] \"execution_done\" future was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execution_done_future.done()

        notification: ExecutionCompleteNotification = execution_done_future.result()
        unit_test_logger.debug(f"Got ExecutionCompleteNotification: {notification}")
        assert notification is not None
        assert notification.proposer_id == 1
        assert notification.election_term == 1

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == NUM_FAILURES + 1
            assert proposal.is_lead

        commit_value(raftLog, notification, value_id=notification.id)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execute_request_task.done()
        assert_election_success(election)

    spoofed_session: SpoofedSession = kernel.session
    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert "smr_ready" in spoofed_session.message_types_sent
    assert "smr_lead_task" in spoofed_session.message_types_sent
    assert "status" in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 23
    assert len(spoofed_session.message_types_sent) == 23
    assert spoofed_session.message_types_sent_counts["execute_input"] == (NUM_FAILURES + 1)
    assert spoofed_session.message_types_sent_counts["execute_reply"] == (NUM_FAILURES + 1)
    assert spoofed_session.message_types_sent_counts["smr_ready"] == 1
    assert spoofed_session.message_types_sent_counts["smr_lead_task"] == 1
    assert spoofed_session.message_types_sent_counts["status"] == 1


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_election_success_after_timing_out(kernel: DistributedKernel, execution_request: dict[str, any]):
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
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(
            kernel.execute_request(None, [], execution_request))
        proposedValue: LeaderElectionProposal = await election_proposal_future

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)
    leading_future: asyncio.Future[int] = raftLog._leading_future

    propose(raftLog, proposedValue, election, execute_request_task)

    vote_proposal_future: asyncio.Future[LeaderElectionVote] = loop.create_future()

    async def mocked_append_election_vote(*args, **kwargs):
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future
    assert (election_decision_future is not None)

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        unit_test_logger.debug(
            f"\n\nMocked RaftLog::_append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             _append_execution_end_notification=mocked_raftlog_append_execution_end_notification):
        try:
            proposedVote: LeaderElectionVote = await asyncio.wait_for(vote_proposal_future, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        unit_test_logger.debug(f"Got proposed vote: {proposedVote}")

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
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        unit_test_logger.debug("\"Leading\" future should be done now.")
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
            unit_test_logger.debug("[ERROR] \"execution_done\" future was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execution_done_future.done()

        notification: ExecutionCompleteNotification = execution_done_future.result()
        unit_test_logger.debug(f"Got ExecutionCompleteNotification: {notification}")
        assert notification is not None
        assert notification.proposer_id == 1
        assert notification.election_term == 1

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 1
            assert proposal.attempt_number == 1
            assert proposal.is_lead

        commit_value(raftLog, notification, value_id=notification.id)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execute_request_task.done()
        assert_election_success(election)

    spoofed_session: SpoofedSession = kernel.session
    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert "smr_ready" in spoofed_session.message_types_sent
    assert "smr_lead_task" in spoofed_session.message_types_sent
    assert 'status' in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 5
    assert len(spoofed_session.message_types_sent) == 5
    assert spoofed_session.message_types_sent_counts["execute_input"] == 1
    assert spoofed_session.message_types_sent_counts["execute_reply"] == 1
    assert spoofed_session.message_types_sent_counts["smr_ready"] == 1
    assert spoofed_session.message_types_sent_counts["smr_lead_task"] == 1
    assert spoofed_session.message_types_sent_counts["status"] == 1


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_election_loss_buffer_one_lead_proposal(kernel: DistributedKernel, execution_request: dict[str, any]):
    """
    The election succeeds like normal after buffering one of the proposals.

    Node #1 does not win the election, though. Node #2 wins the election in this test.
    """
    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")

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

    commit_value(raftLog, leadProposalFromNode2)

    assert raftLog._buffered_proposals is not None
    assert len(raftLog._buffered_proposals) == 1

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(
            kernel.execute_request(None, [], execution_request))
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
    assert (raftLog.election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog.election_decision_future.done() == False)
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
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future
    assert (election_decision_future is not None)

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        unit_test_logger.debug(
            f"\n\nMocked RaftLog::_append_execution_end_notification called with args {args} and kwargs {kwargs}.")
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
            unit_test_logger.debug("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        unit_test_logger.debug(f"Got proposed vote: {proposedVote}")

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
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        unit_test_logger.debug("\"Leading\" future should be done now.")
        assert leading_future.done() == True
        assert raftLog.leader_id == 2
        assert raftLog.leader_term == 1
        wait, leading = raftLog._is_leading(1)
        assert wait == False
        assert leading == False

        await asyncio.sleep(0.5)

        notification: ExecutionCompleteNotification = ExecutionCompleteNotification(proposer_id=2, election_term=1)

        commit_value(raftLog, notification)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execute_request_task.done()

    spoofed_session: SpoofedSession = kernel.session
    assert spoofed_session is not None
    assert isinstance(spoofed_session, SpoofedSession)

    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert 'smr_ready' in spoofed_session.message_types_sent
    assert 'status' in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 4
    assert len(spoofed_session.message_types_sent) == 4


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_election_win_buffer_one_yield_proposal(kernel: DistributedKernel, execution_request: dict[str, any]):
    """
    The election succeeds like normal after buffering one of the proposals.

    Node #1 wins the election, as the buffered proposal is a YIELD proposal.
    """
    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")

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

    commit_value(raftLog, leadProposalFromNode2)

    assert raftLog._buffered_proposals is not None
    assert len(raftLog._buffered_proposals) == 1

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(
            kernel.execute_request(None, [], execution_request))
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
    assert (raftLog.election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog.election_decision_future.done() == False)
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
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future
    assert (election_decision_future is not None)

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        unit_test_logger.debug(
            f"\n\nMocked RaftLog::_append_execution_end_notification called with args {args} and kwargs {kwargs}.")
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
            unit_test_logger.debug("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        unit_test_logger.debug(f"Got proposed vote: {proposedVote}")

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
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        unit_test_logger.debug("\"Leading\" future should be done now.")
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
            unit_test_logger.debug("[ERROR] \"execution_done\" future was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execution_done_future.done()

        notification: ExecutionCompleteNotification = execution_done_future.result()
        unit_test_logger.debug(f"Got ExecutionCompleteNotification: {notification}")
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

        commit_value(raftLog, notification, value_id=notification.id)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execute_request_task.done()
        assert_election_success(election)

    spoofed_session: SpoofedSession = kernel.session
    assert spoofed_session is not None
    assert isinstance(spoofed_session, SpoofedSession)

    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert 'smr_ready' in spoofed_session.message_types_sent
    assert 'smr_lead_task' in spoofed_session.message_types_sent
    assert 'status' in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 5
    assert len(spoofed_session.message_types_sent) == 5


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_catch_up_after_migration(kernel: DistributedKernel, execution_request: dict[str, any]):
    """
    The election succeeds like normal after buffering two of the proposals.
    """
    global CommittedValues

    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    unit_test_logger.debug("FAILING ELECTION")

    # Get to a failed election
    await fail_election(kernel, execution_request, loop=loop)

    unit_test_logger.debug("ELECTION FAILED")

    # Now perform the migration, spoofing some aspects of it
    close_future: asyncio.Future[int] = loop.create_future()

    def mocked_raftlog_close(*args, **kwargs):
        unit_test_logger.debug(f"\nMocked RaftLog::close called with args {args} and kwargs {kwargs}.")
        close_future.set_result(1)

    write_data_dir_to_hdfs_future: asyncio.Future[bytes] = loop.create_future()

    async def mocked_raftlog_write_data_dir_to_hdfs(*args, **kwargs):
        unit_test_logger.debug(f"\nMocked RaftLog::write_data_dir_to_hdfs called with args {args} and kwargs {kwargs}.")

        assert isinstance(args[0], RaftLog)

        raftlog_state_serialized: bytes = args[0]._get_serialized_state()

        write_data_dir_to_hdfs_future.set_result(raftlog_state_serialized)

        return FakePersistentStorePath

    # with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "close", mocked_raftlog_close):
    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             close=mocked_raftlog_close,
                             write_data_dir_to_hdfs=mocked_raftlog_write_data_dir_to_hdfs):
        await kernel.prepare_to_migrate_request(None, [], {})

    assert close_future.done()
    assert close_future.result() == 1

    assert write_data_dir_to_hdfs_future.done()

    spoofed_session: SpoofedSession = kernel.session
    assert spoofed_session is not None
    assert isinstance(spoofed_session, SpoofedSession)

    assert "prepare_to_migrate_reply" in spoofed_session.message_types_sent
    assert "execute_input" in spoofed_session.message_types_sent
    assert "execute_reply" in spoofed_session.message_types_sent
    assert 'smr_ready' in spoofed_session.message_types_sent

    unit_test_logger.debug(f"spoofed_session.num_send_calls: {spoofed_session.num_send_calls}")
    unit_test_logger.debug(f"spoofed_session.message_types_sent: {spoofed_session.message_types_sent}")

    assert spoofed_session.num_send_calls == 5
    assert len(spoofed_session.message_types_sent) == 5

    serialized_state: bytes = write_data_dir_to_hdfs_future.result()
    assert serialized_state is not None
    assert isinstance(serialized_state, bytes)

    catchup_with_peers_future: asyncio.Future[int] = loop.create_future()

    async def mock_catchup_with_peers(*args, **kwargs):
        unit_test_logger.debug(f"Mocked RaftLog::catchup_with_peers called with args {args} and kwargs {kwargs}")
        catchup_with_peers_future.set_result(1)

    def mock_retrieve_serialized_state_from_remote_storage(*args, **kwargs):
        unit_test_logger.debug(
            f"Mocked RaftLog::retrieve_serialized_state_from_remote_storage called with args {args} and kwargs {kwargs}")
        return serialized_state

    new_raft_log_future: asyncio.Future[RaftLog] = loop.create_future()
    append_catchup_value_future: asyncio.Future[SynchronizedValue] = loop.create_future()

    async def mock_append_catchup_value(*args, **kwargs):
        unit_test_logger.debug(f"Mocked RaftLog::_append_catchup_value called with args {args} and kwargs {kwargs}")

        def set_append_catchup_value_future_result():
            unit_test_logger.debug(f"Setting result of append_catchup_value_future to {args[1]}")
            append_catchup_value_future.set_result(args[1])
            unit_test_logger.debug(f"Set result of append_catchup_value_future to {args[1]}")

        def set_new_raft_log_future_result():
            unit_test_logger.debug(f"Setting result of new_raft_log_future to {args[0]}")
            new_raft_log_future.set_result(args[0])
            unit_test_logger.debug(f"Set result of new_raft_log_future to {args[0]}")

        if asyncio.get_running_loop() != loop:
            # This is generally what should happen, assuming the configuration/setup of the test(s) does not change.
            # Specifically, the mock_append_catchup_value function will be called from the "control thread", which
            # is running a separate IO loop than the one running the unit test(s).
            loop.call_soon_threadsafe(append_catchup_value_future.set_result, args[1])
            loop.call_soon_threadsafe(new_raft_log_future.set_result, args[0])
        else:
            # This branch is not likely.
            set_append_catchup_value_future_result()
            set_new_raft_log_future_result()

    unit_test_logger.debug("Creating next kernel...\n\n\n\n\n\n\n\n\n\n")

    # with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "catchup_with_peers", catchup_with_peers_future):
    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             retrieve_serialized_state_from_remote_storage=mock_retrieve_serialized_state_from_remote_storage,
                             _append_catchup_value=mock_append_catchup_value), \
            mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "create_log_node", mock_create_log_node):
        # TODO: Apply serialized state. Recreate Kernel.
        new_kernel: DistributedKernel = await create_kernel(
            persistent_id=FakePersistentStorePath,
            init_persistent_store=False,
            call_start=True)
        assert new_kernel is not None

        # with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "create_log_node", mock_create_log_node):
        # init_persistent_store_task: asyncio.Task[str] = asyncio.create_task(kernel.init_persistent_store_with_persistent_id(FakePersistentStorePath), name = "Initialize Persistent Store")

        assert new_kernel != kernel

        try:
            unit_test_logger.debug("Waiting for 'catchup' future.")
            await asyncio.wait_for(append_catchup_value_future, timeout=5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] Future for appending 'catchup' value timed-out.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False  # fail the test

        try:
            unit_test_logger.debug("Waiting for 'catchup' future.")
            await asyncio.wait_for(new_raft_log_future, timeout=5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] Future for appending 'catchup' value timed-out.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False  # fail the test

        assert append_catchup_value_future.done()
        catchup_value: SynchronizedValue = append_catchup_value_future.result()
        assert isinstance(catchup_value, SynchronizedValue)
        assert catchup_value.proposer_id == 1
        assert catchup_value.key == KEY_CATCHUP
        assert catchup_value.should_end_execution == False
        assert catchup_value.operation == KEY_CATCHUP

        assert new_raft_log_future.done()
        new_raft_log_from_future: RaftLog = new_raft_log_future.result()
        assert new_raft_log_from_future is not None
        assert new_raft_log_from_future._node_id == 1
        assert catchup_value.election_term == new_raft_log_from_future._leader_term_before_migration

        catchup_future: asyncio.Future[SynchronizedValue] = new_raft_log_from_future._catchup_future
        assert catchup_future is not None

        for val in CommittedValues:
            val_id: str = ""
            if val.proposer_id == 1:
                val_id = val.id

            commit_value(new_raft_log_from_future, val, value_id=val_id, record=False)

        commit_value(new_raft_log_from_future, catchup_value, value_id=catchup_value.id, record=True)

        # unit_test_logger.debug("Created 'init persistent store' task.")
        await new_kernel.init_raft_log_event.wait()

        new_raft_log: RaftLog = new_kernel.synclog
        assert new_raft_log != kernel.synclog
        assert new_raft_log == new_raft_log_from_future

        assert new_kernel.kernel_id == kernel.kernel_id
        assert new_kernel.smr_node_id == kernel.smr_node_id

        assert new_raft_log._kernel_id == kernel.kernel_id
        assert new_raft_log.node_id == kernel.smr_node_id

        await new_kernel.init_synchronizer_event.wait()
        await new_kernel.start_synchronizer_event.wait()

        catchup_awaitable: asyncio.Future[SynchronizedValue] = catchup_future
        if catchup_future.get_loop() != loop:
            catchup_awaitable = loop.create_future()

            async def wait_target():
                try:
                    catchup_result = await catchup_future
                except Exception as e:
                    loop.call_soon_threadsafe(catchup_awaitable.set_exception, e)
                else:
                    loop.call_soon_threadsafe(catchup_awaitable.set_result, catchup_result)

            asyncio.run_coroutine_threadsafe(wait_target(), catchup_future.get_loop())

        try:
            await asyncio.wait_for(catchup_awaitable, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] New RaftLog's '_catchup_future' timed-out.")

            unit_test_logger.debug("Current thread's IO loop tasks:")
            for task in asyncio.all_tasks(loop=asyncio.get_running_loop()):
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n")

            unit_test_logger.debug("Control thread's IO loop tasks:")
            for task in asyncio.all_tasks(loop=new_kernel.control_thread.io_loop.asyncio_loop):
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n")

            unit_test_logger.debug(f"New kernel's control thread is alive: {new_kernel.control_thread.is_alive()}")

            assert False  # fail the test

        await new_kernel.init_persistent_store_event.wait()
        unit_test_logger.debug("New Persistent Store initialized.")


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_get_election_metadata(kernel: DistributedKernel, execution_request: dict[str, any]):
    """
    Test that getting the election metadata and serializing it to JSON works.
    """

    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")

    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(
            kernel.execute_request(None, [], execution_request))
        proposedValue: LeaderElectionProposal = await election_proposal_future

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(1)

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
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        unit_test_logger.debug(
            f"\n\nMocked RaftLog::_append_execution_end_notification called with args {args} and kwargs {kwargs}.")
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
            unit_test_logger.debug("[ERROR] LeaderElectionVote was not proposed.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        unit_test_logger.debug(f"Got proposed vote: {proposedVote}")

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(leading_future, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        unit_test_logger.debug("\"Leading\" future should be done now.")
        wait, leading = raftLog._is_leading(1)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execution_done_future, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execution_done\" future was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert execution_done_future.done()

        notification: ExecutionCompleteNotification = execution_done_future.result()
        unit_test_logger.debug(f"Got ExecutionCompleteNotification: {notification}")

        commit_value(raftLog, notification, value_id=notification.id)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(execute_request_task, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"execute_request\" task was not resolved.")

            for task in asyncio.all_tasks():
                asyncio.Task.print_stack(task)
                unit_test_logger.debug("\n\n\n")

            assert False

        assert_election_success(election)

    metadata: dict[str, any] = election.get_election_metadata()

    unit_test_logger.info(f"Raw Metadata:\n{metadata}")

    assert metadata is not None
    assert isinstance(metadata, dict)
    assert len(metadata) == 16

    import json
    metadata_json: Optional[str] = None
    try:
        metadata_json = json.dumps(metadata, indent=2)
    except Exception as ex:
        unit_test_logger.error(f"Failed to serialize Election metadata because: {ex}")

        assert ex is None  # Fail

    assert metadata_json is not None
    assert isinstance(metadata_json, str)
    assert len(metadata_json) > 0

    unit_test_logger.info(f"JSON Metadata:\n{metadata_json}")


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_skip_election_dropped_messages(kernel: DistributedKernel, execution_request: dict[str, any]):
    """
    Test that an election can be skipped, and that the next election can proceed correctly if
    the "execute_request" and "yield_request" messages associated with the skipped election are never received.
    """
    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert (synchronizer is not None)
    assert (raftLog is not None)

    # Call "value committed" handler again for the 2nd proposal.
    leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                           proposer_id=2,
                                                                           election_term=1,
                                                                           attempt_number=1)

    # Receive first proposal from peers.
    commit_value(raftLog, leadProposalFromNode2)

    assert len(raftLog._buffered_votes) == 0
    assert len(raftLog._buffered_proposals) == 1
    assert len(raftLog._buffered_proposals[1]) == 1
    assert raftLog._buffered_proposals[1][0].proposal == leadProposalFromNode2

    leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                           proposer_id=3,
                                                                           election_term=1,
                                                                           attempt_number=1)

    # Receive second proposal from peers.
    commit_value(raftLog, leadProposalFromNode3)

    assert len(raftLog._buffered_votes) == 0
    assert len(raftLog._buffered_proposals) == 1
    assert len(raftLog._buffered_proposals[1]) == 2
    assert raftLog._buffered_proposals[1][0].proposal == leadProposalFromNode2
    assert raftLog._buffered_proposals[1][1].proposal == leadProposalFromNode3

    vote: LeaderElectionVote = LeaderElectionVote(proposed_node_id=2,
                                                  proposer_id=2,
                                                  election_term=1,
                                                  attempt_number=1)

    # Receive vote from peers.
    commit_value(raftLog, vote)

    assert len(raftLog._buffered_votes) == 1
    assert len(raftLog._buffered_votes[1]) == 1
    assert raftLog._buffered_votes[1][0].vote == vote
    assert raftLog.current_election is None

    notification: ExecutionCompleteNotification = ExecutionCompleteNotification(proposer_id=2, election_term=1)
    commit_value(raftLog, notification)

    unit_test_logger.info(f"raftLog.num_elections_skipped = {raftLog.num_elections_skipped}")

    assert raftLog.current_election is not None
    assert raftLog.current_election.term_number == 1
    assert raftLog.current_election.winner_id == 2
    assert raftLog.current_election.was_skipped == True
    assert raftLog.num_elections_skipped == 1




@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_skip_election_delayed_messages(kernel: DistributedKernel, execution_request: dict[str, any]):
    """
    Test that an election can be skipped, and that the next election can proceed correctly if
    the "execute_request" and "yield_request" messages associated with the skipped election are received
    eventually, such as during the next election.
    """
    pass

# @mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
# @pytest.mark.asyncio
# async def test_get_election_metadata(kernel: DistributedKernel, execution_request: dict[str, any]):
#     """
#
#     """
#     pass
#
# @mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
# @pytest.mark.asyncio
# async def test_get_election_metadata(kernel: DistributedKernel, execution_request: dict[str, any]):
#     """
#
#     """
#     pass
