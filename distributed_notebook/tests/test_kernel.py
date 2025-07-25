import asyncio
import logging
import os
import shutil
import sys
import uuid
from collections import OrderedDict
from typing import Optional, Dict, Any, Type
from unittest import mock

import pytest
import pytest_asyncio
import torch
from ipykernel.control import ControlThread

import distributed_notebook.sync.raft_log
from distributed_notebook.deep_learning import ResNet18, CIFAR10, DeepLearningModel, VGG16, InceptionV3, VGG19, VGG13, \
    VGG11, IMDbLargeMovieReviewTruncated, Bert, GPT2, DeepSpeech2, LibriSpeech, TinyImageNet, CoLA
from distributed_notebook.deep_learning.data.custom_dataset import CustomDataset
from distributed_notebook.kernel import DistributedKernel
from distributed_notebook.sync import Synchronizer, RaftLog, SyncAST
from distributed_notebook.sync.election import Election, ExecutionCompleted, AllReplicasProposedYield
from distributed_notebook.sync.log import ElectionProposalKey, LeaderElectionProposal, \
    LeaderElectionVote, Checkpointer, ExecutionCompleteNotification, SynchronizedValue, KEY_CATCHUP
from distributed_notebook.sync.simulated_checkpointing.simulated_checkpointer import SimulatedCheckpointer
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
FakePersistentStorePath: str = f"unit-test-persistent-store/{str(uuid.uuid4())}"

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


def mock_create_log_node(*args, **mock_kwargs):
    unit_test_logger.debug(f"Mocked RaftLog::create_log_node called with args {args} and kwargs {mock_kwargs}.")

    return SpoofedLogNode(**mock_kwargs)


DefaultResourceRequest: Dict[str, Any] = {
    "gpus": 1,
    "cpus": 1000,
    "memory": 512,
    "vram": 0.1,
}


async def create_kernel(
        remote_storage_hostname: str = "127.0.0.1:10000",
        kernel_id: str = DefaultKernelId,
        smr_port: int = 8000,
        smr_node_id: int = 1,
        smr_nodes: list[int] = None,
        smr_join: bool = False,
        should_register_with_local_daemon: bool = False,
        pod_name: str = "TestPod",
        node_name: str = "TestNode",
        debug_port: int = -1,
        storage_base: str = "./",
        init_persistent_store: bool = True,
        call_start: bool = True,
        local_tcp_server_port: int = -1,
        simulate_checkpointing_latency: bool = False,
        persistent_id: Optional[str] = None,
        resource_request: Optional[Dict[str, Any]] = None,
        remote_storage_definitions: Optional[Dict[str, Any]] = None,
        simulate_training_using_sleep: bool = False,
        smr_enabled: bool = True,
        **kwargs
) -> DistributedKernel:
    global DefaultResourceRequest

    if smr_nodes is None:
        smr_nodes = []

    if resource_request is None:
        resource_request = DefaultResourceRequest

    if os.environ.get("SIMULATE_CHECKPOINTING_LATENCY", "") != "":
        print("Setting `simulate_checkpointing_latency` to True for unit tests")
        simulate_checkpointing_latency = True

    keyword_args = {
        "remote_storage_hostname": remote_storage_hostname,
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
        "simulate_checkpointing_latency": simulate_checkpointing_latency,
        "simulate_training_using_sleep": simulate_training_using_sleep,
        "smr_enabled": smr_enabled,
    }

    keyword_args.update(kwargs)

    os.environ.setdefault("PROMETHEUS_METRICS_PORT", "-1")
    kernel: DistributedKernel = DistributedKernel(**keyword_args)
    kernel.control_thread = ControlThread(daemon=True)
    kernel.control_stream = SpoofedStream()
    kernel.control_thread.start()
    kernel.num_replicas = 3
    kernel.should_read_data_from_remote_storage = False
    kernel.deployment_mode = "DOCKER_SWARM"
    kernel.session = SpoofedSession()
    kernel.prometheus_port = -1
    kernel.debug_port = -1
    kernel.kernel_id = DefaultKernelId

    if resource_request is not None:
        kernel.current_resource_request = resource_request
        kernel.resource_requests.append(resource_request)

    if remote_storage_definitions is not None:
        for name, definition in remote_storage_definitions.items():
            kernel.register_remote_storage_definition(definition)

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


def assert_election_success(election: Election):
    assert election.code_execution_completed_successfully
    assert election.voting_phase_completed_successfully
    assert election.election_finished_event.is_set()
    assert election.completion_reason == ExecutionCompleted


def create_execution_request(kernel_id: str = DefaultKernelId,
                             message_id: str = "70d1412e-f937-416e-99fe-a48eed8dc8a4"):
    return {
        "header": {
            "msg_id": message_id,
            "msg_type": "execute_request",
            "date": DefaultDate,
            "username": kernel_id,
            "session": kernel_id,
            "version": 1
        },
        "parent_header": {},
        "content": {
            "code": "a = 1",
            "stop_on_error": False,
        },
        "metadata": {}
    }


async def mocked_sync(synchronizer: Synchronizer,
                      execution_ast: SyncAST,
                      source: Optional[str] = None,
                      checkpointer: Optional[Checkpointer] = None) -> bool:
    unit_test_logger.debug(
        f"\nMocked Synchronizer::sync called with execution_ast={execution_ast}, source={source}, checkpointer={checkpointer}")
    await asyncio.sleep(0.25)
    return True


async def mocked_serialize_and_append_value(raft_log: RaftLog, value: SynchronizedValue):
    unit_test_logger.debug(
        f"\nMocked RaftLog::_serialize_and_append_value called with raft_log={raft_log} and value={value}")
    await asyncio.sleep(0.25)

    value_id: str = ""
    if raft_log.node_id == value.proposer_id:
        value_id = value.id

    commit_value(raft_log, value, value_id=value_id, record=True)


def propose(raftLog: RaftLog, proposedValue: LeaderElectionProposal, election: Election,
            execute_request_task: asyncio.Task[any], expected_num_proposals_received: int = -1,
            expected_attempt_number: int = 1, expected_num_values_proposed: int = 1):
    if expected_num_proposals_received == -1:
        expected_num_proposals_received = election.num_proposals_received + 1

    valueId: str = ""
    if proposedValue.proposer_id == 1:
        valueId = proposedValue.id

    commit_value(raftLog, proposedValue, value_id=valueId)

    if election.num_lead_proposals_received > 0:
        assert (raftLog.decide_election_future is not None)

    # Check that everything is updated correctly now that we've received a proposal.
    assert (election.num_proposals_received == expected_num_proposals_received)
    assert (election.proposals.get(proposedValue.proposer_id) is not None)
    assert (election.proposals.get(proposedValue.proposer_id) == proposedValue)
    assert (len(raftLog._proposed_values) == expected_num_values_proposed)

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


async def example(kernel: DistributedKernel, execution_request: Dict[str, Any]):
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
            f"\n\nMocked RaftLog::append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             append_execution_end_notification=mocked_raftlog_append_execution_end_notification):

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


async def perform_training(
        model_class: Type,
        dataset_class: Type,
        num_training_loops: int = 3,
        target_training_duration_ms: float = 1000.0
):
    """
    Helper/utility function to carry out a unit test in which a kernel proposes and leads the execution of
    some deep learning training code using a specified model and dataset.

    :param model_class: the specified model
    :param dataset_class: the specified dataset
    :param num_training_loops: how many times to execute
    :param target_training_duration_ms: how long each execution should aim to last
    :return:
    """
    kernel: DistributedKernel = await create_kernel(
        remote_storage_hostname = "127.0.0.1:10000",
        kernel_id = DefaultKernelId,
        smr_port = 8000,
        smr_node_id = 1,
        smr_nodes = None,
        smr_join = False,
        should_register_with_local_daemon = False,
        pod_name = "TestPod",
        node_name = "TestNode",
        debug_port = -1,
        simulate_training_using_sleep = True,
        remote_storage = "local",
        smr_enabled = True,
    )
    assert kernel is not None

    assert issubclass(model_class, DeepLearningModel)
    assert issubclass(dataset_class, CustomDataset)

    assert num_training_loops > 0
    assert target_training_duration_ms > 0

    weights: Optional[torch.Tensor] = None
    for i in range(1, num_training_loops + 1):
        print(f'\n\n\n{"\033[0;36m"}Training Loop {i}/{num_training_loops} for Model "{model_class.model_name()}" on '
              f'Dataset "{dataset_class.dataset_name()}"{"\033[0m"}\n\n')
        execution_request: Dict[str, Any] = create_execution_request(message_id = str(uuid.uuid4()))
        assert execution_request is not None

        # Update request metadata.
        metadata: Dict[str, Any] = execution_request["metadata"]
        assert metadata is not None
        metadata["model"] = model_class.model_name()
        metadata["dataset"] = dataset_class.dataset_name()
        metadata["gpu_device_ids"] = [0]

        # Update request content (specifically the user-submitted code).
        content: Dict[str, Any] = execution_request["content"]
        assert content is not None
        content["code"] = f"training_duration_millis = {target_training_duration_ms}"

        await propose_lead_and_win(kernel, execution_request, term_number=i, expected_num_values_proposed=i)

        async with kernel.user_ns_lock:
            model: DeepLearningModel = kernel.shell.user_ns.get("model", None)

        assert model is not None
        assert isinstance(model, model_class)

        next_weights: torch.nn.Parameter = model.output_layer.weight.clone()
        # After the first loop, compare previous weights against current weights to ensure they're changing.
        if i > 1:
            assert weights is not None
            assert not weights.equal(next_weights)

        weights = next_weights

        async with kernel.user_ns_lock:
            dataset: CustomDataset = kernel.shell.user_ns.get("dataset", None)

        assert dataset is not None
        assert isinstance(dataset, dataset_class)

        print(torch.cuda.memory_summary(abbreviated=False))
        await asyncio.sleep(0.125)

    assert kernel.get_creation_code_called == 1
    assert kernel.get_download_code_called == num_training_loops - 1

    print(torch.cuda.memory_summary(abbreviated=False))
    await asyncio.sleep(0.25)
    print(f'{"\033[0;32m"}\n\n\n\n\n\nFinished test for training model "{model_class.model_name()}" on '
          f'dataset "{dataset_class.dataset_name()}"\n\n\n\n\n{"\033[0m"}')

    # The user_ns seems to persist between unit tests sometimes...
    # Not sure why, but we clear it here to prevent any issues.
    kernel.shell.user_ns.clear()


async def propose_lead_and_win(
        kernel: DistributedKernel,
        execution_request: Dict[str, Any],
        term_number: int = 1,
        expected_num_values_proposed: int = 1,
):
    assert kernel is not None
    assert execution_request is not None

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
    assert (proposedValue.election_term == term_number)

    # Check that the kernel created an election, but that no proposals were received yet.
    election: Election = kernel.synclog.get_election(term_number)
    assert (election is not None)
    assert (election.term_number == term_number)
    assert (election.num_proposals_received == 0)
    assert (raftLog._future_io_loop is not None)
    assert (raftLog.election_decision_future is not None)
    assert (raftLog._leading_future is not None)
    assert (raftLog.election_decision_future.done() == False)
    assert (raftLog._leading_future.done() == False)

    # We've proposed it, so the RaftLog knows about it, even though the value hasn't been committed yet.
    assert (len(raftLog._proposed_values) == expected_num_values_proposed)

    innerMap: OrderedDict[int, LeaderElectionProposal] = raftLog._proposed_values.get(election.term_number)
    assert (innerMap is not None)
    assert (len(innerMap) == 1)
    assert (1 in innerMap)
    assert (innerMap[1] == proposedValue)

    leading_future: asyncio.Future[int] = raftLog._leading_future
    assert leading_future is not None
    assert leading_future.done() == False

    propose(raftLog, proposedValue, election, execute_request_task, expected_num_values_proposed = expected_num_values_proposed)

    # Call "value committed" handler again for the 2nd proposal.
    leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                           proposer_id=2,
                                                                           election_term=term_number,
                                                                           attempt_number=1)
    propose(raftLog, leadProposalFromNode2, election, execute_request_task, expected_num_values_proposed = expected_num_values_proposed)

    vote_proposal_future: asyncio.Future[LeaderElectionVote] = loop.create_future()

    async def mocked_append_election_vote(*args, **kwargs):
        unit_test_logger.debug(f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
        vote_proposal_future.set_result(args[1])

    election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future
    assert (election_decision_future is not None)

    execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

    async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
        unit_test_logger.debug(
            f"\n\nMocked RaftLog::append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             append_execution_end_notification=mocked_raftlog_append_execution_end_notification):

        assert raftLog.append_execution_end_notification is not None
        assert kernel.synclog.append_execution_end_notification is not None
        assert kernel.synchronizer._synclog.append_execution_end_notification is not None

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=term_number,
                                                                               attempt_number=1)
        propose(raftLog, leadProposalFromNode3, election, execute_request_task, expected_num_values_proposed = expected_num_values_proposed)

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
        assert (len(raftLog._proposed_values) == expected_num_values_proposed)
        assert election.is_active
        assert election.voting_phase_completed_successfully == False

        propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future,
                     expected_election_term=term_number)

        try:
            # We'll wait up to 5 seconds, but it should happen very quickly.
            await asyncio.wait_for(leading_future, 5)
        except TimeoutError:
            unit_test_logger.debug("[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
            assert False  # Fail the test.

        unit_test_logger.debug("\"Leading\" future should be done now.")
        assert leading_future.done() == True
        assert raftLog.leader_id == 1
        assert raftLog.leader_term == term_number
        wait, leading = raftLog._is_leading(term_number)
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
        assert notification.election_term == term_number

        for _, proposal in election.proposals.items():
            assert proposal.election_term == term_number
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

    if term_number == 1:
        assert spoofed_session.num_send_calls == 5
        assert len(spoofed_session.message_types_sent) == 5

    assert synchronizer.execution_count == term_number

@pytest_asyncio.fixture
async def kernel(
        remote_storage_hostname: str = "127.0.0.1:10000",
        kernel_id: str = DefaultKernelId,
        smr_port: int = 8000,
        smr_node_id: int = 1,
        smr_nodes: list[int] = None,
        smr_join: bool = False,
        should_register_with_local_daemon: bool = False,
        pod_name: str = "TestPod",
        node_name: str = "TestNode",
        debug_port: int = -1,
        simulate_training_using_sleep: bool = False,
        remote_storage: str = "local",
        smr_enabled: bool = True,
        **kwargs
) -> DistributedKernel:
    if smr_nodes is None:
        smr_nodes = []

    if len(kwargs):
        print('Passing the following keyword arguments to the create_kernel function:', flush = True)
        for k, v in kwargs.items():
            print(f'\t"{k}": {v}', flush=True)

    print(f"simulate_training_using_sleep = {simulate_training_using_sleep}", flush = True)
    print(f"remote_storage = {remote_storage}", flush = True)
    print(f"smr_enabled = {smr_enabled}", flush = True)

    return await create_kernel(
        remote_storage_hostname=remote_storage_hostname,
        remote_storage=remote_storage,
        kernel_id=kernel_id,
        smr_port=smr_port,
        smr_node_id=smr_node_id,
        smr_nodes=smr_nodes,
        smr_join=smr_join,
        should_register_with_local_daemon=should_register_with_local_daemon,
        pod_name=pod_name,
        node_name=node_name,
        debug_port=debug_port,
        simulate_training_using_sleep=simulate_training_using_sleep,
        smr_enabled=smr_enabled,
        **kwargs,
    )


@pytest.fixture
def execution_request():
    return create_execution_request()


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

@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.asyncio
async def test_propose_lead_and_win(
        kernel: DistributedKernel,
        execution_request: Dict[str, Any],
):
    await propose_lead_and_win(kernel, execution_request)

@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.asyncio
async def test_lead_multiple_elections_in_a_row(kernel: DistributedKernel, execution_request: Dict[str, Any]):
    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    assert (synchronizer is not None)
    assert (raftLog is not None)

    kernel.election_timeout_seconds = 0.5
    raftLog._election_timeout_sec = 0.5

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    for term in range(1, 6):
        unit_test_logger.debug(f"TERM {term}\n\n\n\n\n\n\n")
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
        assert (proposedValue.election_term == term)

        # Check that the kernel created an election, but that no proposals were received yet.
        election: Election = kernel.synclog.get_election(term)
        assert (election is not None)
        assert (election.term_number == term)
        assert (election.num_proposals_received == 0)
        assert (raftLog._future_io_loop is not None)
        assert (raftLog.election_decision_future is not None)
        assert (raftLog._leading_future is not None)
        assert (raftLog.election_decision_future.done() == False)
        assert (raftLog._leading_future.done() == False)

        # We've proposed it, so the RaftLog knows about it, even though the value hasn't been committed yet.
        assert (len(raftLog._proposed_values) == term)

        innerMap: OrderedDict[int, LeaderElectionProposal] = raftLog._proposed_values.get(election.term_number)
        assert (innerMap is not None)
        assert (len(innerMap) == 1)
        assert (1 in innerMap)
        assert (innerMap[1] == proposedValue)

        leading_future: asyncio.Future[int] = raftLog._leading_future
        assert leading_future is not None
        assert leading_future.done() == False

        propose(raftLog, proposedValue, election, execute_request_task, expected_num_values_proposed=term)

        # Call "value committed" handler again for the 2nd proposal.
        leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=2,
                                                                               election_term=term,
                                                                               attempt_number=1)
        propose(raftLog, leadProposalFromNode2, election, execute_request_task, expected_num_values_proposed=term)

        vote_proposal_future: asyncio.Future[LeaderElectionVote] = loop.create_future()

        async def mocked_append_election_vote(*args, **kwargs):
            unit_test_logger.debug(
                f"\nMocked RaftLog::_append_election_vote called with args {args} and kwargs {kwargs}.")
            vote_proposal_future.set_result(args[1])

        election_decision_future: Optional[asyncio.Future[LeaderElectionVote]] = raftLog.election_decision_future
        assert (election_decision_future is not None)

        execution_done_future: asyncio.Future[ExecutionCompleteNotification] = loop.create_future()

        async def mocked_raftlog_append_execution_end_notification(*args, **kwargs):
            unit_test_logger.debug(
                f"\n\nMocked RaftLog::append_execution_end_notification called with args {args} and kwargs {kwargs}.")
            execution_done_future.set_result(args[1])

        with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                                 _append_election_vote=mocked_append_election_vote,
                                 append_execution_end_notification=mocked_raftlog_append_execution_end_notification):

            assert raftLog.append_execution_end_notification is not None
            assert kernel.synclog.append_execution_end_notification is not None
            assert kernel.synchronizer._synclog.append_execution_end_notification is not None

            leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                                   proposer_id=3,
                                                                                   election_term=term,
                                                                                   attempt_number=1)
            propose(raftLog, leadProposalFromNode3, election, execute_request_task, expected_num_values_proposed=term)

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
            assert (len(raftLog._proposed_values) == term)
            assert election.is_active
            assert election.voting_phase_completed_successfully == False

            propose_vote(raftLog, proposedVote, leading_future, election, election_decision_future)

            try:
                # We'll wait up to 5 seconds, but it should happen very quickly.
                await asyncio.wait_for(leading_future, 5)
            except TimeoutError:
                unit_test_logger.debug(
                    "[ERROR] \"Leading\" future was not resolved. It should've been resolved by now.")
                assert False  # Fail the test.

            unit_test_logger.debug("\"Leading\" future should be done now.")
            assert leading_future.done() == True
            assert raftLog.leader_id == 1
            assert raftLog.leader_term == term
            wait, leading = raftLog._is_leading(term)
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
            assert notification.election_term == term

            for _, proposal in election.proposals.items():
                assert proposal.election_term == term
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

        # assert spoofed_session.num_send_calls == 5
        # assert len(spoofed_session.message_types_sent) == 5

        assert synchronizer.execution_count == term

        await asyncio.sleep(1)

        # Generate a new "execute_request".
        execution_request = create_execution_request(message_id=str(uuid.uuid4()))


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_propose_lead_and_lose(kernel: DistributedKernel, execution_request: Dict[str, Any]):
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

        assert raftLog.append_execution_end_notification is not None
        assert kernel.synclog.append_execution_end_notification is not None
        assert kernel.synchronizer._synclog.append_execution_end_notification is not None

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

        notification: ExecutionCompleteNotification = ExecutionCompleteNotification(
            execution_request['header']['msg_id'], proposer_id=2, election_term=1)

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
async def test_propose_yield_and_lose(kernel: DistributedKernel, execution_request: Dict[str, Any]):
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

        assert raftLog.append_execution_end_notification is not None
        assert kernel.synclog.append_execution_end_notification is not None
        assert kernel.synchronizer._synclog.append_execution_end_notification is not None

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

        notification: ExecutionCompleteNotification = ExecutionCompleteNotification(
            execution_request['header']['msg_id'], proposer_id=2, election_term=1)

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


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_election_fails_when_all_propose_yield(kernel: DistributedKernel, execution_request: Dict[str, Any]):
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

    assert raftLog.append_execution_end_notification is not None
    assert kernel.synclog.append_execution_end_notification is not None
    assert kernel.synchronizer._synclog.append_execution_end_notification is not None

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

    try:
        await asyncio.wait_for(execute_request_task, 5)
    except TimeoutError:
        unit_test_logger.debug("[ERROR] \"execute_request_task\" future was not resolved.")

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
async def test_all_propose_yield_and_win_second_round(kernel: DistributedKernel, execution_request: Dict[str, Any]):
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
    propose(raftLog, yieldProposalFromNode3, election, execute_request_task, expected_num_proposals_received=3,
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

    await asyncio.sleep(1)

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
            f"\n\nMocked RaftLog::append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             append_execution_end_notification=mocked_raftlog_append_execution_end_notification):

        assert raftLog.append_execution_end_notification is not None
        assert kernel.synclog.append_execution_end_notification is not None
        assert kernel.synchronizer._synclog.append_execution_end_notification is not None

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
        execution_request: Dict[str, Any],
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

    try:
        await asyncio.wait_for(execute_request_task, 5)
    except TimeoutError:
        unit_test_logger.debug("[ERROR] \"execute_request_task\" future was not resolved.")

        for task in asyncio.all_tasks():
            asyncio.Task.print_stack(task)
            unit_test_logger.debug("\n\n\n")

        assert False

    assert_election_failed(election, execute_request_task, election_decision_future, expected_proposer_id=1,
                           expected_term_number=current_election_term, expected_attempt_number=current_attempt_number,
                           expected_proposals_received=3)


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_fail_election_nine_times_then_win(kernel: DistributedKernel, execution_request: Dict[str, Any]):
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
            f"\n\nMocked RaftLog::append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             append_execution_end_notification=mocked_raftlog_append_execution_end_notification):

        assert raftLog.append_execution_end_notification is not None
        assert kernel.synclog.append_execution_end_notification is not None
        assert kernel.synchronizer._synclog.append_execution_end_notification is not None

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
async def test_election_success_after_timing_out(kernel: DistributedKernel, execution_request: Dict[str, Any]):
    """
    Test that an election which only receives 1/3 proposals will succeed after the timeout period elapses,
    if that one proposal is a LEAD proposal.
    """
    synchronizer: Synchronizer = kernel.synchronizer
    raftLog: RaftLog = kernel.synclog

    kernel.election_timeout_seconds = 1.0
    raftLog._election_timeout_sec = 1.0

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
            f"\n\nMocked RaftLog::append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             append_execution_end_notification=mocked_raftlog_append_execution_end_notification):
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
async def test_election_loss_buffer_one_lead_proposal(kernel: DistributedKernel, execution_request: Dict[str, Any]):
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
            f"\n\nMocked RaftLog::append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             append_execution_end_notification=mocked_raftlog_append_execution_end_notification):

        assert raftLog.append_execution_end_notification is not None
        assert kernel.synclog.append_execution_end_notification is not None
        assert kernel.synchronizer._synclog.append_execution_end_notification is not None

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

        notification: ExecutionCompleteNotification = ExecutionCompleteNotification(
            execution_request['header']['msg_id'], proposer_id=2, election_term=1)

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
async def test_election_win_buffer_one_yield_proposal(kernel: DistributedKernel, execution_request: Dict[str, Any]):
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
            f"\n\nMocked RaftLog::append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             append_execution_end_notification=mocked_raftlog_append_execution_end_notification):

        assert raftLog.append_execution_end_notification is not None
        assert kernel.synclog.append_execution_end_notification is not None
        assert kernel.synchronizer._synclog.append_execution_end_notification is not None

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
async def test_catch_up_after_migration(kernel: DistributedKernel, execution_request: Dict[str, Any]):
    """
    The election succeeds like normal after buffering two of the proposals.
    """
    global CommittedValues

    if os.environ.get("SIMULATE_CHECKPOINTING_LATENCY"):
        assert kernel.simulate_checkpointing_latency
    else:
        assert not kernel.simulate_checkpointing_latency

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

    write_data_dir_to_remote_storage_future: asyncio.Future[bytes] = loop.create_future()

    async def mocked_raftlog_write_data_dir_to_remote_storage(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::write_data_dir_to_remote_storage called with args {args} and kwargs {kwargs}.")

        assert isinstance(args[0], RaftLog)

        raftlog_state_serialized: bytes = args[0]._get_serialized_state(**kwargs)

        write_data_dir_to_remote_storage_future.set_result(raftlog_state_serialized)

        return FakePersistentStorePath

    # with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "close", mocked_raftlog_close):
    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             close=mocked_raftlog_close,
                             write_data_dir_to_remote_storage=mocked_raftlog_write_data_dir_to_remote_storage):
        await kernel.prepare_to_migrate_request(None, [], {})

    assert close_future.done()
    assert close_future.result() == 1

    assert write_data_dir_to_remote_storage_future.done()

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

    serialized_state: bytes = write_data_dir_to_remote_storage_future.result()
    assert serialized_state is not None
    assert isinstance(serialized_state, bytes)

    catchup_with_peers_future: asyncio.Future[int] = loop.create_future()

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

        if os.environ.get("SIMULATE_CHECKPOINTING_LATENCY"):
            assert new_kernel.simulate_checkpointing_latency
        else:
            assert not new_kernel.simulate_checkpointing_latency

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

        assert new_kernel.current_resource_request is not None
        assert new_kernel.resource_requests is not None
        assert len(new_kernel.resource_requests) > 0

        assert new_kernel.remote_storages is not None
        print("new_kernel.remote_storages:", new_kernel.remote_storages)
        assert len(new_kernel.remote_storages) == 1

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
            await asyncio.wait_for(catchup_awaitable, 10)
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

        assert new_kernel.remote_storages is not None
        assert len(new_kernel.remote_storages) == 1

        remote_storage: SimulatedCheckpointer = list(new_kernel.remote_storages.values())[0]
        assert remote_storage is not None

        if os.environ.get("SIMULATE_CHECKPOINTING_LATENCY"):
            assert new_kernel.simulate_checkpointing_latency
            assert remote_storage.total_num_read_ops == 2
            assert remote_storage.total_num_write_ops == 0

            assert len(remote_storage.read_latencies) == 2
            assert len(remote_storage.write_latencies) == 0

            # Should be about 2.
            # The max it should be about 2.631sec based on variance % and average rate.
            assert remote_storage.read_latencies[0] > 0
            assert remote_storage.read_latencies[1] > 0
        else:
            assert not new_kernel.simulate_checkpointing_latency


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_get_election_metadata(kernel: DistributedKernel, execution_request: Dict[str, Any]):
    """
    Test that getting the election metadata and serializing it to JSON works.
    """

    unit_test_logger.debug(f"Testing execute request with kernel {kernel} and execute request {execution_request}")

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
            f"\n\nMocked RaftLog::append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             append_execution_end_notification=mocked_raftlog_append_execution_end_notification):

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

    metadata: Dict[str, Any] = election.get_election_metadata()

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
async def test_skip_election_dropped_messages(kernel: DistributedKernel, execution_request: Dict[str, Any]):
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

    notification: ExecutionCompleteNotification = ExecutionCompleteNotification(execution_request['header']['msg_id'],
                                                                                proposer_id=2, election_term=1)
    commit_value(raftLog, notification)

    unit_test_logger.info(f"raftLog.num_elections_skipped = {raftLog.num_elections_skipped}")

    assert raftLog.current_election is not None
    assert raftLog.current_election.term_number == 1
    assert raftLog.current_election.winner_id == 2
    assert raftLog.current_election.was_skipped == True
    assert raftLog.num_elections_skipped == 1

    # TODO: Try to orchestrate election for term 2 successfully.


@mock.patch.object(distributed_notebook.sync.synchronizer.Synchronizer, "sync", mocked_sync)
@pytest.mark.asyncio
async def test_skip_election_delayed_messages(kernel: DistributedKernel, execution_request: Dict[str, Any]):
    """
    Test that an election can be skipped, and that the next election can proceed correctly if
    the "execute_request" and "yield_request" messages associated with the skipped election are received
    eventually, such as during the next election.
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

    notification: ExecutionCompleteNotification = ExecutionCompleteNotification(
        execution_request['header']['msg_id'],
        proposer_id=2, election_term=1)
    commit_value(raftLog, notification)

    unit_test_logger.info(f"raftLog.num_elections_skipped = {raftLog.num_elections_skipped}")

    assert raftLog.current_election is not None
    assert raftLog.current_election.term_number == 1
    assert raftLog.current_election.winner_id == 2
    assert raftLog.current_election.was_skipped == True
    assert raftLog.num_elections_skipped == 1

    # Now let's see what happens upon receiving the "execute_request" for election #1 late.
    # It should just be discarded/ignored.
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    execute_request_task: asyncio.Task[any] = loop.create_task(kernel.execute_request(None, [], execution_request))

    try:
        await asyncio.wait_for(execute_request_task, 5)
    except TimeoutError as ex:
        unit_test_logger.error("Timed out waiting for \"execute_request\" to be processed.")
        assert ex is False  # Fail

    assert execute_request_task.done()

    # Now let's try to submit a NEW "execute_request" message.
    new_execute_request: Dict[str, Any] = create_execution_request(message_id="f12bbdb3-a8df-493b-94e4-18b3110f0160")

    election_proposal_future: asyncio.Future[LeaderElectionProposal] = loop.create_future()

    async def mocked_append_election_proposal(*args, **kwargs):
        unit_test_logger.debug(
            f"\nMocked RaftLog::_append_election_proposal called with args {args} and kwargs {kwargs}.")
        election_proposal_future.set_result(args[1])

    with mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_append_election_proposal",
                           mocked_append_election_proposal):
        execute_request_task: asyncio.Task[any] = loop.create_task(
            kernel.execute_request(None, [], new_execute_request))
        proposedValue: LeaderElectionProposal = await election_proposal_future

        # Check that the kernel proposed a LEAD value.
        assert (proposedValue.key == str(ElectionProposalKey.LEAD))
        assert (proposedValue.proposer_id == kernel.smr_node_id)
        assert (proposedValue.election_term == 2)

        # Check that the kernel created an election, but that no proposals were received yet.
        election: Election = kernel.synclog.get_election(2)
        assert (election is not None)
        assert (election.term_number == 2)
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
                                                                               election_term=2,
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
            f"\n\nMocked RaftLog::append_execution_end_notification called with args {args} and kwargs {kwargs}.")
        execution_done_future.set_result(args[1])

    with mock.patch.multiple(distributed_notebook.sync.raft_log.RaftLog,
                             _append_election_vote=mocked_append_election_vote,
                             append_execution_end_notification=mocked_raftlog_append_execution_end_notification):
        assert raftLog.append_execution_end_notification is not None
        assert kernel.synclog.append_execution_end_notification is not None
        assert kernel.synchronizer._synclog.append_execution_end_notification is not None

        leadProposalFromNode3: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                               proposer_id=3,
                                                                               election_term=2,
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
        assert raftLog.leader_term == 2
        wait, leading = raftLog._is_leading(2)
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
        assert notification.election_term == 2

        for _, proposal in election.proposals.items():
            assert proposal.election_term == 2
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

    assert spoofed_session.num_send_calls == 7
    assert len(spoofed_session.message_types_sent) == 7

@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.parametrize("model_class,dataset_class", [
    (ResNet18, CIFAR10), (ResNet18, TinyImageNet),
    (InceptionV3, CIFAR10), (InceptionV3, TinyImageNet),
    (VGG11, CIFAR10), (VGG11, TinyImageNet),
    # (VGG13, CIFAR10), (VGG13, TinyImageNet),
    # (VGG16, CIFAR10), (VGG16, TinyImageNet),
    # (VGG19, CIFAR10), (VGG19, TinyImageNet),
    (Bert, IMDbLargeMovieReviewTruncated), (Bert, CoLA),
    (GPT2, IMDbLargeMovieReviewTruncated), (GPT2, CoLA),
    (DeepSpeech2, LibriSpeech)
])
@pytest.mark.asyncio
async def test_train_model_on_dataset(model_class: Type[DeepLearningModel], dataset_class: Type[CustomDataset]):
    await perform_training(model_class, dataset_class, target_training_duration_ms=2000.0)