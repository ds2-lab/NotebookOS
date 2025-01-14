import asyncio
import logging
import os
import sys
import uuid
from collections import OrderedDict
from typing import Optional, Dict, Any, Type, List
from unittest import mock
from itertools import islice

import pytest
import torch
from ipykernel.control import ControlThread

import distributed_notebook.sync.raft_log
from distributed_notebook.deep_learning import ResNet18, CIFAR10, DeepLearningModel, VGG16, InceptionV3, VGG19, VGG13, \
    VGG11, IMDbLargeMovieReviewTruncated, Bert, GPT2, DeepSpeech2, LibriSpeech, TinyImageNet, CoLA
from distributed_notebook.deep_learning.datasets.custom_dataset import CustomDataset
from distributed_notebook.kernel import DistributedKernel
from distributed_notebook.sync import Synchronizer, RaftLog, SyncAST
from distributed_notebook.sync.election import Election, ExecutionCompleted, AllReplicasProposedYield
from distributed_notebook.sync.log import ElectionProposalKey, LeaderElectionProposal, \
    LeaderElectionVote, Checkpointer, ExecutionCompleteNotification, SynchronizedValue
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
        use_real_gpus: bool = False,
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
        "use_real_gpus": use_real_gpus,
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

def moving_window(slice_, window_size):
    n = len(slice_)
    if n == 0 or window_size <= 0:
        raise ValueError("Slice must be non-empty and window size must be positive.")

    idx = 0  # Start index for the window
    while True:
        # Create the current window, wrapping around the slice using modulo arithmetic
        window = tuple(slice_[(idx + i) % n] for i in range(window_size))
        yield window
        idx += 1  # Move to the next starting position

async def perform_training(
        model_class: Type,
        dataset_class: Type,
        num_training_loops: int = 3,
        target_training_duration_ms: float = 1000.0,
        gpu_allocation_mode: str = "moving-window",
        gpu_allocation_window_size: int = 1,
        gpu_ids: Optional[List[int]] = None,
):
    """
    Helper/utility function to carry out a unit test in which a kernel proposes and leads the execution of
    some deep learning training code using a specified model and dataset.

    :param model_class: the specified model
    :param dataset_class: the specified dataset
    :param num_training_loops: how many times to execute
    :param target_training_duration_ms: how long each execution should aim to last
    :param gpu_allocation_mode: determines how GPUs are allocated. Options include "fixed", "moving-window", or "all".
                                the "moving-window" allocation mode functions like round-robin when the window size is
                                set to 1.
    :param gpu_allocation_window_size: window size when using the "moving-window" GPU allocation mode.
    :param gpu_ids: gpu device IDs to allocate when gpu_allocation_mode is specified as "fixed".
    :return:
    """
    kernel: DistributedKernel = await create_kernel(
        remote_storage_hostname="127.0.0.1:10000",
        kernel_id=DefaultKernelId,
        smr_port=8000,
        smr_node_id=1,
        smr_nodes=None,
        smr_join=False,
        should_register_with_local_daemon=False,
        pod_name="TestPod",
        node_name="TestNode",
        debug_port=-1,
        use_real_gpus=True,
        remote_storage="local",
        smr_enabled=True,
    )
    assert kernel is not None

    assert issubclass(model_class, DeepLearningModel)
    assert issubclass(dataset_class, CustomDataset)

    assert num_training_loops > 0
    assert target_training_duration_ms > 0

    # Default to device 0
    if gpu_ids is None:
        gpu_ids = [0]

    # only used for "moving-window" allocation mode
    device_ids_gen = moving_window(list(range(0, torch.cuda.device_count())), gpu_allocation_window_size)

    weights: Optional[torch.Tensor] = None
    for i in range(1, num_training_loops + 1):
        print(f'\n\n\nTraining Loop {i}/{num_training_loops} for Model "{model_class.model_name()}" on '
              f'Dataset "{dataset_class.dataset_name()}"\n\n')
        execution_request: Dict[str, Any] = create_execution_request(message_id=str(uuid.uuid4()))
        assert execution_request is not None

        # Update request metadata.
        metadata: Dict[str, Any] = execution_request["metadata"]
        assert metadata is not None
        metadata["model"] = model_class.model_name()
        metadata["dataset"] = dataset_class.dataset_name()

        if gpu_allocation_mode.lower().strip() == "fixed":
            # Allocate a fixed, specified set of GPUs.
            metadata["gpu_device_ids"] = gpu_ids
        elif gpu_allocation_mode.lower().strip() == "round-robin":
            # Cyclic moving window allocations.
            metadata["gpu_device_ids"] = next(device_ids_gen)
        elif gpu_allocation_mode.lower().strip() == "all":
            # Allocate all GPUs.
            metadata["gpu_device_ids"] = list(range(0, torch.cuda.device_count()))
        else:
            raise ValueError(f"Unknown or unsupported GPU allocation mode: '{gpu_allocation_mode}'")

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
    print(f'\n\n\n\n\n\nFinished test for training model "{model_class.model_name()}" on '
          f'dataset "{dataset_class.dataset_name()}"\n\n\n\n\n')


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

    propose(raftLog, proposedValue, election, execute_request_task,
            expected_num_values_proposed=expected_num_values_proposed)

    # Call "value committed" handler again for the 2nd proposal.
    leadProposalFromNode2: LeaderElectionProposal = LeaderElectionProposal(key=str(ElectionProposalKey.LEAD),
                                                                           proposer_id=2,
                                                                           election_term=term_number,
                                                                           attempt_number=1)
    propose(raftLog, leadProposalFromNode2, election, execute_request_task,
            expected_num_values_proposed=expected_num_values_proposed)

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
        propose(raftLog, leadProposalFromNode3, election, execute_request_task,
                expected_num_values_proposed=expected_num_values_proposed)

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


##################################
# Category: Computer Vision (CV)
# Dataset: CIFAR-10
##################################

@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_resnet18_on_cifar10():
    await perform_training(ResNet18, CIFAR10, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_vgg11_on_cifar10():
    await perform_training(VGG11, CIFAR10, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_vgg13_on_cifar10():
    await perform_training(VGG13, CIFAR10, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_vgg16_on_cifar10():
    await perform_training(VGG16, CIFAR10, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_vgg19_on_cifar10():
    await perform_training(VGG19, CIFAR10, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_inception_v3_on_cifar10():
    await perform_training(InceptionV3, CIFAR10, target_training_duration_ms=2000.0)


##################################
# Category: Computer Vision (CV)
# Dataset: Tiny ImageNet
##################################

@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_resnet18_on_tiny_imagenet():
    await perform_training(ResNet18, TinyImageNet, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_vgg11_on_tiny_imagenet():
    await perform_training(VGG11, TinyImageNet, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_vgg13_on_tiny_imagenet():
    await perform_training(VGG13, TinyImageNet, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_vgg16_on_tiny_imagenet():
    await perform_training(VGG16, TinyImageNet, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_vgg19_on_tiny_imagenet():
    await perform_training(VGG19, TinyImageNet, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_cv_inception_v3_on_tiny_imagenet():
    await perform_training(InceptionV3, TinyImageNet, target_training_duration_ms=2000.0)


#######################################################
# Category: Natural Language Processing (NLP)
# Dataset: IMDb Large Movie Review Dataset (Truncated)
#######################################################

@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_nlp_bert_on_truncated_imdb():
    await perform_training(Bert, IMDbLargeMovieReviewTruncated, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_nlp_gpt2_on_truncated_imdb():
    await perform_training(GPT2, IMDbLargeMovieReviewTruncated, target_training_duration_ms=2000.0)


#######################################################
# Category: Natural Language Processing (NLP)
# Dataset: Corpus of Linguistic Acceptability (CoLA)
#######################################################

@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_nlp_bert_on_cola():
    await perform_training(Bert, CoLA, target_training_duration_ms=2000.0)


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_nlp_gpt2_on_cola():
    await perform_training(GPT2, CoLA, target_training_duration_ms=2000.0)


###################################################
# Category: Speech
# Dataset: LibriSpeech
###################################################

@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., multiple GPUs)")
@pytest.mark.asyncio
async def test_train_speech_deep_speech2_on_libri_speech():
    await perform_training(DeepSpeech2, LibriSpeech, target_training_duration_ms=2000.0)
