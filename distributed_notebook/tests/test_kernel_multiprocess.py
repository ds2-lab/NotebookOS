import asyncio
import logging
import multiprocessing
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
from distributed_notebook.deep_learning.data.custom_dataset import CustomDataset
from distributed_notebook.kernel import DistributedKernel
from distributed_notebook.sync import Synchronizer, RaftLog, SyncAST
from distributed_notebook.sync.election import Election, ExecutionCompleted, AllReplicasProposedYield
from distributed_notebook.sync.log import ElectionProposalKey, LeaderElectionProposal, \
    LeaderElectionVote, Checkpointer, ExecutionCompleteNotification, SynchronizedValue
from distributed_notebook.tests.test_kernel import mocked_serialize_and_append_value, create_kernel, \
    create_execution_request, propose_lead_and_win
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


async def perform_training(
        process_identifier: int,
        model_class: Type,
        dataset_class: Type,
        num_training_loops: int = 3,
        target_training_duration_ms: float = 1000.0,
):
    """
    Helper/utility function to carry out a unit test in which a kernel proposes and leads the execution of
    some deep learning training code using a specified model and dataset.

    :param process_identifier: the identifier of the process. this is not the OS-level PID.
    :param model_class: the specified model
    :param dataset_class: the specified dataset
    :param num_training_loops: how many times to execute
    :param target_training_duration_ms: how long each execution should aim to last
    :return:
    """
    print(f"Process {process_identifier} has started running.")

    kernel: DistributedKernel = await create_kernel(
        remote_storage_hostname=f"127.0.0.{process_identifier}:10000",
        kernel_id=DefaultKernelId,
        smr_port=8000,
        smr_node_id=1,
        smr_nodes=None,
        smr_join=False,
        should_register_with_local_daemon=False,
        pod_name=f"TestPod-{process_identifier}",
        node_name=f"TestNode-{process_identifier}",
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

    weights: Optional[torch.Tensor] = None
    for i in range(1, num_training_loops + 1):
        print(f'\n\n\n{"\033[0;36m"}[PROCESS {process_identifier}] Training Loop {i}/{num_training_loops} for Model '
              f'"{model_class.model_name()}" on Dataset "{dataset_class.dataset_name()}"{"\033[0m"}\n\n')
        execution_request: Dict[str, Any] = create_execution_request(message_id=str(uuid.uuid4()))
        assert execution_request is not None

        # Update request metadata.
        metadata: Dict[str, Any] = execution_request["metadata"]
        assert metadata is not None
        metadata["model"] = model_class.model_name()
        metadata["dataset"] = dataset_class.dataset_name()
        metadata["gpu_device_ids"] = [process_identifier]

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
    print(f'{"\033[0;32m"}\n\n\n\n\n\n[PROCESS {process_identifier}] Finished test for training model '
          f'"{model_class.model_name()}" on dataset "{dataset_class.dataset_name()}"\n\n\n\n\n{"\033[0m"}')

    # The user_ns seems to persist between unit tests sometimes...
    # Not sure why, but we clear it here to prevent any issues.
    kernel.shell.user_ns.clear()


@mock.patch.object(distributed_notebook.sync.raft_log.RaftLog, "_serialize_and_append_value",
                   mocked_serialize_and_append_value)
@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires >= 2 torch.cuda.devices (i.e., 2+ GPUs)")
def test_train_resnet_cifar10():
    """
    Create two processes, each of which houses a DistributedKernel that will train ResNet18 on CIFAR-10.
    """

    p1_args = (0, ResNet18, CIFAR10)
    kwargs = {
        "num_training_loops": 3,
        "target_training_duration_ms": 10000,
    }
    p1: multiprocessing.Process = multiprocessing.Process(target=perform_training, args=p1_args, kwargs=kwargs)

    p2_args = (1, ResNet18, CIFAR10)
    kwargs = {
        "num_training_loops": 3,
        "target_training_duration_ms": 10000,
    }
    p2: multiprocessing.Process = multiprocessing.Process(target=perform_training, args=p2_args, kwargs=kwargs)

    p1.start()
    p2.start()

    p1.join()
    p2.join()