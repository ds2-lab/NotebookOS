import fakeredis
import torch
import asyncio
import random
import types
import uuid
from typing import Any, Optional, Type
from unittest import mock

import builtins as builtin_mod
import pytest
from moto import mock_aws
from torch import Tensor
from torch.nn import Parameter

import distributed_notebook
import distributed_notebook.sync
from distributed_notebook.deep_learning import ResNet18, VGG11, InceptionV3, \
    Bert, IMDbLargeMovieReviewTruncated, GPT2, LibriSpeech, CIFAR10, DeepSpeech2, TinyImageNet, \
    CoLA, get_model_and_dataset
from distributed_notebook.deep_learning.data import load_dataset
from distributed_notebook.deep_learning.data.custom_dataset import CustomDataset
from distributed_notebook.deep_learning.models.loader import load_model
from distributed_notebook.deep_learning.models.model import DeepLearningModel
from distributed_notebook.deep_learning.models.simple_model import SimpleModel
from distributed_notebook.sync import Synchronizer, RaftLog
from distributed_notebook.sync.checkpointing.checkpointer import Checkpointer
from distributed_notebook.sync.checkpointing.pointer import ModelPointer
from distributed_notebook.sync.checkpointing.pointer import SyncPointer, DatasetPointer
from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer
from distributed_notebook.sync.log import SynchronizedValue, SyncLog
from distributed_notebook.sync.object import SyncObjectMeta
from distributed_notebook.sync.raft_log import RaftLog
from distributed_notebook.sync.remote_storage.local_provider import LocalStorageProvider
from distributed_notebook.sync.remote_storage.redis_provider import RedisProvider
from distributed_notebook.sync.remote_storage.s3_provider import S3Provider
from distributed_notebook.sync.remote_storage_log import RemoteStorageLog
from distributed_notebook.sync.synchronizer import CHECKPOINT_AUTO

from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import Any, TypeVar

import aiobotocore
import aiobotocore.endpoint
import aiobotocore.httpchecksum
import aiobotocore.response
import botocore.awsrequest
import botocore.httpchecksum


T = TypeVar("T")
R = TypeVar("R")

# Need to batch
# https://github.com/aio-libs/aiobotocore/issues/755#issuecomment-2602810530

@dataclass
class _PatchedAWSResponseContent:
    """Patched version of `botocore.awsrequest.AWSResponse.content`"""

    content: bytes | Awaitable[bytes]

    def __await__(self) -> Iterator[bytes]:
        async def _generate_async() -> bytes:
            if isinstance(self.content, Awaitable):
                return await self.content
            else:
                return self.content

        return _generate_async().__await__()

    def decode(self, encoding: str) -> str:
        assert isinstance(self.content, bytes)
        return self.content.decode(encoding)


class PatchedAWSResponse:
    """Patched version of `botocore.awsrequest.AWSResponse`"""

    def __init__(self, response: botocore.awsrequest.AWSResponse) -> None:
        self._response = response
        self.status_code = response.status_code
        self.content = _PatchedAWSResponseContent(response.content)
        self.raw = response.raw
        if not hasattr(self.raw, "raw_headers"):
            self.raw.raw_headers = {}


def _factory(
        original: Callable[[botocore.awsrequest.AWSResponse, T], Awaitable[R]],
) -> Callable[[botocore.awsrequest.AWSResponse, T], Awaitable[R]]:
    """Factory for patching `aiobotocore.endpoint.convert_to_response_dict`"""

    async def patched_convert_to_response_dict(http_response: botocore.awsrequest.AWSResponse, operation_model: T) -> R:
        return await original(PatchedAWSResponse(http_response), operation_model)  # type: ignore[arg-type]

    return patched_convert_to_response_dict


aiobotocore.endpoint.convert_to_response_dict = _factory(aiobotocore.endpoint.convert_to_response_dict)  # type: ignore[assignment]


async def _patched_read(self: aiobotocore.response.StreamingBody, _amt: Any = None) -> Any:
    """Patched version of `aiobotocore.response.StreamingBody.read`"""
    return self.__wrapped__.read()


aiobotocore.response.StreamingBody.read = _patched_read  # type: ignore[assignment]

# Remove the async version of the function, which is not compatible with Moto.
del aiobotocore.httpchecksum.AioAwsChunkedWrapper._make_chunk  # noqa: SLF001

example_data: dict[str, Any] = {
    "Name": ["Alice", "Bob", "Charlie", "Dave"],
    "Age": [25, 32, 22, 45],
    "City": ["New York", "Los Angeles", "Chicago", "Miami"],
    "Salary": [55000, 74000, 48000, 66000],
}

KERNEL_ID:str = str(uuid.uuid4())

class DummyObject(object):
    def __init__(self, n: int = 10, lst: Optional[list[int]] = None):
        if lst is not None:
            self.lst = lst
        else:
            self.lst: list[int] = []
            for _ in range(0, n):
                self.lst.append(random.randrange(0, 512))

    def __len__(self) -> int:
        return len(self.lst)

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return f"DummyObject[lst={self.lst}]"

@pytest.fixture(autouse=True)
def moto_boto():
    """
    Ensures that we're mocking AWS S3.
    """
    with mock_aws():
        yield

def loaded_serialized_state_callback(state=None):
    pass


def large_object_pointer_committed(
        pointer: SyncPointer,
        checkpointer: Checkpointer,
        existing_model: Optional[DeepLearningModel] = None,
) -> Optional[CustomDataset | DeepLearningModel]:
    """
    Callback to be executed when a pointer to a large object is committed to the RaftLog.
    """
    if isinstance(pointer, DatasetPointer):
        return load_dataset(pointer.dataset_description)
    elif isinstance(pointer, ModelPointer):
        model_state_dict, optimizer_state_dict, criterion_state_dict, constructor_args_dict = (
            checkpointer.read_state_dicts(pointer)
        )

        print(
            f'Passing the following keyword arguments to load_model function for model "{pointer.large_object_name}":')
        for k, v in constructor_args_dict.items():
            print(f'\t"{k}": {v}', flush=True)

        return load_model(
            model_name=pointer.large_object_name,
            existing_model=existing_model,
            # commented out:
            # out_features should/will be passed via the constructor_args_state dictionary.
            # out_features=pointer.out_features,
            model_state_dict=model_state_dict,
            optimizer_state_dict=optimizer_state_dict,
            criterion_state_dict=criterion_state_dict,
            **constructor_args_dict,  # out_features should/will be in this dictionary.
        )
    else:
        print(f"SyncPointer of unknown or unsupported type committed: {pointer}")
        raise ValueError(
            f"SyncPointer of unknown or unsupported type committed: {pointer}"
        )


def report_error(title: str, msg: str):
    print('Reporting error "{title}": "{msg}"')


def send_notifcation(title: str, msg: str, notification_type: int):
    print('Send notification: "{title}": "{msg}" (type={notification_type})')


def prepare_user_module():
    user_module: types.ModuleType = types.ModuleType(
        "__main__",
        doc="Automatically created module for IPython interactive environment",
    )

    # We must ensure that __builtin__ (without the final 's') is always
    # available and pointing to the __builtin__ *module*.  For more details:
    # http://mail.python.org/pipermail/python-dev/2001-April/014068.html
    user_module.__dict__.setdefault("__builtin__", builtin_mod)
    user_module.__dict__.setdefault("__builtins__", builtin_mod)
    user_ns = user_module.__dict__

    return user_module, user_ns


def __get_raft_log() -> SyncLog|RaftLog|RemoteStorageLog:
    raft_log: SyncLog|RaftLog|RemoteStorageLog = RemoteStorageLog(
        node_id=1,
        kernel_id=KERNEL_ID,
        skip_create_log_node=True,
        base_path="./store",
    )

    return raft_log


def __get_synchronizer(
        sync_log: SyncLog|RaftLog|RemoteStorageLog,
        user_module: types.ModuleType,
        checkpointer: Checkpointer,
) -> Synchronizer:
    def pointer_committed(pointer: SyncPointer):
        return large_object_pointer_committed(pointer, checkpointer)

    synchronizer: Synchronizer = Synchronizer(
        sync_log,
        store_path="./store",
        module=user_module,
        opts=CHECKPOINT_AUTO,
        node_id=1,
        large_object_pointer_committed=pointer_committed,
        remote_checkpointer=checkpointer,
    )
    assert synchronizer is not None

    return synchronizer


def synchronize_variable(
        io_loop: asyncio.AbstractEventLoop,
        synchronizer: Synchronizer,
        raft_log: SyncLog|RaftLog|RemoteStorageLog,
        val: Any,
        meta: SyncObjectMeta,
        key: str = "my_var",
):
    append_future = io_loop.create_future()

    async def mocked_append(rlog: SyncLog|RaftLog|RemoteStorageLog, val: SynchronizedValue):
        print(f"Mocked RaftLog::append called with RaftLog {rlog} and SynchronizedValue {val}")
        append_future.set_result(val)

    if isinstance(synchronizer.synclog, RaftLog):
        with mock.patch.object(
                distributed_notebook.sync.raft_log.RaftLog, "append", mocked_append
        ):
            io_loop.run_until_complete(
                synchronizer.sync_key(
                    sync_log=raft_log,
                    key=key,
                    val=val,
                    end_execution=True,
                    checkpointing=False,
                    meta=meta,
                )
            )
    else:
        with mock.patch.object(
                distributed_notebook.sync.remote_storage_log.RemoteStorageLog, "append", mocked_append
        ):
            io_loop.run_until_complete(
                synchronizer.sync_key(
                    sync_log=raft_log,
                    key=key,
                    val=val,
                    end_execution=True,
                    checkpointing=False,
                    meta=meta,
                )
            )

    assert append_future.done()

    synchronized_key: SynchronizedValue = append_future.result()
    print(f"synchronized_key: {synchronized_key}")

    synchronizer.change_handler(synchronized_key, restoring=False)


def test_sync_and_change_int_variable():
    """
    Test calling sync_key followed by change_handler for an int variable.
    """
    local_provider: LocalStorageProvider = LocalStorageProvider()
    local_checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)
    assert local_checkpointer is not None

    raft_log: SyncLog|RaftLog|RemoteStorageLog = __get_raft_log()
    assert raft_log is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        raft_log, user_module, local_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    io_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    val: int = 3

    synchronize_variable(
        io_loop=io_loop,
        synchronizer=synchronizer,
        raft_log=raft_log,
        val=val,
        meta=meta,
    )

    assert "my_var" in synchronizer.global_ns
    assert "my_var" in user_ns
    assert hasattr(user_module, "my_var")

    assert user_ns["my_var"] == val
    assert isinstance(user_ns["my_var"], int)

    assert synchronizer.global_ns["my_var"] == val
    assert isinstance(synchronizer.global_ns["my_var"], int)

    assert user_module.my_var == val
    assert isinstance(user_module.my_var, int)

    # Do this for many iterations.
    next_val = val
    for _ in range(0, 10):
        synchronize_variable(
            io_loop=io_loop,
            synchronizer=synchronizer,
            raft_log=raft_log,
            val=next_val,
            meta=meta,
        )

        assert "my_var" in synchronizer.global_ns
        assert "my_var" in user_ns
        assert hasattr(user_module, "my_var")

        assert user_ns["my_var"] == next_val
        assert isinstance(user_ns["my_var"], int)

        assert synchronizer.global_ns["my_var"] == next_val
        assert isinstance(synchronizer.global_ns["my_var"], int)

        assert user_module.my_var == next_val
        assert isinstance(user_module.my_var, int)

        next_val = val + 1


def test_sync_and_change_dummy_object_variable():
    """
    Test calling sync_key followed by change_handler for an DummyObject variable.
    """
    local_provider: LocalStorageProvider = LocalStorageProvider()
    local_checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)
    assert local_checkpointer is not None

    raft_log: SyncLog|RaftLog|RemoteStorageLog = __get_raft_log()
    assert raft_log is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        raft_log, user_module, local_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    io_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    dummy_obj: DummyObject = DummyObject(lst=[1, 2, 3, 4])

    synchronize_variable(
        io_loop=io_loop,
        synchronizer=synchronizer,
        raft_log=raft_log,
        val=dummy_obj,
        meta=meta,
    )

    assert "my_var" in synchronizer.global_ns
    assert "my_var" in user_ns
    assert hasattr(user_module, "my_var")

    assert user_ns["my_var"] == dummy_obj
    assert isinstance(user_ns["my_var"], DummyObject)

    assert synchronizer.global_ns["my_var"] == dummy_obj
    assert isinstance(synchronizer.global_ns["my_var"], DummyObject)

    assert user_module.my_var == dummy_obj
    assert isinstance(user_module.my_var, DummyObject)

    assert user_ns["my_var"].lst is not None
    assert len(user_ns["my_var"].lst) == 4
    assert user_ns["my_var"].lst == [1, 2, 3, 4]
    assert user_ns["my_var"].lst == dummy_obj.lst

    # Do this for 10 iterations.
    for i in range(0, 10):
        # Update the variable, then we'll re-sync it.
        lst = []
        test_lst = []
        for elem in dummy_obj.lst:
            new_elem = elem + 1

            lst.append(new_elem)
            test_lst.append(new_elem)

        dummy_obj.lst = lst

        synchronize_variable(
            io_loop=io_loop,
            synchronizer=synchronizer,
            raft_log=raft_log,
            val=dummy_obj,
            meta=meta,
        )

        assert "my_var" in synchronizer.global_ns
        assert "my_var" in user_ns
        assert hasattr(user_module, "my_var")

        assert user_ns["my_var"] == dummy_obj
        assert isinstance(user_ns["my_var"], DummyObject)

        assert synchronizer.global_ns["my_var"] == dummy_obj
        assert isinstance(synchronizer.global_ns["my_var"], DummyObject)

        assert user_module.my_var == dummy_obj
        assert isinstance(user_module.my_var, DummyObject)

        assert user_ns["my_var"].lst is not None
        assert len(user_ns["my_var"].lst) == len(test_lst)
        assert user_ns["my_var"].lst == test_lst
        assert user_ns["my_var"].lst == dummy_obj.lst

        assert synchronizer.global_ns["my_var"].lst is not None
        assert len(synchronizer.global_ns["my_var"].lst) == len(test_lst)
        assert synchronizer.global_ns["my_var"].lst == test_lst
        assert synchronizer.global_ns["my_var"].lst == dummy_obj.lst

        assert user_module.my_var.lst is not None
        assert len(user_module.my_var.lst) == len(test_lst)
        assert user_module.my_var.lst == test_lst
        assert user_module.my_var.lst == dummy_obj.lst

async def mocked_s3_provider_write_value_async(s3_provider: S3Provider, key: str, value: Any, size_bytes:int = -1):
    return s3_provider.write_value(key, value, size_bytes)

async def mocked_s3_provider_read_value_async(s3_provider: S3Provider, key: str):
    return s3_provider.read_value(key)

@mock.patch.object(distributed_notebook.sync.remote_storage.s3_provider.S3Provider, "write_value_async",
                   mocked_s3_provider_write_value_async)
@mock.patch.object(distributed_notebook.sync.remote_storage.s3_provider.S3Provider, "read_value_async",
                   mocked_s3_provider_read_value_async)
@mock_aws
def test_sync_and_change_large_deep_learning_model_s3():
    """
    Test calling sync_key followed by change_handler for a DeepLearningModel variable.
    """
    sync_log: RemoteStorageLog = RemoteStorageLog(
        node_id=1,
        remote_storage_provider=S3Provider(),
        base_path="./store",
        prometheus_enabled=False,
        kernel_id=KERNEL_ID,
        workload_id=str(uuid.uuid4()),
        remote_storage_write_latency_milliseconds=None,
    )
    assert sync_log is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    s3_provider: S3Provider() = S3Provider()
    s3_checkpointer: RemoteCheckpointer = RemoteCheckpointer(s3_provider)
    assert s3_checkpointer is not None

    synchronizer: Synchronizer = __get_synchronizer(
        sync_log, user_module, s3_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    io_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    # Create the model.
    initial_weights: float = 1.5
    model: Bert = Bert(out_features=2)

    output_layer = model.output_layer
    for param in output_layer.parameters():
        param.data.fill_(initial_weights)

    weight: Parameter = model.output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    model.requires_checkpointing = True

    synchronize_variable(
        io_loop=io_loop,
        synchronizer=synchronizer,
        raft_log=sync_log,
        val=model,
        meta=meta,
        key="model",
    )

    print(f"synchronizer.global_ns: {synchronizer.global_ns}")
    print(f"user_ns: {user_ns}")
    print(f"user_module: {user_module}")

    assert "model" in synchronizer.global_ns
    assert "model" in user_ns
    assert hasattr(user_module, "model")

    assert isinstance(user_ns["model"], Bert)
    assert isinstance(synchronizer.global_ns["model"], Bert)
    assert isinstance(user_module.model, Bert)

    weight: Parameter = user_ns["model"].output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    weight: Parameter = synchronizer.global_ns["model"].output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    weight: Parameter = user_module.model.output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    updated_weights: float = initial_weights

    # Do this for just 3 iterations.
    for i in range(0, 3):
        updated_weights = updated_weights + 1

        with torch.no_grad():  # Manually modify weights
            for param in model.model.parameters():
                # Random weight updates
                param.data.fill_(updated_weights)

        weight: Parameter = model.output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        model.requires_checkpointing = True

        synchronize_variable(
            io_loop=io_loop,
            synchronizer=synchronizer,
            raft_log=sync_log,
            val=model,
            meta=meta,
            key="model",
        )

        print(f"synchronizer.global_ns: {synchronizer.global_ns}")
        print(f"user_ns: {user_ns}")
        print(f"user_module: {user_module}")

        assert "model" in synchronizer.global_ns
        assert "model" in user_ns
        assert hasattr(user_module, "model")

        assert isinstance(user_ns["model"], Bert)
        assert isinstance(synchronizer.global_ns["model"], Bert)
        assert isinstance(user_module.model, Bert)

        weight: Parameter = user_ns["model"].output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        weight: Parameter = synchronizer.global_ns["model"].output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        weight: Parameter = user_module.model.output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

def test_sync_and_change_large_deep_learning_model_redis():
    """
    Test calling sync_key followed by change_handler for a DeepLearningModel variable.
    """
    server = fakeredis.FakeServer()

    sync_log_redis_provider: RedisProvider = RedisProvider(
        redis_client=fakeredis.FakeStrictRedis(server=server),
        async_redis_client=fakeredis.FakeAsyncRedis(server=server),
    )

    sync_log: RemoteStorageLog = RemoteStorageLog(
        node_id=1,
        remote_storage_provider=sync_log_redis_provider,
        base_path="./store",
        prometheus_enabled=False,
        kernel_id=KERNEL_ID,
        workload_id=str(uuid.uuid4()),
        remote_storage_write_latency_milliseconds=None,
    )
    assert sync_log is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer_redis_provider: RedisProvider = RedisProvider(
        redis_client=fakeredis.FakeStrictRedis(server=server),
        async_redis_client=fakeredis.FakeAsyncRedis(server=server),
    )
    synchronizer_redis_checkpointer: RemoteCheckpointer = RemoteCheckpointer(synchronizer_redis_provider)
    assert synchronizer_redis_checkpointer is not None

    synchronizer: Synchronizer = __get_synchronizer(
        sync_log, user_module, synchronizer_redis_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    io_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    # Create the model.
    initial_weights: float = 1.5
    model: Bert = Bert(out_features=2)

    output_layer = model.output_layer
    for param in output_layer.parameters():
        param.data.fill_(initial_weights)

    weight: Parameter = model.output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    model.requires_checkpointing = True

    synchronize_variable(
        io_loop=io_loop,
        synchronizer=synchronizer,
        raft_log=sync_log,
        val=model,
        meta=meta,
        key="model",
    )

    print(f"synchronizer.global_ns: {synchronizer.global_ns}")
    print(f"user_ns: {user_ns}")
    print(f"user_module: {user_module}")

    assert "model" in synchronizer.global_ns
    assert "model" in user_ns
    assert hasattr(user_module, "model")

    assert isinstance(user_ns["model"], Bert)
    assert isinstance(synchronizer.global_ns["model"], Bert)
    assert isinstance(user_module.model, Bert)

    weight: Parameter = user_ns["model"].output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    weight: Parameter = synchronizer.global_ns["model"].output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    weight: Parameter = user_module.model.output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    updated_weights: float = initial_weights

    # Do this for just 3 iterations.
    for i in range(0, 3):
        updated_weights = updated_weights + 1

        with torch.no_grad():  # Manually modify weights
            for param in model.model.parameters():
                # Random weight updates
                param.data.fill_(updated_weights)

        weight: Parameter = model.output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        model.requires_checkpointing = True

        synchronize_variable(
            io_loop=io_loop,
            synchronizer=synchronizer,
            raft_log=sync_log,
            val=model,
            meta=meta,
            key="model",
        )

        print(f"synchronizer.global_ns: {synchronizer.global_ns}")
        print(f"user_ns: {user_ns}")
        print(f"user_module: {user_module}")

        assert "model" in synchronizer.global_ns
        assert "model" in user_ns
        assert hasattr(user_module, "model")

        assert isinstance(user_ns["model"], Bert)
        assert isinstance(synchronizer.global_ns["model"], Bert)
        assert isinstance(user_module.model, Bert)

        weight: Parameter = user_ns["model"].output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        weight: Parameter = synchronizer.global_ns["model"].output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        weight: Parameter = user_module.model.output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

def test_sync_and_change_large_deep_learning_model_local():
    """
    Test calling sync_key followed by change_handler for a DeepLearningModel variable.
    """
    local_provider: LocalStorageProvider = LocalStorageProvider()
    local_checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)
    assert local_checkpointer is not None

    sync_log: RemoteStorageLog = RemoteStorageLog(
        node_id=1,
        remote_storage_provider=LocalStorageProvider(),
        base_path="./store",
        prometheus_enabled=False,
        kernel_id=KERNEL_ID,
        workload_id=str(uuid.uuid4()),
        remote_storage_write_latency_milliseconds=None,
    )
    assert sync_log is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        sync_log, user_module, local_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    io_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    # Create the model.
    initial_weights: float = 1.5
    model: Bert = Bert(out_features=2)

    output_layer = model.output_layer
    for param in output_layer.parameters():
        param.data.fill_(initial_weights)

    weight: Parameter = model.output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    model.requires_checkpointing = True

    synchronize_variable(
        io_loop=io_loop,
        synchronizer=synchronizer,
        raft_log=sync_log,
        val=model,
        meta=meta,
        key="model",
    )

    print(f"synchronizer.global_ns: {synchronizer.global_ns}")
    print(f"user_ns: {user_ns}")
    print(f"user_module: {user_module}")

    assert "model" in synchronizer.global_ns
    assert "model" in user_ns
    assert hasattr(user_module, "model")

    assert isinstance(user_ns["model"], Bert)
    assert isinstance(synchronizer.global_ns["model"], Bert)
    assert isinstance(user_module.model, Bert)

    weight: Parameter = user_ns["model"].output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    weight: Parameter = synchronizer.global_ns["model"].output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    weight: Parameter = user_module.model.output_layer.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    updated_weights: float = initial_weights

    # Do this for 3 iterations.
    for i in range(0, 3):
        updated_weights = updated_weights + 1

        with torch.no_grad():  # Manually modify weights
            for param in model.model.parameters():
                # Random weight updates
                param.data.fill_(updated_weights)

        weight: Parameter = model.output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        model.requires_checkpointing = True

        synchronize_variable(
            io_loop=io_loop,
            synchronizer=synchronizer,
            raft_log=sync_log,
            val=model,
            meta=meta,
            key="model",
        )

        print(f"synchronizer.global_ns: {synchronizer.global_ns}")
        print(f"user_ns: {user_ns}")
        print(f"user_module: {user_module}")

        assert "model" in synchronizer.global_ns
        assert "model" in user_ns
        assert hasattr(user_module, "model")

        assert isinstance(user_ns["model"], Bert)
        assert isinstance(synchronizer.global_ns["model"], Bert)
        assert isinstance(user_module.model, Bert)

        weight: Parameter = user_ns["model"].output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        weight: Parameter = synchronizer.global_ns["model"].output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        weight: Parameter = user_module.model.output_layer.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

def test_sync_and_change_deep_learning_model():
    """
    Test calling sync_key followed by change_handler for a DeepLearningModel variable.
    """
    local_provider: LocalStorageProvider = LocalStorageProvider()
    local_checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)
    assert local_checkpointer is not None

    raft_log: SyncLog|RaftLog|RemoteStorageLog = __get_raft_log()
    assert raft_log is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        raft_log, user_module, local_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    io_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    # Create the model.
    input_size: int = 4
    initial_weights: float = 1.5
    initial_bias: float = 2.5
    model: SimpleModel = SimpleModel(
        input_size=input_size,
        out_features=1,
        created_for_first_time=True,
        initial_weights=initial_weights,
        initial_bias=initial_bias
    )

    weight: Parameter = model.model.fc.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    for b in model.model.fc.bias.data:
        assert b == initial_bias

    synchronize_variable(
        io_loop=io_loop,
        synchronizer=synchronizer,
        raft_log=raft_log,
        val=model,
        meta=meta,
        key="model",
    )

    print(f"synchronizer.global_ns: {synchronizer.global_ns}")
    print(f"user_ns: {user_ns}")
    print(f"user_module: {user_module}")

    assert "model" in synchronizer.global_ns
    assert "model" in user_ns
    assert hasattr(user_module, "model")

    assert isinstance(user_ns["model"], SimpleModel)
    assert isinstance(synchronizer.global_ns["model"], SimpleModel)
    assert isinstance(user_module.model, SimpleModel)

    weight: Parameter = user_ns["model"].model.fc.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    for b in user_ns["model"].model.fc.bias.data:
        assert b == initial_bias

    weight: Parameter = synchronizer.global_ns["model"].model.fc.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    for b in synchronizer.global_ns["model"].model.fc.bias.data:
        assert b == initial_bias

    weight: Parameter = user_module.model.model.fc.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    for b in user_module.model.model.fc.bias.data:
        assert b == initial_bias

    updated_weights: float = initial_weights
    updated_bias: float = initial_weights

    # Do this for 10 iterations.
    for i in range(0, 10):
        updated_weights = updated_weights + 1
        updated_bias = updated_bias + 1

        model.set_weights(updated_weights)
        model.set_bias(updated_bias)

        weight: Parameter = model.model.fc.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        for b in model.model.fc.bias.data:
            assert b == updated_bias

        synchronize_variable(
            io_loop=io_loop,
            synchronizer=synchronizer,
            raft_log=raft_log,
            val=model,
            meta=meta,
            key="model",
        )

        print(f"synchronizer.global_ns: {synchronizer.global_ns}")
        print(f"user_ns: {user_ns}")
        print(f"user_module: {user_module}")

        assert "model" in synchronizer.global_ns
        assert "model" in user_ns
        assert hasattr(user_module, "model")

        assert isinstance(user_ns["model"], SimpleModel)
        assert isinstance(synchronizer.global_ns["model"], SimpleModel)
        assert isinstance(user_module.model, SimpleModel)

        weight: Parameter = user_ns["model"].model.fc.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        for b in user_ns["model"].model.fc.bias.data:
            assert b == updated_bias

        weight: Parameter = synchronizer.global_ns["model"].model.fc.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        for b in synchronizer.global_ns["model"].model.fc.bias.data:
            assert b == updated_bias

        weight: Parameter = user_module.model.model.fc.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        for b in user_module.model.model.fc.bias.data:
            assert b == updated_bias


def test_sync_and_change_deep_learning_model_new_model_obj():
    """
    Test calling sync_key followed by change_handler for a DeepLearningModel variable.

    In this version of the unit test, we create a whole new SimpleModel object with new weights and sync that
    during the second round, rather than modify the weights of the original object and re-sync it.
    """
    local_provider: LocalStorageProvider = LocalStorageProvider()
    local_checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)
    assert local_checkpointer is not None

    raft_log: SyncLog|RaftLog|RemoteStorageLog = __get_raft_log()
    assert raft_log is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        raft_log, user_module, local_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    io_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    # Create the model.
    input_size: int = 4
    initial_weights: float = 1.5
    initial_bias: float = 2.5
    model: SimpleModel = SimpleModel(
        input_size=input_size,
        out_features=1,
        created_for_first_time=True,
        initial_weights=initial_weights,
        initial_bias=initial_bias
    )

    weight: Parameter = model.model.fc.weight
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    for b in model.model.fc.bias.data:
        assert b == initial_bias

    synchronize_variable(
        io_loop=io_loop,
        synchronizer=synchronizer,
        raft_log=raft_log,
        val=model,
        meta=meta,
        key="model",
    )

    print(f"synchronizer.global_ns: {synchronizer.global_ns}")
    print(f"user_ns: {user_ns}")
    print(f"user_module: {user_module}")

    assert "model" in synchronizer.global_ns
    assert "model" in user_ns
    assert hasattr(user_module, "model")

    assert isinstance(user_ns["model"], SimpleModel)
    assert isinstance(synchronizer.global_ns["model"], SimpleModel)
    assert isinstance(user_module.model, SimpleModel)

    weight: Parameter = user_ns["model"].model.fc.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    for b in user_ns["model"].model.fc.bias.data:
        assert b == initial_bias

    weight: Parameter = synchronizer.global_ns["model"].model.fc.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    for b in synchronizer.global_ns["model"].model.fc.bias.data:
        assert b == initial_bias

    weight: Parameter = user_module.model.model.fc.weight.detach().cpu().clone()
    for weight_vector in weight.data:
        for w in weight_vector:
            assert w == initial_weights

    for b in user_module.model.model.fc.bias.data:
        assert b == initial_bias

    updated_weights: float = initial_weights
    updated_bias: float = initial_bias

    # Do this for 10 iterations.
    for i in range(0, 10):
        updated_weights = updated_weights + 1
        updated_bias = updated_bias + 1

        model = SimpleModel(
            input_size=input_size,
            out_features=1,
            created_for_first_time=True,
            initial_weights=updated_weights,
            initial_bias=updated_bias
        )

        weight: Parameter = model.model.fc.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        for b in model.model.fc.bias.data:
            assert b == updated_bias

        synchronize_variable(
            io_loop=io_loop,
            synchronizer=synchronizer,
            raft_log=raft_log,
            val=model,
            meta=meta,
            key="model",
        )

        print(f"synchronizer.global_ns: {synchronizer.global_ns}")
        print(f"user_ns: {user_ns}")
        print(f"user_module: {user_module}")

        assert "model" in synchronizer.global_ns
        assert "model" in user_ns
        assert hasattr(user_module, "model")

        assert isinstance(user_ns["model"], SimpleModel)
        assert isinstance(synchronizer.global_ns["model"], SimpleModel)
        assert isinstance(user_module.model, SimpleModel)

        weight: Parameter = user_ns["model"].model.fc.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        for b in user_ns["model"].model.fc.bias.data:
            assert b == updated_bias

        weight: Parameter = synchronizer.global_ns["model"].model.fc.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        for b in synchronizer.global_ns["model"].model.fc.bias.data:
            assert b == updated_bias

        weight: Parameter = user_module.model.model.fc.weight.detach().cpu().clone()
        for weight_vector in weight.data:
            for w in weight_vector:
                assert w == updated_weights

        for b in user_module.model.model.fc.bias.data:
            assert b == updated_bias


def train_and_sync_model(
        model_class: Type,
        dataset_class: Type,
        num_training_loops: int = 5,
        target_training_duration_ms: float = 1000.0
):
    assert issubclass(model_class, DeepLearningModel)
    assert issubclass(dataset_class, CustomDataset)

    local_provider: LocalStorageProvider = LocalStorageProvider()
    local_checkpointer: RemoteCheckpointer = RemoteCheckpointer(local_provider)
    assert local_checkpointer is not None

    raft_log: SyncLog|RaftLog|RemoteStorageLog = __get_raft_log()
    assert raft_log is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        raft_log, user_module, local_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    io_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    # Create the model.
    # model: DeepLearningModel = model_class(created_for_first_time=True)
    # initial_weights: Tensor = model.output_layer.weight.detach().cpu().clone()

    # Create the dataset.
    # if dataset_class.category() == ComputerVision:
    #     assert issubclass(model_class, ComputerVisionModel)
    #     dataset = dataset_class(image_size=model_class.expected_image_size())
    # elif dataset_class.category() == NaturalLanguageProcessing:
    #     assert issubclass(model_class, Bert) or issubclass(model_class, GPT2)
    #     dataset = dataset_class(model_name=model_class.model_name())
    # else:
    #     assert dataset_class.category() == Speech
    #     dataset = dataset_class(train_split=LibriSpeech.test_clean, test_split=LibriSpeech.test_other)

    model, dataset = get_model_and_dataset(model_class.model_name(), dataset_class.dataset_name())
    assert isinstance(model, model_class)
    assert isinstance(dataset, dataset_class)
    initial_weights: Tensor = model.output_layer.weight.detach().cpu().clone()

    synchronize_variable(
        io_loop=io_loop,
        synchronizer=synchronizer,
        raft_log=raft_log,
        val=model,
        meta=meta,
        key="model",
    )

    print(f"synchronizer.global_ns: {synchronizer.global_ns}")
    print(f"user_ns: {user_ns}")
    print(f"user_module: {user_module}")

    assert "model" in synchronizer.global_ns
    assert "model" in user_ns
    assert hasattr(user_module, "model")

    assert isinstance(user_ns["model"], model_class)
    assert isinstance(synchronizer.global_ns["model"], model_class)
    assert isinstance(user_module.model, model_class)

    weight: Parameter = user_ns["model"].output_layer.weight.detach().cpu().clone()
    assert weight.equal(initial_weights)

    weight: Parameter = synchronizer.global_ns["model"].output_layer.weight.detach().cpu().clone()
    assert weight.equal(initial_weights)

    weight: Parameter = user_module.model.output_layer.weight.detach().cpu().clone()
    assert weight.equal(initial_weights)

    previous_weights: Tensor = initial_weights.clone()
    for i in range(0, num_training_loops):
        # Train for a while.
        model.train(dataset.train_loader, target_training_duration_ms)

        # Establish that the model's weights have changed.
        updated_weights = model.output_layer.weight.detach().cpu().clone()
        assert not previous_weights.equal(updated_weights)
        previous_weights = updated_weights.clone()

        synchronize_variable(
            io_loop=io_loop,
            synchronizer=synchronizer,
            raft_log=raft_log,
            val=model,
            meta=meta,
            key="model",
        )

        print(f"synchronizer.global_ns: {synchronizer.global_ns}")
        print(f"user_ns: {user_ns}")
        print(f"user_module: {user_module}")

        assert "model" in synchronizer.global_ns
        assert "model" in user_ns
        assert hasattr(user_module, "model")

        assert isinstance(user_ns["model"], model_class)
        assert isinstance(synchronizer.global_ns["model"], model_class)
        assert isinstance(user_module.model, model_class)

        weight: Parameter = user_ns["model"].output_layer.weight.detach().cpu().clone()
        assert weight.equal(updated_weights)

        weight: Parameter = synchronizer.global_ns["model"].output_layer.weight.detach().cpu().clone()
        assert weight.equal(updated_weights)

        weight: Parameter = user_module.model.output_layer.weight.detach().cpu().clone()
        assert weight.equal(updated_weights)


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
def test_train_model_on_dataset(model_class: Type[DeepLearningModel], dataset_class: Type[CustomDataset]):
    train_and_sync_model(model_class, dataset_class, target_training_duration_ms=2000.0)
