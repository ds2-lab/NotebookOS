import asyncio
import random
import types
from typing import Any, Optional
import uuid

from unittest import mock
import distributed_notebook
from distributed_notebook.datasets.base import CustomDataset
from distributed_notebook.models.model import DeepLearningModel
from distributed_notebook.sync import Synchronizer
import builtins as builtin_mod
import pandas as pd

import distributed_notebook.sync
from distributed_notebook.sync.checkpointing.local_checkpointer import LocalCheckpointer
from distributed_notebook.sync.checkpointing.pointer import SyncPointer
from distributed_notebook.sync.checkpointing.remote_checkpointer import (
    RemoteCheckpointer,
)
from distributed_notebook.sync.log import SynchronizedValue
from distributed_notebook.sync.object import SyncObjectMeta, SyncObjectWrapper
from distributed_notebook.sync.raft_log import RaftLog
from distributed_notebook.sync.referer import SyncReferer
from distributed_notebook.sync.synchronizer import CHECKPOINT_AUTO

example_data: dict[str, Any] = {
    "Name": ["Alice", "Bob", "Charlie", "Dave"],
    "Age": [25, 32, 22, 45],
    "City": ["New York", "Los Angeles", "Chicago", "Miami"],
    "Salary": [55000, 74000, 48000, 66000],
}


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


def loaded_serialized_state_callback(state: dict[str, dict[str, Any]] = dict()):
    pass


def large_object_pointer_committed(
        pointer: SyncPointer,
) -> Optional[CustomDataset | DeepLearningModel]:
    return None


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


def __get_raft_log(remote_checkpointer: RemoteCheckpointer) -> RaftLog:
    raft_log: RaftLog = RaftLog(
        node_id=1,
        kernel_id=str(uuid.uuid4()),
        skip_create_log_node=True,
        base_path="./store",
        remote_checkpointer=remote_checkpointer,
    )

    return raft_log


def __get_synchronizer(
        raft_log: RaftLog,
        user_module: types.ModuleType,
        remote_checkpointer: RemoteCheckpointer,
) -> Synchronizer:
    synchronizer: Synchronizer = Synchronizer(
        raft_log,
        store_path="./store",
        module=user_module,
        opts=CHECKPOINT_AUTO,
        node_id=1,
        large_object_pointer_committed=large_object_pointer_committed,
        remote_checkpointer=remote_checkpointer,
    )
    assert synchronizer is not None

    return synchronizer

def synchronize_variable(
        append_future: asyncio.Future[SynchronizedValue],
        io_loop: asyncio.AbstractEventLoop,
        synchronizer: Synchronizer,
        raft_log: RaftLog,
        val: Any,
        meta: SyncObjectMeta,
):
    async def mocked_append(rlog: RaftLog, val: SynchronizedValue):
        print(f"Mocked RaftLog::append called with RaftLog {rlog} and SynchronizedValue {val}")
        append_future.set_result(val)

    with mock.patch.object(
            distributed_notebook.sync.raft_log.RaftLog, "append", mocked_append
    ):
        io_loop.run_until_complete(
            synchronizer.sync_key(
                sync_log=raft_log,
                key="my_var",
                val=val,
                end_execution=True,
                checkpointing=False,
                meta=meta,
            )
        )

    assert append_future.done()

    synchronized_key: SynchronizedValue = append_future.result()
    print(f"synchronized_key: {synchronized_key}")

    synchronizer.change_handler(synchronized_key, restoring = False)

def test_sync_and_change_int_variable():
    """
    Test calling sync_key followed by change_handler for an int variable.
    """
    remote_checkpointer: LocalCheckpointer = LocalCheckpointer()
    assert remote_checkpointer is not None

    raft_log: RaftLog = __get_raft_log(remote_checkpointer)
    assert raft_log is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        raft_log, user_module, remote_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    io_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    append_future: asyncio.Future[SynchronizedValue] = io_loop.create_future()
    val: int = 3

    synchronize_variable(
        append_future = append_future,
        io_loop = io_loop,
        synchronizer = synchronizer,
        raft_log = raft_log,
        val = val,
        meta = meta,
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

    val = 5
    append_future = io_loop.create_future()

    synchronize_variable(
        append_future = append_future,
        io_loop = io_loop,
        synchronizer = synchronizer,
        raft_log = raft_log,
        val = val,
        meta = meta,
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


def test_sync_and_change_dummy_object_variable():
    """
    Test calling sync_key followed by change_handler for an DummyObject variable.
    """
    remote_checkpointer: LocalCheckpointer = LocalCheckpointer()
    assert remote_checkpointer is not None

    raft_log: RaftLog = __get_raft_log(remote_checkpointer)
    assert raft_log is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        raft_log, user_module, remote_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    io_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    append_future: asyncio.Future[SynchronizedValue] = io_loop.create_future()
    dummy_obj: DummyObject = DummyObject(lst=[1, 2, 3, 4])

    synchronize_variable(
        append_future = append_future,
        io_loop = io_loop,
        synchronizer = synchronizer,
        raft_log = raft_log,
        val = dummy_obj,
        meta = meta,
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

    # Update the variable, then we'll re-sync it.
    dummy_obj.lst = [5, 6, 7, 8, 9, 10, 11]

    append_future = io_loop.create_future()

    synchronize_variable(
        append_future = append_future,
        io_loop = io_loop,
        synchronizer = synchronizer,
        raft_log = raft_log,
        val = dummy_obj,
        meta = meta,
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
    assert len(user_ns["my_var"].lst) == 7
    assert user_ns["my_var"].lst == [5, 6, 7, 8, 9, 10, 11]
    assert user_ns["my_var"].lst == dummy_obj.lst

    assert synchronizer.global_ns["my_var"].lst is not None
    assert len(synchronizer.global_ns["my_var"].lst) == 7
    assert synchronizer.global_ns["my_var"].lst == [5, 6, 7, 8, 9, 10, 11]
    assert synchronizer.global_ns["my_var"].lst == dummy_obj.lst

    assert user_module.my_var.lst is not None
    assert len(user_module.my_var.lst) == 7
    assert user_module.my_var.lst == [5, 6, 7, 8, 9, 10, 11]
    assert user_module.my_var.lst == dummy_obj.lst