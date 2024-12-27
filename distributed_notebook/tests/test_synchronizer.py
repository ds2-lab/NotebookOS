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


def mocked_append(*args):
    print(f"Mocked RaftLog::append called with arguments: {args}")


@mock.patch.object(
    distributed_notebook.sync.raft_log.RaftLog,
    "append",
    mocked_append,
)
def test_sync_key():
    remote_checkpointer: LocalCheckpointer = LocalCheckpointer()
    assert remote_checkpointer is not None

    raft_log: RaftLog = __get_raft_log(remote_checkpointer)

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        raft_log, user_module, remote_checkpointer
    )

    meta = SyncObjectMeta(batch=(str(1)))

    asyncio.get_event_loop().run_until_complete(
        synchronizer.sync_key(
            raft_log,
            "my_var",
            3,
            end_execution=True,
            checkpointing=False,
            meta=meta,
        )
    )


def test_restore_int():
    sync_object = SyncObjectWrapper(referer=SyncReferer())
    print("===== Created SyncObjectWrapper =====\n\n")
    sync_object.config_pickler(profiling=False)
    print("===== Configured Pickler =====\n\n")

    val = sync_object.diff(3)
    print("===== Called Diff on SyncObjectWrapper =====\n\n")
    assert val is not None

    print("===== Called Update on SyncObjectWrapper =====\n\n")
    result = sync_object.update(val)

    # Check that the result is as expected
    assert result is not None
    assert isinstance(result, int)


def test_restore_dummy_object():
    sync_object = SyncObjectWrapper(referer=SyncReferer())
    print("===== Created SyncObjectWrapper =====\n\n")
    sync_object.config_pickler(profiling=False)
    print("===== Configured Pickler =====\n\n")

    dummy_obj: DummyObject = DummyObject(lst=[1, 2, 3, 4])
    val = sync_object.diff(dummy_obj)
    print("===== Called Diff on SyncObjectWrapper =====\n\n")
    assert val is not None

    print("===== Called Update on SyncObjectWrapper =====\n\n")
    result = sync_object.update(val)

    # Check that the result is as expected
    assert result is not None
    assert isinstance(result, pd.DataFrame)


def test_restore_dataframe():
    sync_object = SyncObjectWrapper(referer=SyncReferer())
    print("===== Created SyncObjectWrapper =====\n\n")
    sync_object.config_pickler(profiling=False)
    print("===== Configured Pickler =====\n\n")
    val = sync_object.diff(pd.DataFrame(example_data))
    print("===== Called Diff on SyncObjectWrapper =====\n\n")
    assert val is not None

    print("===== Called Update on SyncObjectWrapper =====\n\n")
    result = sync_object.update(val)

    # Check that the result is as expected
    assert result is not None
    assert isinstance(result, pd.DataFrame)


def test_synchronizer_init():
    remote_checkpointer: LocalCheckpointer = LocalCheckpointer()
    assert remote_checkpointer is not None

    raft_log: RaftLog = __get_raft_log(remote_checkpointer)

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        raft_log, user_module, remote_checkpointer
    )


def test_handle_changed_value():
    remote_checkpointer: LocalCheckpointer = LocalCheckpointer()
    assert remote_checkpointer is not None

    raft_log: RaftLog = __get_raft_log(remote_checkpointer)

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = __get_synchronizer(
        raft_log, user_module, remote_checkpointer
    )

    sync_obj_wrapper: SyncObjectWrapper = SyncObjectWrapper(
        referer=SyncReferer(),
    )
    sync_obj_wrapper.config_pickler(profiling=True)

    dummy_val: DummyObject = DummyObject(lst=[1, 2, 3, 4])
    val = sync_obj_wrapper.diff(dummy_val, SyncObjectMeta(str(1)))

    assert val is not None
    result = sync_obj_wrapper.update(val)

    # Check that the result is as expected
    assert result is not None
    assert isinstance(result, DummyObject)

    # synchronizer.change_handler(sync_val, restoring=False)
