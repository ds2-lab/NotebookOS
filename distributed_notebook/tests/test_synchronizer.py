import types
from typing import Any, Optional
import uuid
from distributed_notebook.datasets.base import CustomDataset
from distributed_notebook.models.model import DeepLearningModel
from distributed_notebook.sync import Synchronizer
import builtins as builtin_mod

from distributed_notebook.sync.checkpointing.local_checkpointer import LocalCheckpointer
from distributed_notebook.sync.checkpointing.pointer import SyncPointer
from distributed_notebook.sync.checkpointing.remote_checkpointer import (
    RemoteCheckpointer,
)
from distributed_notebook.sync.raft_log import RaftLog
from distributed_notebook.sync.synchronizer import CHECKPOINT_AUTO


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
