import types
from typing import Any, Optional
from distributed_notebook.datasets.base import CustomDataset
from distributed_notebook.models.model import DeepLearningModel
from distributed_notebook.sync import Synchronizer
import builtins as builtin_mod

from distributed_notebook.sync.checkpointing.local_checkpointer import LocalCheckpointer
from distributed_notebook.sync.checkpointing.pointer import SyncPointer
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
    user_module = types.ModuleType(
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


def test_synchronizer_init():
    remote_checkpointer: LocalCheckpointer = LocalCheckpointer()
    assert remote_checkpointer is not None

    user_module, user_ns = prepare_user_module()
    assert user_module is not None
    assert user_ns is not None

    synchronizer: Synchronizer = Synchronizer(
        None,
        store_path="./store",
        module=user_module,
        opts=CHECKPOINT_AUTO,
        node_id=1,
        large_object_pointer_committed=large_object_pointer_committed,
        remote_checkpointer=remote_checkpointer,
    )
    assert synchronizer is not None
