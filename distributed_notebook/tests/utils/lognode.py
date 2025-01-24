import inspect
import logging
from types import CodeType

from distributed_notebook.tests.utils.util import extract_last_two_parts


class SpoofedLogNode(object):
    """
    Fake distributed_notebook.smr.smr.LogNode class for use in unit tests.
    """

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            try:
                setattr(self, key, value)
            except Exception as ex:
                print(f"Failed to set \"{key}\" attribute because: {ex}")

        self.log = logging.getLogger(__class__.__name__)
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        fm = logging.Formatter(fmt = "%(asctime)s [%(levelname)s] %(name)s [%(threadName)s (%(thread)d)]: %(message)s ")
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(fm)
        self.log.addHandler(ch)

        self.log.debug(f"SpoofedLogNode::__init__ called with kwargs={kwargs}.")

    def Propose(self, *args, **kwargs):
        f_code: CodeType = inspect.currentframe().f_back.f_code
        print(f"SpoofedLogNode::Propose called by function '{f_code.co_name}' from "
              f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno} with args {args} and kwargs {kwargs}.")

    # Upper-case because the real LogNode calls into Golang methods, which are upper-case.
    def RemoteStorageReadLatencyMilliseconds(self, *args, **kwargs) -> int:
        f_code: CodeType = inspect.currentframe().f_back.f_code
        print(f"SpoofedLogNode::RemoteStorageReadLatencyMilliseconds called by function '{f_code.co_name}' from "
              f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno} with args {args} and kwargs {kwargs}.")
        return 0

    # Upper-case because the real LogNode calls into Golang methods, which are upper-case.
    def GetSerializedState(self, *args, **kwargs) -> bytes:
        f_code: CodeType = inspect.currentframe().f_back.f_code
        self.log.debug(f"SpoofedLogNode::GetSerializedState called by function '{f_code.co_name}' from "
                       f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno} with args {args} and kwargs {kwargs}.")
        return b''

    # Upper-case because the real LogNode calls into Golang methods, which are upper-case.
    def Start(self, *args, **kwargs) -> bool:
        f_code: CodeType = inspect.currentframe().f_back.f_code
        self.log.debug(f"SpoofedLogNode::Start called by function '{f_code.co_name}' from "
                       f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno} with args {args} and kwargs {kwargs}.")
        return True

    def CloseRemoteStorageClient(self, *args, **kwargs):
        f_code: CodeType = inspect.currentframe().f_back.f_code
        self.log.debug(f"SpoofedLogNode::CloseRemoteStorageClient called by function '{f_code.co_name}' from "
                       f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno} with args {args} and kwargs {kwargs}.")
