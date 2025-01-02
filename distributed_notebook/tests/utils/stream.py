import inspect
import logging
from types import CodeType
from typing import Sequence, Callable, Any

import zmq
from zmq.eventloop.zmqstream import ZMQStream

from distributed_notebook.tests.utils.util import extract_last_two_parts


class SpoofedStream(ZMQStream):
    def __init__(self, *args, **kwargs):
        context = zmq.Context()
        if len(args) == 0:
            socket = context.socket(zmq.DEALER)
        else:
            socket = args[0]

        super().__init__(socket, **kwargs)

        self.log = logging.getLogger(__class__.__name__)
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        fm = logging.Formatter(fmt = "%(asctime)s [%(levelname)s] %(name)s [%(threadName)s (%(thread)d)]: %(message)s ")
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(fm)
        self.log.addHandler(ch)

        self.log.debug(f"SpoofedStream::__init__ called with args={args} and kwargs={kwargs}.")

    def on_recv(self, callback, copy=True):
        """
        register a callback to be run every time the socket has something to receive
        """
        f_code: CodeType = inspect.currentframe().f_back.f_code
        self.log.debug(f"SpoofedStream::on_recv called by function '{f_code.co_name}' from "
              f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno} with callback {callback} and copy={copy}")

    def on_send(self, callback):
        """
        register a callback to be run every time you call send
        """
        f_code: CodeType = inspect.currentframe().f_back.f_code
        self.log.debug(
            f"SpoofedStream::on_send called by function '{f_code.co_name}' from "
            f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno} with callback {callback}")

    def send_multipart(
            self,
            msg: Sequence[Any],
            flags: int = 0,
            copy: bool = True,
            track: bool = False,
            callback: Callable | None = None,
            **kwargs: Any,
    ) -> None:
        """
        perform a send that will trigger the callback if callback is passed, on_send is also called.
        """
        f_code: CodeType = inspect.currentframe().f_back.f_code
        self.log.debug(f"SpoofedStream::send_multipart called by called by function '{f_code.co_name}' from "
              f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno} with msg={msg}, "
              f"flags={flags}, copy={copy}, track={track}, callable={callable}, and kwargs={kwargs}")

    def stop_on_recv(self):
        """
        turn off the recv callback
        """
        f_code: CodeType = inspect.currentframe().f_back.f_code
        self.log.debug(f"SpoofedStream::stop_on_recv called by called by function '{f_code.co_name}' from "
              f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno}")

    def stop_on_send(self):
        """
        turn off the send callback
        """
        f_code: CodeType = inspect.currentframe().f_back.f_code
        self.log.debug(f"SpoofedStream::stop_on_send called by called by function '{f_code.co_name}' from "
              f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno}")
