import logging
from types import CodeType

from jupyter_client.session import Session, Message

import typing as t
import zmq
import inspect
from zmq.eventloop.zmqstream import ZMQStream
from collections import defaultdict

from distributed_notebook.tests.utils.util import extract_last_two_parts


class SpoofedSession(Session):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.num_send_calls: int = 0
        self.message_types_sent: list[str] = []
        self.message_types_sent_counts: defaultdict[str, int] = defaultdict(int)

        self.log = logging.getLogger(__class__.__name__)
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        fm = logging.Formatter(fmt = "%(asctime)s [%(levelname)s] %(name)s [%(threadName)s (%(thread)d)]: %(message)s ")
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(fm)
        self.log.addHandler(ch)

        self.log.debug(f"SpoofedSession::__init__ called with kwargs={kwargs}.")

    def send(
            self,
            stream: zmq.sugar.socket.Socket | ZMQStream | None,
            msg_or_type: dict[str, t.Any] | str,
            content: dict[str, t.Any] | None = None,
            parent: dict[str, t.Any] | None = None,
            ident: bytes | list[bytes] | None = None,
            buffers: list[bytes] | None = None,
            track: bool = False,
            header: dict[str, t.Any] | None = None,
            metadata: dict[str, t.Any] | None = None,
    ):
        f_code: CodeType = inspect.currentframe().f_back.f_code
        self.log.debug(f"TestSession::send called by function '{f_code.co_name}' from "
              f"{extract_last_two_parts(f_code.co_filename)}::{f_code.co_firstlineno} with msg_or_type={msg_or_type}, content={content}, "
              f"parent={parent}, ident={ident}, buffers={buffers}, metadata={metadata}, and header={header}.")

        self.num_send_calls += 1

        if isinstance(msg_or_type, (Message, dict)):
            # We got a Message or message dict, not a msg_type so don't
            # build a new Message.
            msg = msg_or_type
        else:
            msg = self.msg(
                msg_or_type,
                content=content,
                parent=parent,
                header=header,
                metadata=metadata,
            )

        if header is None:
            header = msg["header"]

        if isinstance(header, dict) and "msg_type" in header:
            msg_type:str = header["msg_type"]
            self.message_types_sent.append(msg_type)
            count: int = self.message_types_sent_counts[msg_type]
            self.message_types_sent_counts[msg_type] = count + 1

        return msg
