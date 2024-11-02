from jupyter_client.session import Session, Message

import typing as t
import zmq
import os
from zmq.eventloop.zmqstream import ZMQStream


class TestSession(Session):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        print("Created new Test Session")

        self.num_send_calls: int = 0

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
        print(f"TestSession::send called with msg_or_type={msg_or_type}, content={content}, parent={parent}, "
              f"ident={ident}, buffers={buffers}, metadata={metadata}, and header={header}.")

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

        return msg
