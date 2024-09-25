import datetime
import json 
import uuid

from dateutil.tz import tzlocal
from typing import Any, List, Dict, Union, Optional, Callable

from jupyter_client.jsonutil import json_default
from jupyter_server.services.kernels.connection.base import BaseKernelWebsocketConnection, deserialize_msg_from_ws_v1
from jupyter_server.services.kernels.connection.channels import ZMQChannelsWebsocketConnection
from jupyter_server.services.kernels.connection.abc import KernelWebsocketConnectionABC

import zmq
from zmq.eventloop.zmqstream import ZMQStream

from tornado.concurrent import Future

DummyMessage:str = "dummy_message"
DummyMessageReply:str = DummyMessage + "_reply"

class ZMQChannelsWebsocketConnectionV2(ZMQChannelsWebsocketConnection):
    special_message_types: List[str] = [DummyMessage]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.special_message_handlers: Dict[str, Callable[[Dict[str, Any], str]]] = {
            DummyMessage: self.__handle_dummy_message
        }

    def __handle_dummy_message(self, msg: Dict[str, Any], channel: str):
        self.log.debug(f"Received message of special type \"{DummyMessage}\": {msg}")
        msg['header']['msg_type'] = DummyMessageReply
        msg['parent_header'] = msg['header'].copy()
        msg['header']['date'] = json_default(datetime.datetime.now())
        msg['header']['msg_id'] = str(uuid.uuid4())
        
        stream = self.channels.get(channel, None)
        if not stream:
            raise ValueError(f"Could not find stream associated with specified channel \"{channel}\"")

        if self.subprotocol == "v1.kernel.websocket.jupyter.org":
            raise ValueError("ZMQChannelsWebsocketConnectionV2 does not support the \"v1.kernel.websocket.jupyter.org\" protocol")
        else:
            self._on_zmq_reply(stream, msg)
    
    def handle_incoming_message(self, incoming_msg: str) -> None:
        """Handle an incoming message."""
        self.log.debug(f"Handling incoming message: {incoming_msg}")
        # super().handle_incoming_message(incoming_msg)
        
        """Handle incoming messages from Websocket to ZMQ Sockets."""
        ws_msg:str = incoming_msg
        if not self.channels:
            # already closed, ignore the message
            self.log.debug("Received message on closed websocket %r", ws_msg)
            return

        if self.subprotocol == "v1.kernel.websocket.jupyter.org":
            channel, msg_list = deserialize_msg_from_ws_v1(ws_msg)
            msg: Dict[str, Any] = { "header": None }
        else:
            if isinstance(ws_msg, bytes):  # type:ignore[unreachable]
                msg: Dict[str, Any] = deserialize_binary_message(ws_msg)  # type:ignore[unreachable]
            else:
                msg: Dict[str, Any] = json.loads(ws_msg)
            
            msg_list: List[Any] = []
            channel: Optional[str] = msg.pop("channel", None)

        if channel is None:
            self.log.warning("No channel specified, assuming shell: %s", msg)
            channel = "shell"
            
        if channel not in self.channels:
            self.log.warning("No such channel: %r", channel)
            return
        
        ignore_msg: bool = False
        am = self.multi_kernel_manager.allowed_message_types
        # if am:
        msg["header"] = self.get_part("header", msg["header"], msg_list)
        assert msg["header"] is not None
        
        msg_type:str = msg["header"]["msg_type"]
        if am and msg_type not in am:  
            self.log.warning(f'Received message of type "{msg_type}", which is not allowed. Ignoring.')
            ignore_msg = True
        elif msg_type in self.special_message_types:
            handler: Optional[Callable] = self.special_message_handlers.get(msg_type, None)
            
            if handler:
                handler(msg, channel)
            
            ignore_msg = True
        
        if not ignore_msg:
            stream = self.channels[channel]
            
            if self.subprotocol == "v1.kernel.websocket.jupyter.org":
                self.session.send_raw(stream, msg_list)
            else:
                self.session.send(stream, msg)

    def handle_outgoing_message(self, stream: str | ZMQStream, outgoing_msg: List[Any]) -> None:
        """Handle an outgoing message."""
        if isinstance(stream, ZMQStream):
            # Don't print the actual messages unless they're SHELL or CONTROL messages.
            socket: zmq.Socket = stream.socket 
            socket_type = socket.get(zmq.TYPE)
            socket_type_name:str = list(zmq.SocketType)[socket_type].name # type: ignore
            
            if socket_type == zmq.SocketType.DEALER or socket_type == zmq.SocketType.ROUTER:
                self.log.debug(f"Handling outgoing {socket_type_name} message: {outgoing_msg}")
            else:
                self.log.debug(f"Handling outgoing {socket_type_name} message now.")
        else: 
            self.log.debug(f"Handling outgoing {stream} message: {outgoing_msg}")
            
        super().handle_outgoing_message(stream, outgoing_msg) # type: ignore

    @property
    def write_message(self):
        """Alias to the websocket handler's write_message method."""
        # return self.websocket_handler.write_message
        return self.__write_message_internal

    def __write_message_internal(self, message: Union[bytes, str, Dict[str, Any]], binary: bool = False) -> Future[None]:
        self.log.debug(f'Writing websocket message (binary={binary}): {message}')
        return self.websocket_handler.write_message(message, binary = binary)        

    KernelWebsocketConnectionABC.register(BaseKernelWebsocketConnection)