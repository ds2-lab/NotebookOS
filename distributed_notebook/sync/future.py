import asyncio
import logging  

from typing import Any 

from .errors import FromGoError
from ..logging import ColoredLogFormatter


class Future:
    def __init__(self, loop=None, name:str = ""):
        if loop is None:
            loop = asyncio.get_running_loop()
        self.future: asyncio.Future[Any] = loop.create_future()
        self.name:str = name 
        self.logger: logging.Logger = logging.getLogger(__class__.__name__ + f"[{self.name}]")
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.logger.addHandler(ch)
    
    async def resolve(self, value, goerr):
        err = FromGoError(goerr)
        if err is None:
            self.logger.debug(f"Setting result to value: {value}")
            self.future.set_result(value)
        else:
            self.logger.debug(f"Setting exception to: {err}")
            self.future.set_exception(err)

    async def result(self):
        self.logger.debug("Awaiting result.")
        res: Any = await self.future
        self.logger.debug(f"Result has been resolved: {res}")
        return res 