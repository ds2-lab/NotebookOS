import asyncio

from .errors import FromGoError

class Future:
    def __init__(self, loop=None):
        if loop is None:
            loop = asyncio.get_running_loop()
        self.future = loop.create_future()
    
    async def resolve(self, value, goerr):
        err = FromGoError(goerr)
        if err is None:
            self.future.set_result(value)
        else:
            self.future.set_exception(err)

    async def result(self):
        return await self.future