import asyncio

class Future:
    def __init__(self, loop=None):
        if loop is None:
            loop = asyncio.get_running_loop()
        self.future = loop.create_future()
    
    async def resolve(self, value):
       self.future.set_result(value)

    async def result(self):
        return await self.future