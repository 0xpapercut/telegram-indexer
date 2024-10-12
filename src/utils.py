async def async_enumerate(aiterable, start=0):
    index = start
    async for item in aiterable:
        yield index, item
        index += 1
