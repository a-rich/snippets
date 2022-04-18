from argparse import ArgumentParser
import asyncio
import random

from async_tools import AsyncRateLimiter
    

async def main(args):
    async def func(args):
        await asyncio.sleep(1)
        return round(random.random(), 2)

    limiter = AsyncRateLimiter(func,
                               [[i] for i in range(1, 11)],
                               batch_size=2,
                               throttle=lambda x: x > 0.75)
    async for result in limiter():
        print(f"got result: {result}")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--requests', type=int, default=300)
    args = parser.parse_args()

    asyncio.run(main(args))
