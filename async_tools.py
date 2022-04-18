import asyncio
import logging
from time import perf_counter


async def ok(val):
    return val


async def gather_map(coro_dict):
    tasks = [asyncio.create_task(coro(*args) 
            if isinstance(args, tuple) else coro(args))
            for args, coro in coro_dict.items()]
    results = await asyncio.gather(*tasks)

    return dict(zip(coro_dict.keys(), results))


class AsyncRateLimiter():
    def __init__(self, func, args, batch_size=1024, throttle=lambda x: x):
        self.func = func
        self.args = args
        self.batch_size = batch_size
        self.throttle = throttle
        logging.basicConfig(
                level='INFO',
                datefmt='%Y-%m-%d %H:%M:%S',
                format='%(asctime)s - %(name)s:%(lineno)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger()
    
    async def wrap(self, func, args):
        return args, await func(*args)
    
    def is_throttled(self, result):
        return self.throttle(result)
    
    async def __aiter__(self):
        while self.args:
            args = self.args[:self.batch_size]
            self.args = self.args[self.batch_size:]
            yield args

    async def __call__(self): 
        self.logger.info(f"Rate limiting {len(self.args)} tasks with " 
                         f"batch_size {self.batch_size}")
        t = task_n = 0
        async for batch in self:
            self.logger.debug(f"\tCreating {len(batch)} tasks -- " \
                              f"{len(self.args)} remaining")
            t1 = perf_counter()    
            tasks = await asyncio.gather(*[asyncio.create_task(
                    self.wrap(self.func, args)) for args in batch])
            t2 = perf_counter()
            t += round(t2 - t1, 2)
            task_n += len(tasks)
            self.logger.debug(f"\tGathered {len(tasks)} tasks in " \
                              f"{t2-t1} seconds...")
            rerun = []
            for args, result in tasks:
                self.logger.debug(f"\t\targs: {args} -- result: {result}")
                if not self.is_throttled(result):
                    yield result
                else:
                    rerun.append(args)

            if rerun:
                self.logger.info(f"{len(rerun)} tasks throttled after running {task_n} tasks...rerunning {len(rerun)} tasks after sleeping for {t} seconds")
                self.args.extend(rerun)
                await asyncio.sleep(t)
                t = task_n = 0
