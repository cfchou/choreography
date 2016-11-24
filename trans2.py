# vim:fileencoding=utf-8

import random
from asyncio import BaseEventLoop
import attr
from attr.validators import instance_of
from transitions import Machine
import time
import functools

from prometheus_async.aio import web
from prometheus_client import Counter, Gauge, Summary, Histogram
import asyncio
import uvloop
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
import sys
import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

a_list = ['a']


def process_bound_job(t, n, shared):
    print('proc {}: >>'.format(n))
    policy = asyncio.get_event_loop_policy()
    print('proc {}: {} {}'.format(n, id(policy), policy))

    loop = policy.new_event_loop()

    # id() doesn't necessarily give different values when using ProcessExecutor
    # when loops are different objects residing in different processes
    print('proc {}: {} {}'.format(n, id(loop), loop))

    policy.set_event_loop(loop)
    loop.run_until_complete(top_one(t, n))
    shared[0] = n
    print('proc {}: done'.format(n))
    return n

async def top_level(loop):
    t1 = loop.run_in_executor(None, process_bound_job, 5, 1, a_list)
    #await asyncio.wait([t1])
    t2 = loop.run_in_executor(None, process_bound_job, 10, 2, a_list)
    await asyncio.wait([t1, t2])
    #t3 = loop.run_in_executor(None, process_bound_job, 15, 3, a_list)
    #await asyncio.wait([t1, t2, t3])

async def top_one(t, n):
    gauge = Gauge('cg_connections_total', 'connections in total')
    print('{}: top_one >>'.format(n))
    loop = asyncio.get_event_loop()
    print('{}: {} {}'.format(n, id(loop), loop))
    for _ in range(0, 3):
        await asyncio.sleep(t)
        print('{}: gauge.inc'.format(n))
        gauge.inc()
    print('{}: top_one done {}'.format(n, gauge))

def run():

    # optional: uvloop as a drop-in replacement
    policy = uvloop.EventLoopPolicy()
    print('proc {}: {} {}'.format(0, id(policy), policy))
    asyncio.set_event_loop_policy(policy=policy)

    loop = asyncio.get_event_loop()

    # total: n + 1 processes/threads
    executor = ProcessPoolExecutor(3)
    #executor = ThreadPoolExecutor(3)
    loop.set_default_executor(executor)

    loop.create_task(top_one(3, 0))
    loop.create_task(web.start_http_server(addr='10.1.204.14', port=28080,
                                           loop=loop))
    loop.create_task(top_level(loop))
    loop.run_forever()
    executor.shutdown()


if __name__ == '__main__':
    run()


