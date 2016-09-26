
import sys
import asyncio
import os
from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.errors import MQTTException
from hbmqtt.version import get_version
from docopt import docopt
from hbmqtt.mqtt.constants import QOS_0
from hbmqtt.utils import read_yaml_config
import types

from choreography import launcher as lc
import yaml

import logging.config
import logging
log = logging.getLogger(__name__)



async def _do_fire(loop, fire: lc.Fire):
    log.debug('_do_fire for {} secs'.format(fire.duration))
    for i in range(0, fire.duration):
        log.debug('_do_fire for {}/sec'.format(fire.rate))
        cs = [MQTTClient() for i in range(0, fire.rate)]
        fire_coros = [c.connect(uri='mqtt://127.0.0.1:1883') for c in cs]
        fire_coros.append(asyncio.sleep(1))
        coro = asyncio.wait(fire_coros, loop=loop, timeout=3)
        done, not_done = await coro
        log.debug('done:{}, not_done:{}'.format(len(done), len(not_done)))
    return 'done'


async def create_clients(launcher, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    ctx = lc.RunnerContext.new_conext()
    base = loop.time()
    while True:
        log.debug("ask_next...")
        resp = launcher.ask_next(ctx)
        if resp.is_terminate():
            break
        before = loop.time()
        log.debug('before:{}'.format(before))
        if resp.is_fire():
            res = await _do_fire(loop, resp.action)
            log.debug(res)
            after = loop.time()
            log.debug('after:{}'.format(after))
        else:
            # TODO: Idle might be removed
            # keep the loop incomplete
            asyncio.sleep(1, loop=loop)
        ctx.append_history(resp,
                           lc.RunnerHistoryItem(at=before, succeeded=1, failed=0))


async def sleeper():
    loop = asyncio.get_event_loop()
    while True:
        log.debug('sleep at {}'.format(loop.time()))
        await asyncio.sleep(10)


def run():
    launcher = lc.OneShotLancher()
    loop = asyncio.get_event_loop()
    runner = asyncio.ensure_future(create_clients(loop=loop, launcher=launcher),
                                   loop=loop)

    log.debug("run")
    loop.run_until_complete(asyncio.wait([runner, sleeper()]))


if __name__ == '__main__':
    with open('log_config.yaml') as fh:
        try:
            logging.config.dictConfig(yaml.load(fh))
            run()
        finally:
            logging.shutdown()
