
import sys
import asyncio
import os
from hbmqtt.client import MQTTClient, ConnectException, ClientException
from hbmqtt.mqtt import connack
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


class CgClient(MQTTClient):
    def __init__(self, client_id=None, config=None, loop=None):
        super().__init__(client_id, config, loop)


async def _do_fire(loop, fire: lc.Fire):
    async def connect_nothrow(c):
        try:
            return await c.connect(uri='mqtt://127.0.0.1:1883')
        except ConnectException as e:
            log.exception(e)
            return connack.SERVER_UNAVAILABLE

    config = {
        'auto_reconnect': False,
    }
    log.debug('_do_fire for {} secs'.format(fire.duration))
    history = []
    for i in range(0, fire.duration):
        fire_at = loop.time()
        log.debug('_do_fire for {}/sec'.format(fire.rate))
        cs = [MQTTClient(config=config) for i in range(0, fire.rate)]
        fire_coros = [connect_nothrow(c) for c in cs]
        fire_coros.append(asyncio.sleep(1))
        coro = asyncio.wait(fire_coros, loop=loop, timeout=3)
        done, _ = await coro
        succeeded = len([d for d in done
                         if d.result() != connack.SERVER_UNAVAILABLE]) - 1
        log.debug('done:{}, succeeded:{}'.format(len(done), succeeded))
        history.append(lc.RunnerHistoryItem(at=fire_at, succeeded=succeeded,
                                            failed=fire.rate-succeeded))
    return history


async def create_clients(launcher, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    ctx = lc.RunnerContext.new_conext()
    while True:
        log.debug("ask_next...")
        resp = launcher.ask_next(ctx)
        if resp.is_terminate():
            break
        before = loop.time()
        log.debug('before:{}'.format(before))
        if resp.is_fire():
            history = await _do_fire(loop, resp.action)
            log.debug('len(hist):{}'.format(len(history)))
            after = loop.time()
            log.debug('after:{}'.format(after))
        else:
            # TODO: Idle might be removed
            # keep the loop incomplete
            asyncio.sleep(1, loop=loop)
        ctx.update(resp, history)


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


async def foo():
    try:
        await bar()
    except ClientException as e:
        print(repr(e))

    #raise Exception('WWWWW')

async def bar():
    await asyncio.sleep(2)
    raise ClientException('WWWWW')

def run2():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait([foo(), sleeper()]))



if __name__ == '__main__':
    with open('log_config.yaml') as fh:
        try:
            logging.config.dictConfig(yaml.load(fh))
            run()
        finally:
            logging.shutdown()
