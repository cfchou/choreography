
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
from stevedore import extension

from choreography.launcher import Fire, RunnerHistoryItem, RunnerContext, OneShotLancher
import yaml
import logging.config
import logging
log = logging.getLogger(__name__)


class CgClient(MQTTClient):
    def __init__(self, client_id=None, config=None, loop=None):
        default_config = {
            'auto_reconnect': False
        }
        if config is not None:
            default_config.update(config)
        super().__init__(client_id, default_config, loop)

    async def connect(self, uri=None, cleansession=None, cafile=None, capath=None,
                cadata=None):
        try:
            log.debug('connect to {}'.format(uri))
            return await super().connect(uri, cleansession, cafile, capath,
                                         cadata)
        except ConnectException as e:
            #log.exception(e)
            return connack.SERVER_UNAVAILABLE


async def _do_fire(loop, fire: Fire):
    #async def connect_nothrow(c):
    #    try:
    #        return await c.connect(uri='mqtt://127.0.0.1:1883')
    #    except ConnectException as e:
    #        log.exception(e)
    #        return connack.SERVER_UNAVAILABLE

    log.debug('_do_fire for {} secs'.format(fire.duration))
    history = []
    for i in range(0, fire.duration):
        fire_at = loop.time()
        log.debug('_do_fire for {}/sec'.format(fire.rate))

        #cs = [MQTTClient(config={'auto_reconnect': False}, loop=loop)
        #      for i in range(0, fire.rate)]
        #fire_coros = [connect_nothrow(c) for c in cs]

        cs = [CgClient(loop=loop) for i in range(0, fire.rate)]
        fire_coros = [c.connect(uri='mqtt://127.0.0.1:1883') for c in cs]

        fire_coros.append(asyncio.sleep(1))
        coro = asyncio.wait(fire_coros, loop=loop, timeout=3)
        done, _ = await coro
        succeeded = len([d for d in done
                         if d.result() != connack.SERVER_UNAVAILABLE]) - 1
        log.debug('done:{}, succeeded:{}'.format(len(done), succeeded))
        history.append(RunnerHistoryItem(at=fire_at, succeeded=succeeded,
                                            failed=fire.rate-succeeded))
    return history


async def create_clients(launcher, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    ctx = RunnerContext.new_conext()
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
    launcher = OneShotLancher()
    loop = asyncio.get_event_loop()
    runner = asyncio.ensure_future(create_clients(loop=loop, launcher=launcher),
                                   loop=loop)

    log.debug("run")
    loop.run_until_complete(asyncio.wait([runner, sleeper()]))


def run3():
    with open('launchers.yaml') as fh:
        try:
            launcher_config = yaml.load(fh)
            launchers_mgr = extension.ExtensionManager(
                namespace='choreography.launcher_plugins')
            log.info('available launcher_plugins:{}'.format(
                launchers_mgr.extensions))

            def new_launcher_plugin(lc):
                exts = [ext for ext in launchers_mgr.extensions
                        if ext.name == lc['entry']]
                if len(exts) == 0:
                    log.error('plugin {} doesn\'t exist'.format(lc['entry']))
                    return None
                if len(exts) > 1:
                    log.error('duplicated plugins {} found'.format(lc['entry']))
                    return None
                return exts[0].plugin(lc['args'])

            launchers = [new_launcher_plugin(launcher)
                         for launcher in launcher_config['launchers']]


        except Exception as e:
            pass



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
