# vim:fileencoding=utf-8

import asyncio
from hbmqtt.client import MQTTClient, ConnectException, ClientException
from hbmqtt.mqtt import connack

from choreography import cg_launcher
from choreography.cg_launcher import RunnerHistoryItem
from choreography.cg_launcher import Launcher
from choreography.cg_companion import Companion
from choreography.cg_util import RunnerContext, PluginConf
from typing import List
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

    async def connect(self, uri=None, cleansession=None, cafile=None,
                      capath=None, cadata=None):
        try:
            log.debug('connect to {}'.format(uri))
            return await super().connect(uri, cleansession, cafile, capath,
                                         cadata)
        except ConnectException as e:
            #log.exception(e)
            return connack.SERVER_UNAVAILABLE


async def _do_fire(companions_conf, companion_mgr,
                   fire: cg_launcher.Fire, loop):
    log.debug('_do_fire for {} secs'.format(fire.step * fire.num_steps))
    history = []
    for i in range(0, fire.num_steps):
        fire_at = loop.time()
        log.debug('_do_fire {} clients at step {}'.format(fire.rate, i))

        # TODO: CgClient config
        cs = [CgClient(loop=loop) for _ in range(0, fire.rate)]
        fire_coros = [c.connect(uri='mqtt://127.0.0.1:1883') for c in cs]
        fire_coros.append(asyncio.sleep(fire.step))
        # TODO: config timeout
        done, _ = await asyncio.wait(fire_coros, loop=loop, timeout=3)
        # TODO: filter out sleep Task
        succeeded = len([d for d in done
                         if d.result() != connack.SERVER_UNAVAILABLE]) - 1
        log.debug('done:{}, succeeded:{}'.format(len(done), succeeded))
        history.append(RunnerHistoryItem(at=fire_at, succeeded=succeeded,
                                         failed=fire.rate-succeeded))
    return history


async def _do_idle(idle: cg_launcher.Idle, loop=None):
    await asyncio.sleep(idle.steps * idle.num_steps)


async def launcher_runner(companions_conf, companion_mgr,
                          launcher: Launcher,
                          ctx: RunnerContext=None,
                          loop: asyncio.BaseEventLoop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    if ctx is None:
        ctx = RunnerContext.new_conext()

    while True:
        log.debug("ask...")
        resp = launcher.ask(ctx)
        if resp.is_terminate():
            log.debug('Terminate {}'.format(ctx))
            break
        if resp.is_idle():
            await _do_idle(resp.action, loop)
            continue
        # resp.is_fire
        before = loop.time()
        log.debug('before:{}'.format(before))
        history = await _do_fire(companions_conf, companion_mgr, resp.action,
                                 loop)
        log.debug('len(hist):{}'.format(len(history)))
        after = loop.time()
        log.debug('after:{}'.format(after))
        ctx.update(resp, history)

