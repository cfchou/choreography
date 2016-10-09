# vim:fileencoding=utf-8

from hbmqtt.client import MQTTClient, ClientException

from choreography import cg_launcher
from choreography.cg_launcher import Launcher, LcResp
from choreography.cg_companion import Companion
from choreography.cg_exception import *
from choreography import cg_util
from choreography.cg_util import CompanionPluginConf, RunnerContext
import random
from typing import List
import asyncio
from asyncio import BaseEventLoop
import logging
log = logging.getLogger(__name__)


class CgClient(MQTTClient):
    """

    hbmqtt.client.ClientException derives from BaseException which cannot be
    captured by asyncio so that the event loop will be aborted. Leave us no
    chance to examine a Future's result.
    CgClient replaces ClientException with CgClientException that derives
    Exception.
    """
    def __init__(self, client_id=None, config=None,
                 loop: BaseEventLoop=None):
        default_config = {
            'keep_alive': 60,
            'ping_delay': 1,
            'default_qos': 0,
            'default_retain': False,
            'auto_reconnect': False,
            'check_hostname': False
        }
        default_broker = {
            'cleansession': True
        }
        if config is not None:
            default_config.update(config)
            default_broker.update(config.get('broker', {}))
        default_config['broker'] = default_broker
        super().__init__(client_id, default_config, loop)

    def is_connected(self):
        return self.session.transitions.is_connected()

    async def connect(self, uri=None, cleansession=None, cafile=None,
                      capath=None, cadata=None):
        try:
            log.debug('connect to {}'.format(uri))
            return await super().connect(uri, cleansession, cafile, capath,
                                         cadata)
        except ClientException as e:
            log.exception(e)
            raise CgClientException from e

    async def run(self, companion: Companion):
        log.debug('{} run with Companion:{}'.format(self.client_id, companion))


def _pick_plugin_conf_func(cps):
    """
    Pick a CompanionPluginConf. It either goes randomly or follows the weights
    if all weights are presented.
    :param cps:
    :return:
    """
    if len(cps) <= 0:
        return lambda _: None
    weights = [cp.weight for cp in cps]
    ws = sum([1 for w in weights if w is not None])
    if ws == 0:
        # no weights at all
        return lambda _: cps[random.randrange(0, len(cps))]
    if ws != len(cps):
        raise CgException('all or none weights')
    # all weights are presented
    cd = cg_util.to_cd(weights)
    return lambda i: cps[cg_util.cdf_from_cd(cd, i)]


def _run_client(uri: str,
                plugin_conf: CompanionPluginConf,
                fire: cg_launcher.LcFire,
                loop: BaseEventLoop,
                cleansession: bool=None,
                cafile: str=None,
                capath: str=None,
                cadata: str=None) -> asyncio.Task:

    async def connect():
        client_id, conf = await fire.conf_queue.get()
        cc = CgClient(client_id=client_id, config=conf, loop=loop)
        log.debug('CgClient {} await connect...'.format(cc.client_id))
        await cc.connect(uri=uri, cleansession=cleansession, cafile=cafile,
                         capath=capath, cadata=cadata)
        log.debug('CgClient {} connect awaited'.format(cc.client_id))
        return cc

    def connect_cb(_fu: asyncio.Future):
        if _fu.cancelled():
            log.info('connect cancelled: {}'.format(_fu))
            return
        if _fu.exception() is not None:
            log.exception(_fu.exception())
            return
        cc = _fu.result()
        if not cc.is_connected():
            log.info('connect is not connected: {}'.format(cc))
            return
        log.info('connect result: {}'.format(cc))
        if plugin_conf is None:
            log.info('skip running because no companion plugs')
            return
        cp = plugin_conf.new_instance(loop)
        log.debug('use companion {} to run'.format(cp.name))
        loop.create_task(cc.run(cp))

    task = loop.create_task(connect())
    task.add_done_callback(connect_cb)
    return task


async def _do_fire(uri: str,
                   companion_plugins: List[CompanionPluginConf],
                   fire: cg_launcher.LcFire,
                   loop: BaseEventLoop,
                   cleansession: bool=None,
                   cafile: str=None,
                   capath: str=None,
                   cadata: str=None):

    def is_running_client(_task: asyncio.Task):
        if _task.cancelled():
            log.info('task cancelled: {}'.format(_task))
            return False
        if _task.exception() is not None:
            log.info('task exception: {}'.format(_task.exception()))
            return False
        cc = _task.result()
        if not isinstance(cc, CgClient):
            return False
        return cc.is_connected()

    pick_func = _pick_plugin_conf_func(companion_plugins)

    def run_client(_i):
        p = pick_func(_i + 1)
        return _run_client(uri, p, fire, loop, cleansession, cafile, capath,
                           cadata)

    log.debug('at {} _do_fire for {} secs'.format(loop.time, fire.duration))
    futs = [run_client(i) for i in range(0, fire.rate)]
    futs.append(asyncio.sleep(fire.duration))
    timeout = max(fire.duration, fire.timeout)
    done, not_done = await asyncio.wait(futs, loop=loop, timeout=timeout)
    running = len([d for d in done if is_running_client(d)])
    log.debug('_dofire: done:{}, running:{}'.format(len(done), running))
    return running, fire.rate - running


async def _do_idle(idle: cg_launcher.LcIdle, loop: BaseEventLoop=None):
    await asyncio.sleep(idle.duration, loop=loop)


# TODO: an Launcher instance should only be run by launcher_runner only once
# might need to embed a state in Launcher.
# that ensures that launcher.ask is re-entrant free.
async def launcher_runner(launcher: Launcher,
                          companion_plugins: List[CompanionPluginConf]):
    loop = launcher.loop
    cmd_resp = None
    conf = launcher.config['broker']
    uri = conf['uri']
    cleansession = conf.get('cleansession'),
    cafile = conf.get('cafile'),
    capath = conf.get('capath'),
    cadata = conf.get('cadata')
    while True:
        log.debug("ask launcher {}".format(launcher.name))
        cmd = await launcher.ask(cmd_resp)
        if isinstance(cmd, cg_launcher.LcTerminate):
            log.debug('terminate launcher {}'.format(launcher.name))
            break
        if isinstance(cmd, cg_launcher.LcIdle):
            log.debug('idle launcher {}'.format(launcher.name))
            await _do_idle(cmd, loop)
            continue
        # isinstance(cmd, cg_launcher.LcFire)
        before = loop.time()
        log.debug('before:{}'.format(before))
        succeeded, failed = await _do_fire(uri,
                                           companion_plugins, cmd, loop,
                                           cleansession=cleansession,
                                           cafile=cafile,
                                           capath=capath,
                                           cadata=cadata)
        after = loop.time()
        log.debug('after:{}'.format(after))
        cmd_resp = LcResp(cmd, succeeded, failed)


async def runner_runner(ctx: RunnerContext, loop: BaseEventLoop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    coros = []
    for lp_conf in ctx.launcher_plugins:
        lc = lp_conf.new_instance(loop)
        cp_confs = ctx.launcher_companion_plugins[lc['name']]
        coros.append(launcher_runner(lc, cp_confs))
    await asyncio.wait(coros)


