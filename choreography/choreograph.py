# vim:fileencoding=utf-8

from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.session import IncomingApplicationMessage
from choreography import cg_launcher
from choreography.cg_launcher import Launcher, LcResp
from choreography import cg_companion
from choreography.cg_companion import Companion, CpResp
from choreography.cg_exception import *
from choreography import cg_util
from choreography.cg_util import CompanionPluginConf, RunnerContext
import random
from typing import List
import asyncio
from asyncio import BaseEventLoop
from prometheus_client import Counter
from prometheus_async.aio import count_exceptions, time, track_inprogress

import logging
log = logging.getLogger(__name__)

connect_fails_total = Counter('connect_fails_total',
                              'total connect failures')

client_runs_total = Counter('client_runs_total',
                            'total clients connected and run with companions')


class CgClientMetrics(object):
    def __init__(self, client_id: str, launcher_name: str='',
                 runner_name: str=''):
        self.client_id = client_id
        self.launcher_name = launcher_name
        self.runner_name = runner_name


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

    @count_exceptions(connect_fails_total)
    async def connect(self, uri=None, cleansession=None, cafile=None,
                      capath=None, cadata=None):
        try:
            log.debug('connect to {}'.format(uri))
            return await super().connect(uri, cleansession, cafile, capath,
                                         cadata)
        except ClientException as e:
            log.exception(e)
            raise CgClientException from e

    def deliver_message(self, timeout=None):
        return super().deliver_message(timeout)

    async def __receive(self, companion: Companion):
        while True:
            msg = await self.deliver_message()
            await companion.received(msg)

    async def __do_idle(self, idle: cg_companion.CpIdle):
        if idle.duration > 0:
            await asyncio.sleep(idle.duration, loop=self._loop)

    async def __do_fire(self, fire: cg_companion.CpFire):
        if isinstance(fire, cg_companion.Subscribe):
            pass
        if isinstance(fire, cg_companion.Publish):
            pass
        if isinstance(fire, cg_companion.Disconnect):
            return True

    async def run(self, companion: Companion):
        log.debug('running: {} with {}'.format(self.client_id, companion))
        # a task for constantly receiving messages
        recv = self._loop.create_task(self.__receive(companion))
        try:
            cmd_prev = None
            cmd_resp = None
            while True:
                log.debug("run: {}".format(self.client_id))
                done, _ = await asyncio.wait([companion.ask(cmd_resp)])
                cmd = cg_util.future_successful_result(done.pop())
                if cmd is None:
                    log.warn('can\'t retrieve cmd')
                    # ask again with cmd_prev
                    cmd_resp = CpResp(cmd_prev, succeeded=False)
                    continue
                if isinstance(cmd, cg_companion.CpTerminate):
                    log.debug('terminate {}'.format(self.client_id))
                    break
                if isinstance(cmd, cg_companion.CpIdle):
                    log.debug('idle {}'.format(self.client_id))
                    await self.__do_idle(cmd)
                    cmd_resp = CpResp(cmd, succeeded=True)
                    cmd_prev = cmd
                    continue
                result = await self.__do_fire(cmd)
                if isinstance(cmd, cg_companion.Disconnect):
                    log.debug('disconnect & terminate {}'.
                              format(self.client_id))
                    break
                cmd_resp = CpResp(cmd, succeeded=result)
                cmd_prev = cmd
        finally:
            recv.cancel()


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
                cp_conf: CompanionPluginConf,
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
        cc = cg_util.future_successful_result(_fu)
        if cc is None:
            return
        if not cc.is_connected():
            log.info('connect is not connected: {}'.format(cc))
            return
        log.info('connect result: {}'.format(cc))
        if cp_conf is None:
            log.info('skip running because no companion plugs')
            return
        cp = cp_conf.new_instance(loop, name=cc.client_id)
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
        cc = cg_util.future_successful_result(_task)
        if cc is None:
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
    jobs = [run_client(i) for i in range(0, fire.rate)]
    if fire.duration > 0:
        jobs.append(asyncio.sleep(fire.duration))
    timeout = None if fire.timeout is 0 else max(fire.duration, fire.timeout)
    done, pending = await asyncio.wait(jobs, loop=loop, timeout=timeout)
    running = len([d for d in done if is_running_client(d)])
    log.debug('_dofire: done:{}, running:{}'.format(len(done), running))
    return running, fire.rate - running


async def _do_idle(idle: cg_launcher.LcIdle, loop: BaseEventLoop=None):
    if idle.duration > 0:
        await asyncio.sleep(idle.duration, loop=loop)


async def launcher_runner(launcher: Launcher,
                          companion_plugins: List[CompanionPluginConf]):
    """
    TODO: an Launcher instance should only be run by launcher_runner only once
    might need to embed a state in Launcher to ensures that launcher.ask is
    re-entrant free.

    :param launcher:
    :param companion_plugins:
    :return:
    """
    loop = launcher.loop
    cmd_prev = None
    cmd_resp = None
    conf = launcher.config['broker']
    uri = conf['uri']
    cleansession = conf.get('cleansession'),
    cafile = conf.get('cafile'),
    capath = conf.get('capath'),
    cadata = conf.get('cadata')
    while True:
        log.debug("ask launcher {}".format(launcher.name))
        done, _ = await asyncio.wait([launcher.ask(cmd_resp)])
        cmd = cg_util.future_successful_result(done.pop())
        if cmd is None:
            log.warn('can\'t retrieve cmd')
            cmd_resp = LcResp(cmd_prev)
            continue
        if isinstance(cmd, cg_launcher.LcTerminate):
            log.debug('terminate launcher {}'.format(launcher.name))
            break
        if isinstance(cmd, cg_launcher.LcIdle):
            log.debug('idle launcher {}'.format(launcher.name))
            await _do_idle(cmd, loop)
            cmd_resp = LcResp(cmd)
            cmd_prev = cmd
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
        cmd_prev = cmd


async def runner_runner(ctx: RunnerContext, loop: BaseEventLoop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    coros = []
    for lc_conf in ctx.launcher_plugins:
        # unlike companions, one launcher conf only creates one instance.
        lc = lc_conf.new_instance(loop, name=lc_conf.name)
        cp_confs = ctx.launcher_companion_plugins[lc_conf.name]
        coros.append(launcher_runner(lc, cp_confs))
    await asyncio.wait(coros)


