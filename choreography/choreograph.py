# vim:fileencoding=utf-8

from hbmqtt.client import MQTTClient, ClientException, ConnectException
from hbmqtt.errors import HBMQTTException
from hbmqtt.client import mqtt_connected
from hbmqtt.session import IncomingApplicationMessage
from choreography import cg_launcher
from choreography.cg_launcher import Launcher, LcResp
from choreography import cg_companion
from choreography.cg_companion import Companion, CpResp
from choreography import cg_util
from choreography.cg_util import CompanionPluginConf, RunnerContext
from choreography.cg_exception import *
from choreography.cg_metrics import *
import random
from typing import List
import asyncio
from asyncio import BaseEventLoop
from prometheus_async.aio import count_exceptions, time, track_inprogress

from autologging import logged

"""
connect_fails_total = Counter('connect_fails_total',
                              'total connect failures')

client_runs_total = Counter('client_runs_total',
                            'total clients connected and run with companions')
"""

class CgClientMetrics(object):
    def __init__(self, client_id: str, launcher_name: str='',
                 runner_name: str=''):
        self.client_id = client_id
        self.launcher_name = launcher_name
        self.runner_name = runner_name


@logged
class CgClient(MQTTClient):
    """

    hbmqtt.client.ClientException derives from BaseException which cannot be
    captured by asyncio so that the event loop will be aborted. Leave us no
    chance to examine a Future's result.
    CgClient replaces ClientException with CgException that derives
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
        if client_id is None:
            client_id = cg_util.gen_client_id()
        super().__init__(client_id, default_config, loop)

        # since wildcards in subscribe/unsubscribe, we can only try our best to
        # avoid unnecessary __do_receive.
        self.companion = None
        self.recv = None

    def is_connected(self):
        return self._connected_state.is_set()

    async def connect(self, uri=None, cleansession=None, cafile=None,
                      capath=None, cadata=None):
        try:
            self.__log.debug('connect to {}'.format(uri))
            ret = await super().connect(uri, cleansession, cafile, capath,
                                        cadata)
            connections_total.inc()
            return ret
        except ClientException as e:
            self.__log.exception(e)
            raise CgConnectException from e

    @time(connect_time)
    async def _connect_coro(self):
        connect_attempts_total.inc()
        await super()._connect_coro()

    async def handle_connection_close(self):
        ret = await super().handle_connection_close()
        if not self.is_connected():
            connections_total.dec()
        return ret

    async def deliver_message(self, timeout=None):
        msg = await super().deliver_message(timeout)
        data = msg.publish_packet.data
        received_bytes_total.inc(len(data))
        return msg

    async def __do_receive(self):
        self.__log.debug('constantly receiving msg: {}'.format(self.client_id))
        count = 0
        try:
            while True:
                try:
                    # copy from @hbmqtt.client.mqtt_connected
                    if not self._connected_state.is_set():
                        self.__log.warning("{} not connected, waiting for it".
                                           format(self.client_id))
                        await self._connected_state.wait()
                    msg = await self.deliver_message()
                    data = msg.publish_packet.data
                    count += 1
                    self.__log.debug('{} receives at {}'.
                                     format(self.client_id, self._loop.time()))
                    self.__log.debug('len={}, msg_{} = {}'.
                                     format(len(data), count,
                                            data.decode('utf-8')))
                    await self.companion.received(msg)
                except asyncio.CancelledError as e:
                    self.__log.exception(e)
                    self.__log.info('{} cancelled'.format(self.client_id))
                    break
                except ConnectException as e:
                    # assert issubclass(ConnectException, ClientException)
                    self.__log.exception(e)
                    raise CgConnectException from e
                except (ClientException, Exception) as e:
                    # assert issubclass(ClientException, BaseException)
                    self.__log.exception(e)
                    # retry
        finally:
            self.__log.warn(' {} leaves')

    @time(subscribe_time)
    async def subscribe(self, topics):
        try:
            ret = await super().subscribe(topics)
            self.recv = self._loop.create_task(self.__do_receive())
            subscribe_total.inc()
            return ret
        except (HBMQTTException, ClientException) as e:
            self.__log.exception(e)
            raise CgSubException from e

    @time(publish_time)
    async def publish(self, topic, message, qos=None, retain=None):
        try:
            ret = await super().publish(topic, message, qos, retain)
            published_bytes_total.inc(len(message))
            publish_total.inc()
            return ret
        except (HBMQTTException, ClientException) as e:
            self.__log.exception(e)
            raise CgPubException from e

    async def disconnect(self):
        try:
            ret = await super().disconnect()
            return ret
        except (HBMQTTException, ClientException) as e:
            self.__log.exception(e)
            raise CgSubException from e

    async def __do_fire(self, fire: cg_companion.CpFire):
        async def __fire(_f):
            if isinstance(_f, cg_companion.CpSubscribe):
                await self.subscribe(_f.topics)
                self.__log.debug('{} subscribed'.format(self.client_id))
                return True
            elif isinstance(_f, cg_companion.CpPublish):
                await self.publish(_f.topic, _f.msg, _f.qos,
                                   retain=_f.retain)
                self.__log.debug('{} published'.format(self.client_id))
                return True
            elif isinstance(_f, cg_companion.CpDisconnect):
                await self.disconnect()
                self.__log.debug('{} disconnected'.format(self.client_id))
                return True
            else:
                self.__log.error('{} unknown {}'.format(self.client_id, _f))
                return False

        tasks = [__fire(fire)]
        if fire.duration > 0:
            tasks.append(asyncio.sleep(fire.duration))
        done, _ = await asyncio.wait(tasks)
        for d in done:
            result = cg_util.future_successful_result(d)
            if result is True:
                return True
        return False

    async def run(self, companion: Companion):
        @cg_util.convert_coro_exception(Exception, CgCompanionException)
        async def _asking(coro):
            return await coro
        self.__log.debug('running: {} with {}'.format(self.client_id, companion))
        self.companion = companion
        idle_forever = False
        # a task for constantly receiving messages
        recv = self._loop.create_task(self.__do_receive())
        try:
            cmd_prev = None
            cmd_resp = None
            while True:
                self.__log.debug("ask companion: {}".format(self.client_id))
                done, _ = await asyncio.wait([_asking(companion.ask(cmd_resp))])
                cmd = cg_util.future_successful_result(done.pop())
                if cmd is None:
                    self.__log.warn('can\'t retrieve cmd')
                    # ask again with cmd_prev
                    cmd_resp = CpResp(cmd_prev, succeeded=False)
                    continue
                if isinstance(cmd, cg_companion.CpTerminate):
                    self.__log.debug('terminate {}'.format(self.client_id))
                    break
                if isinstance(cmd, cg_companion.CpIdle):
                    self.__log.debug('idle {}'.format(self.client_id))
                    if cmd.duration > 0:
                        await asyncio.sleep(cmd.duration, loop=self._loop)
                        cmd_resp = CpResp(cmd, succeeded=True)
                        cmd_prev = cmd
                        continue
                    else:
                        idle_forever = True
                        break
                result = await self.__do_fire(cmd)
                if isinstance(cmd, cg_companion.CpDisconnect):
                    self.__log.debug('disconnect & terminate {}'.
                              format(self.client_id))
                    break
                cmd_resp = CpResp(cmd, succeeded=result)
                cmd_prev = cmd
        finally:
            # if idle_forever then CgClient keep receiving messages
            if not idle_forever and self.recv:
                self.__log.debug('neither companion.ask nor receive')
                self.recv.cancel()
            elif self.recv:
                self.__log.debug('won\'t companion.ask but keep receiving')


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


# TODO: this should be refactored into two part
# It creates a task to client.connect asynchronously, when connected, create a
# task to client.run asynchronously.
# This means two things:
# 1. Upper coro that chooses to wait the client.connect can cancel it but won't
#    be able to cancel client.run.
# 2. Conversely, exceptions leaked from client.run will not propagate to the
#    upper coro and will go directly to the asyncio event loop.
@logged
def _do_client_run(uri: str,
                cp_conf: CompanionPluginConf,
                fire: cg_launcher.LcFire,
                loop: BaseEventLoop,
                cleansession: bool=None,
                cafile: str=None,
                capath: str=None,
                cadata: str=None) -> asyncio.Task:

    @logged
    async def connect():
        client_id, conf = await fire.conf_queue.get()
        cc = CgClient(client_id=client_id, config=conf, loop=loop)
        connect._log.debug('CgClient {} await connect'.format(cc.client_id))
        await cc.connect(uri=uri, cleansession=cleansession, cafile=cafile,
                         capath=capath, cadata=cadata)
        connect._log.debug('CgClient {} connect awaited'.format(cc.client_id))
        return cc

    @logged
    def connect_cb(_fu: asyncio.Future):
        cc = cg_util.future_successful_result(_fu)
        if cc is None:
            return
        if not cc.is_connected():
            connect_cb._log.info('connect is not connected: {}'.format(cc))
            return
        if cp_conf is None:
            connect_cb._log.info('skip running because no companion plugs')
            return
        # TODO: exception is dropped in callback?
        cp = cp_conf.new_instance(loop, name=cc.client_id)
        connect_cb._log.debug('use companion {} to run'.format(cp.name))
        # run asynchorously
        loop.create_task(cc.run(cp))

    task = loop.create_task(connect())
    task.add_done_callback(connect_cb)
    return task


@logged
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
            # asyncio.sleep yields None
            return False
        cc = _task.result()
        if not isinstance(cc, CgClient):
            return False
        return cc.is_connected()

    pick_func = _pick_plugin_conf_func(companion_plugins)

    def run_client(_i):
        p = pick_func(_i + 1)
        return _do_client_run(uri, p, fire, loop, cleansession, cafile, capath,
                           cadata)

    _do_fire._log.debug('at {} for {} secs'.format(loop.time(),
                                                   fire.duration))
    jobs = [run_client(i) for i in range(0, fire.rate)]
    if fire.duration > 0:
        jobs.append(asyncio.sleep(fire.duration))
    timeout = None if fire.timeout == 0 else max(fire.duration, fire.timeout)
    done, pending = await asyncio.wait(jobs, loop=loop, timeout=timeout)
    running = len([d for d in done if is_running_client(d)])
    _do_fire._log.debug('done:{}, running:{}'.format(len(done), running))
    return running, fire.rate - running


@logged
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
    @cg_util.convert_coro_exception(Exception, CgLauncherException)
    async def _asking(coro):
        return await coro
    loop = launcher.loop
    cmd_prev = None
    cmd_resp = None
    conf = launcher.config['broker']
    uri = conf['uri']
    cleansession = conf.get('cleansession')
    cafile = conf.get('cafile')
    capath = conf.get('capath')
    cadata = conf.get('cadata')
    try:
        while True:
            launcher_runner._log.debug("ask launcher {}".format(launcher.name))
            done, _ = await asyncio.wait([_asking(launcher.ask(cmd_resp))])
            cmd = cg_util.future_successful_result(done.pop())
            if cmd is None:
                launcher_runner._log.warn('can\'t retrieve cmd')
                cmd_resp = LcResp(cmd_prev)
                continue
            if isinstance(cmd, cg_launcher.LcTerminate):
                launcher_runner._log.debug('terminate launcher {}'.format(launcher.name))
                break
            if isinstance(cmd, cg_launcher.LcIdle):
                launcher_runner._log.debug('idle launcher {}'.
                                           format(launcher.name))
                if cmd.duration > 0:
                    await asyncio.sleep(cmd.duration, loop=loop)
                    cmd_resp = LcResp(cmd)
                    cmd_prev = cmd
                    continue
                else:
                    launcher_runner._log.debug('terminate launcher {}'.
                                               format(launcher.name))
                    break
            # isinstance(cmd, cg_launcher.LcFire)
            before = loop.time()
            launcher_runner._log.debug('before:{}'.format(before))
            launcher_runner._log.debug('uri={}, cleansession={}, cafile={}, '
                                       'capath={}, cadata={}'.
                                       format(uri, cleansession, cafile, capath,
                                              cadata))
            succeeded, failed = await _do_fire(uri,
                                               companion_plugins, cmd, loop,
                                               cleansession=cleansession,
                                               cafile=cafile,
                                               capath=capath,
                                               cadata=cadata)
            after = loop.time()
            launcher_runner._log.debug('after:{}'.format(after))
            cmd_resp = LcResp(cmd, succeeded, failed)
            cmd_prev = cmd
    except Exception as e:
        launcher_runner._log.exception(e)
        raise CgException from e


@logged
async def runner_runner(ctx: RunnerContext, loop: BaseEventLoop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    coros = []
    for lc_conf in ctx.launcher_plugins:
        # unlike companions, one launcher conf only creates one instance. I.e.
        # lc_conf.name == launcher.plugin_name == launcher.name
        lc = lc_conf.new_instance(loop, name=lc_conf.name)
        cp_confs = ctx.launcher_companion_plugins[lc_conf.name]
        coros.append(launcher_runner(lc, cp_confs))
    await asyncio.wait(coros)


