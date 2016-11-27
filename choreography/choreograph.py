# vim:fileencoding=utf-8

from hbmqtt.client import MQTTClient, ClientException, ConnectException
from hbmqtt.errors import HBMQTTException
from hbmqtt.client import mqtt_connected
from hbmqtt.session import IncomingApplicationMessage
from choreography import cg_launcher
from choreography.cg_launcher import LauncherRunnerDefault
from choreography import cg_companion
from choreography.cg_companion import Companion, CpResp
from choreography import cg_util
from choreography.cg_util import CompanionPluginConf, RunnerContext
from choreography.cg_exception import *
from choreography.cg_metrics import *
from choreography.cg_context import CgContext, CgMetrics
from prometheus_async.aio import web
from stevedore import NamedExtensionManager, DriverManager
import random
from typing import List
import asyncio
from asyncio import BaseEventLoop
from prometheus_async.aio import count_exceptions, time, track_inprogress
from uuid import uuid1
import attr
from attr import validators
import abc
import functools
import itertools
import pprint

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

    def get_loop(self):
        return self._loop

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

    @time(connect_hist)
    async def _connect_coro(self):
        await super()._connect_coro()

    async def handle_connection_close(self):
        ret = await super().handle_connection_close()
        if not self.is_connected():
            # disconnect passively
            connections_total.dec()
        return ret

    async def deliver_message(self, timeout=None):
        msg = await super().deliver_message(timeout)
        data = msg.publish_packet.data
        received_bytes_total.inc(len(data))
        received_total.inc()
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
                    data_len = len(data)
                    data_uc = data.decode('utf-8')
                    count += 1
                    self.__log.info('{} receives {} bytes at {}'.
                                    format(self.client_id, data_len,
                                           self._loop.time()))
                    prefix = data_uc[:min(48, data_len)]
                    self.__log.info('msg_{} = {}'.format(count, prefix))
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

    @time(subscribe_hist)
    async def subscribe(self, topics):
        try:
            ret = await super().subscribe(topics)
            self.recv = self._loop.create_task(self.__do_receive())
            return ret
        except (HBMQTTException, ClientException) as e:
            self.__log.exception(e)
            raise CgSubException from e

    @time(publish_hist)
    async def publish(self, topic, message, qos=None, retain=None):
        try:
            ret = await super().publish(topic, message, qos, retain)
            published_bytes_total.inc(len(message))
            return ret
        except (HBMQTTException, ClientException) as e:
            self.__log.exception(e)
            raise CgPubException from e

    async def disconnect(self):
        try:
            ret = await super().disconnect()
            # disconnect actively
            connections_total.dec()
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
        try:
            cmd_prev = None
            cmd_resp = None
            while True:
                self.__log.debug("ask companion: {}".format(self.client_id))
                # TODO: wait receive(deliver_message) in the same loop here
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
                    if cmd.duration >= 0:
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


@logged
def cg_custom_loop(package_name) -> asyncio.BaseEventLoop:
    if package_name == 'uvloop':
        import uvloop
        policy = uvloop.EventLoopPolicy()
        asyncio.set_event_loop_policy(policy=policy)
    else:
        cg_custom_loop._log('{} not supported'.format(package_name))
    return asyncio.get_event_loop()


@logged
def cg_create_metrics_task(config, service_id, loop=None):
    log = cg_create_metrics_task._log
    if loop is None:
        loop = asyncio.get_event_loop()
    # service discovery
    sd_host = config['service_discovery']['host']
    sd_port = config['service_discovery']['port']
    log.info('*****Service discovery service {} at {}:{}*****'.
             format(service_id, sd_host, sd_port))

    agent = cg_util.SdConsul(name='cg_metrics', service_id=service_id, host=sd_host,
                             port=sd_port)
    # metrics exposure
    ex_host = config['prometheus_exposure']['host']
    ex_port = config['prometheus_exposure']['port']
    log.info('*****Metrics exposed at {}:{}*****'. format(ex_host, ex_port))

    # TODO: initialise custom metrics and update context
    loop.create_task(web.start_http_server(addr=ex_host, port=ex_port,
                                           loop=loop, service_discovery=agent))
    return CgMetrics()


@logged
def cg_create_context(config, service_id, metrics=None, loop=None):
    try:
        log = cg_create_context._log
        if loop is None:
            loop = asyncio.get_event_loop()
        # load companion plugin
        companion_cls = DriverManager(namespace='choreography.companion_plugins',
                                      name=config['companion']['plugin'],
                                      invoke_on_load=False).driver
        # prepare context
        return CgContext(service_id=service_id, metrics=metrics,
                         broker_conf=config['broker'],
                         launcher_conf=config['launcher'],
                         client_conf=config['client'],
                         companion_cls=companion_cls,
                         companion_conf=config['companion'], loop=loop)
    except Exception as e:
        raise CgException from e


@logged
def cg_create_launcher_task(config, context: CgContext, launcher_runner=None):
    try:
        log = cg_create_launcher_task._log
        # load launcher plugin and create launcher
        plugin = config['launcher']['plugin']
        log.info('load launcher plugin {} and create'.format(plugin))
        launcher = DriverManager(namespace='choreography.launcher_plugins',
                                 name=plugin,
                                 invoke_on_load=True,
                                 invoke_kwds={
                                     'context': context,
                                     **config['launcher'].get('config', dict())
                                 }).driver
        if launcher_runner is None:
            launcher_runner = LauncherRunnerDefault(context, launcher)
        return context.loop.create_task(launcher_runner.run())
    except Exception as e:
        raise CgException from e


@logged
def cg_run_forever(config):
    log = cg_run_forever._log
    def get_loop(custom_loop):
        if custom_loop == 'uvloop':
            log.info('custom loop {}'.format(custom_loop))
            return cg_custom_loop('uvloop')
        else:
            return asyncio.get_event_loop()

    service_id = config.get('service_id', uuid1())
    loop = get_loop(config.get('custom_loop'))
    #metrics = cg_create_metrics_task(config, service_id, loop)
    #context = cg_create_context(config, service_id, metrics=metrics, loop=loop)
    context = cg_create_context(config, service_id, loop=loop)
    cg_create_launcher_task(config, context)
    loop.run_forever()





