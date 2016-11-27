# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_exception import CgException, CgLauncherException
from choreography.cg_util import gen_client_id, get_delay
from choreography.cg_util import StepRespModel, StepResp
from hbmqtt.mqtt.connack import CONNECTION_ACCEPTED
import asyncio
from asyncio import BaseEventLoop
import random
import attr
from autologging import logged


class LcCmd(abc.ABC):
    pass


@attr.s(frozen=True)
class LcFire(LcCmd):
    """
    Fire 'rate' clients, only coming back to ask after 't' secs where:
        if 'timeout' is 0:
            duration < t == clients are all done(either connected or failed)
        otherwise:
            duration < t < max(duration, timeout)
    """
    cids = attr.ib(default=attr.Factory(list))
    duration = attr.ib(default=0)
    timeout = attr.ib(default=0)
    #def __init__(self, rate: int, conf_queue: asyncio.Queue, duration: float=0.,
    #             timeout: float=0.):
    #    self.rate = rate
    #    self.duration = duration
    #    self.timeout = timeout
    #    self.conf_queue = conf_queue


class LcTerminate(LcCmd):
    """
    Don't ever come back to ask
    """
    pass


@attr.s(frozen=True)
class LcIdle(LcCmd):
    """
    Come back after 'duration' seconds
    """
    duration = attr.ib(default=0)


@attr.s(frozen=True)
class LcResp(object):
    prev_cmd = attr.ib(validator=attr.validators.instance_of(LcCmd))
    succeeded = attr.ib(default=0)
    failed = attr.ib(default=0)


class Launcher(abc.ABC):
    @abc.abstractmethod
    def __init__(self, context: CgConext, config):
        self.context = context
        self.config = config
        self.loop = context

    @abc.abstractmethod
    async def ask(self, resp: LcResp=None) -> LcCmd:
        """
        :param resp:
        :return:
        """


@logged
class MonoIncLauncher(StepResp, Launcher):
    """
    fire, terminate
    after 'delay' secs, create and connect 'rate' number of clients using
    'step' secs for 'num_steps' times.

    In each step, it may takes more then 'step' seconds. Moreover,
    'auto_reconnect' will affect the time well.
    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop=None):
        try:
            super().__init__(namespace, plugin_name, name, config, loop)
            self.model = StepRespModel(responder=self,
                                       num_steps=self.config.get('num_steps', 1),
                                       step=self.config.get('step', 1),
                                       delay=get_delay(config),
                                       loop=loop)
            self.rate = self.config.get('rate', 1)
            if self.rate < 0:
                raise CgLauncherException('Invalid rate={}'.format(self.rate))

            self.client_id_prefix = self.config.get('client_id_prefix')
            self.fu = None
            self.lc_cmd = None
            self.__log.info('{} args: rate={}, step ={}, num_steps={}, delay={}'.
                            format(self.name, self.rate, self.model.step,
                                   self.model.num_steps, self.model.delay))
        except CgLauncherException as e:
            raise e
        except Exception as e:
            raise CgLauncherException('Invalid configs') from e

    # helpers
    def set_fire(self, count):
        async def put_conf(queue, maxsize):
            for _ in range(0, maxsize):
                if self.client_id_prefix is None:
                    # autogen when client_id is None
                    await queue.put((gen_client_id(), self.config))
                else:
                    await queue.put((gen_client_id(self.client_id_prefix),
                                     self.config))

        queue = asyncio.Queue(maxsize=count, loop=self.loop)
        self.fu = self.loop.create_task(put_conf(queue, count))
        self.lc_cmd = LcFire(rate=count, conf_queue=queue, duration=0)

    # StepResp implementation
    def run_delay(self):
        self.__log.debug('delay for {}'.format(self.model.delay))
        self.lc_cmd = LcIdle(duration=self.model.delay)

    def run_offset(self):
        self.__log.debug('fire offset {}'.format(self.model.offset))
        if self.fu is not None:
            raise CgLauncherException('task of queue should be None')
        self.set_fire(self.model.offset)

    def run_step(self):
        self.__log.debug('fire {} at step {}'.format(self.rate,
                                                     self.model.current_step()))
        if self.fu is not None:
            self.fu.cancel()
        self.set_fire(self.rate)

    def run_idle(self):
        now = self.loop.time()
        diff = max(self.model.step_start_t + self.model.step - now, 0)
        self.__log.debug('idle for {}'.format(diff))
        self.lc_cmd = LcIdle(duration=diff)

    def run_done(self):
        self.__log.debug('terminate')
        self.lc_cmd = LcTerminate()

    # Launcher implementation
    async def ask(self, resp: LcResp=None) -> LcCmd:
        self.model.ask()
        return self.lc_cmd


class LauncherRunner(abc.ABC):
    def __init__(self, context: CgContext, launcher: Launcher):
        pass

    @abc.abstractmethod
    async def run(self, cmd_resp: LcResp=None):
        pass


@logged
class LauncherRunnerDefault(LauncherRunner):

    def __init__(self, context: CgContext, launcher: Launcher):
        self.context = context
        self.launcher = launcher

    async def __connect_task(self, client: CgClient, **kwargs):
        log = self.__log
        loop = client.get_loop()

        def connect_cb(fu: asyncio.Future):
            try:
                ret = fu.result()
                if ret == CONNECTION_ACCEPTED:
                    companion = self.context.companion_cls(
                        self.context, **self.context.companion_conf)
                    loop.create_task(client.run(companion=companion))
                else:
                    # will be disconnected from the server side
                    log.error('{} connect failed {}'.format(client.client_id,
                                                            ret))
            except ConnectException as e:
                log.exception(e)
            except Exception as e:
                log.exception(e)
                loop.create_task(client.disconnect())

        task = loop.create_task(client.connect(**self.context.broker_conf))
        task.add_done_callback(functools.partial(connect_cb, loop=loop))

    async def run_fire(self, cmd: LcFire):
        clients = [CgClient(client_id=cid, config=self.context.client_conf,
                            loop=self.context.loop) for cid in cmd.cids]
        jobs = [self.__connect_task(c) for c in clients]
        if cmd.duration > 0:
            jobs.append(asyncio.sleep(cmd.duration))
        done, _ = await asyncio.wait(jobs)
        succeeded = 0
        for d in done:
            if d.exception() is None and d.result() == CONNECTION_ACCEPTED:
                succeeded += 1
        return succeeded

    async def run(self, cmd_resp: LcResp=None):
        try:
            log = self.__log
            cmd = await asyncio.wait_for(self.launcher.ask(cmd_resp))
            if isinstance(cmd, LcTerminate):
                log.debug('terminate {}'. format(self.launcher.name))
            elif isinstance(cmd, LcIdle):
                log.debug('idle {}'. format(self.launcher.name))
                if cmd.duration >= 0:
                    await asyncio.sleep(cmd.duration, loop=self.context.loop)
                self.context.loop.create_task(self.run(LcResp(cmd)))
            elif isinstance(cmd, LcFire):
                log.debug('fire {}'. format(self.launcher.name))
                succeeded = await self.run_fire(cmd)
                self.context.loop.create_task(self.run(
                    LcResp(cmd, succeeded=succeeded,
                           failed=len(cmd.cids) - succeeded)))
            else:
                raise CgLauncherException('unsupported command {}'.format(cmd))
        except CgException as e:
            raise e
        except Exception as e:
            raise CgException from e


