# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_exception import CgLauncherException
from choreography.cg_util import gen_client_id, get_delay
from choreography.cg_util import StepRespModel, StepResp
import asyncio
from asyncio import BaseEventLoop
import random
import attr
from autologging import logged


class LcCmd(abc.ABC):
    @abc.abstractmethod
    def __init__(self):
        pass


class LcFire(LcCmd):
    """
    Fire 'rate' clients, only coming back to ask after 't' secs where:
        if 'timeout' is 0:
            duration < t == clients are all done(either connected or failed)
        otherwise:
            duration < t < max(duration, timeout)
    """
    def __init__(self, rate: int, conf_queue: asyncio.Queue, duration: float=0.,
                 timeout: float=0.):
        self.rate = rate
        self.duration = duration
        self.timeout = timeout
        self.conf_queue = conf_queue


class LcTerminate(LcCmd):
    """
    Don't ever come back to ask
    """
    def __init__(self):
        pass


class LcIdle(LcCmd):
    """
    Come back after 'duration' seconds

    duration == 0 means LcTerminate
    """
    def __init__(self, duration: float=1.):
        self.duration = duration


class LcResp(object):
    def __init__(self, prev_cmd: LcCmd, succeeded: int=0, failed: int=0):
        self.prev_cmd = prev_cmd
        self.succeeded = succeeded
        self.failed = failed


class Launcher(abc.ABC):
    @abc.abstractmethod
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop=None):
        self.namespace = namespace
        self.plugin_name = plugin_name
        self.name = name
        self.config = config
        self.loop = asyncio.get_event_loop() if loop is None else loop

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

