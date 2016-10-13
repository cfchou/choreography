# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_exception import CgLauncherException
import asyncio
from asyncio import BaseEventLoop

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
class IdleLauncher(Launcher):
    """
    idle, idle, idle...
    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop = None):
        super().__init__(namespace, plugin_name, name, config, loop)

    async def ask(self, resp: LcResp=None) -> LcCmd:
        self.__log.debug('IdleLauncher resp:{}'.format(resp))
        return LcIdle(duration=self.config.get('duration', 1.))


@logged
class OneShotLauncher(Launcher):
    """
    fire, terminate
    after 'delay' secs, launch 'rate' number of clients within 'timeout' secs.

    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop=None):
        super().__init__(namespace, plugin_name, name, config, loop)
        # parameters optional
        self.rate = self.config.get('rate', 1)
        self.timeout = self.config.get('timeout', 0.)
        self.__log.debug('{} args: rate={}, timeout={}'.
                         format(self.name, self.rate, self.timeout))
        self.delay = config.get('delay', 0)

        # stateful
        self.fu = None

    async def ask(self, resp: LcResp=None) -> LcCmd:
        if self.delay > 0:
            self.__log.debug('Idle for {}'.format(self.delay))
            i = self.delay
            self.delay = 0
            return LcIdle(duration=i)

        async def put_conf(q, maxsize):
            for _ in range(0, maxsize):
                # autogen when client_id is None
                await q.put((None, self.config))

        if self.fu is not None:
            self.fu.cancel()
            self.__log.debug('LcTerminate')
            return LcTerminate()

        queue = asyncio.Queue(maxsize=self.rate,
                              loop=self.loop)
        self.fu = self.loop.create_task(put_conf(queue, self.rate))
        self.__log.debug('LcFire')
        return LcFire(rate=self.rate, conf_queue=queue, duration=0,
                      timeout=self.timeout)

