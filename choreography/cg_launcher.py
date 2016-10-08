# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
import asyncio
from asyncio import BaseEventLoop

import logging
log = logging.getLogger(__name__)


class LcCmd(metaclass=abc.ABCMeta):
    pass


class LcFire(LcCmd):
    """
    # Fire 'rate' clients
    # only coming back to ask after t secs where:
    # duration < t < max(duration, timeout)
    """
    def __init__(self, rate: int, duration: int, timeout: int,
                 conf_queue: asyncio.Queue):
        self.rate = rate
        self.duration = duration
        self.timeout = timeout
        self.conf_queue = conf_queue


class LcTerminate(LcCmd):
    """
    # don't ever come back to ask
    Terminate = NamedTuple('Terminate', [])
    """


class LcIdle(LcCmd):
    """
    come back after 'duration' seconds
    Idle = NamedTuple('Idle', [('duration', int)])
    """
    def __init__(self, duration):
        self.duration = duration


class LcResp(object):
    def __init__(self, prev_cmd: LcCmd, succeeded: int, failed: int):
        self.prev_cmd = prev_cmd
        self.succeeded = succeeded
        self.failed = failed


class Launcher(metaclass=abc.ABCMeta):
    def __init__(self, name, config, loop: BaseEventLoop=None):
        self.name = name
        self.config = config
        self.loop = asyncio.get_event_loop() if loop is None else loop

    @abc.abstractmethod
    async def ask(self, resp: LcResp=None) -> LcCmd:
        """
        :param resp:
        :return:
        """


class IdleLauncher(Launcher):
    """
    idle, idle, idle...
    """
    async def ask(self, resp: LcResp=None) -> LcCmd:
        log.debug('IdleLauncher resp:{}'.format(resp))
        return LcIdle(duration=self.config.get('duration', 1))


class OneShotLauncher(Launcher):
    """
    fire, terminate
    """
    def __init__(self, name, config, loop: BaseEventLoop=None):
        super().__init__(name, config, loop)
        self.rate = self.config.get('rate', 1)
        self.duration = self.config.get('duration', 1)
        self.timeout = self.config.get('timeout', self.duration)
        self.fu = None

    async def ask(self, resp: LcResp=None) -> LcCmd:
        maxsize = self.rate

        async def put_conf(q):
            for _ in range(0, maxsize):
                # autogen when client_id is None
                await q.put((None, self.config))

        log.debug('OneShotLauncher resp:{}'.format(resp))
        if self.fu is not None:
            self.fu.cancel()
            return LcTerminate()

        queue = asyncio.Queue(maxsize=maxsize,
                              loop=self.loop)
        self.fu = self.loop.create_task(put_conf(queue))
        return LcFire(rate=self.rate, duration=self.duration,
                      timeout=self.timeout, conf_queue=queue)


