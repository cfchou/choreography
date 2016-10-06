# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
import asyncio
from asyncio import BaseEventLoop

import logging
log = logging.getLogger(__name__)


class Fire(object):
    """
    Fire 'rate' clients every 'step' seconds for 'num_steps' times.
    """
    def __init__(self, rate: int, step: int, num_steps: int, timeout: int,
                 conf_queue: asyncio.Queue):
        self.rate = rate
        self.step = step
        self.num_steps = num_steps
        self.timeout = timeout
        self.conf_queue = conf_queue


# don't ever come back to ask
Terminate = NamedTuple('Terminate', [])

# come back after 'elapse' seconds
Idle = NamedTuple('Idle', [('elapse', int)])


LauncherCmdRespItem = NamedTuple('LauncherCmdRespItem',
                                 [('at', int), ('succeeded', int),
                                  ('failed', int)])


class LauncherCmd(object):
    def __init__(self, action: Union[Fire, Idle], opaque=None):
        self.action = action
        self.opaque = opaque

    def is_fire(self):
        return isinstance(self.action, Fire)

    def is_idle(self):
        return isinstance(self.action, Idle)

    def is_terminate(self):
        return isinstance(self.action, Terminate)


class LauncherCmdResp(object):
    def __init__(self, history: List[LauncherCmdRespItem]=None):
        self._history = [] if history is None else history
        self.opaque = None

    def update(self, prev_cmd: LauncherCmd, history: List[LauncherCmdRespItem]):
        self._history = history
        self.opaque = prev_cmd.opaque


class Launcher(metaclass=abc.ABCMeta):
    def __init__(self, name, config, loop: BaseEventLoop=None):
        self.name = name
        self.config = config
        self.loop = asyncio.get_event_loop() if loop is None else loop

    @abc.abstractmethod
    async def ask(self, resp: LauncherCmdResp=None) -> LauncherCmd:
        """
        :param resp:
        :return:
        """


class IdleLauncher(Launcher):
    """
    idle, idle, idle...
    """
    async def ask(self, resp: LauncherCmdResp=None) -> LauncherCmd:
        log.debug('IdleLauncher resp:{}'.format(resp))
        return LauncherCmd(Idle(elapse=self.config.get('eclapse', 1)))


class OneShotLauncher(Launcher):
    """
    fire, terminate
    """
    def __init__(self, name, config, loop: BaseEventLoop=None):
        super().__init__(name, config, loop)
        self.rate = self.config.get('rate', 1)
        self.step = self.config.get('step', 1)
        self.num_steps = self.config.get('num_steps', 1)
        self.fu = None

    async def ask(self, resp: LauncherCmdResp=None) -> LauncherCmd:
        maxsize = self.rate * self.num_steps

        async def put_conf(q):
            for _ in range(0, maxsize):
                await q.put(self.config)

        log.debug('OneShotLauncher resp:{}'.format(resp))
        if self.fu is not None:
            self.fu.cancel()

        if resp is None:
            return LauncherCmd(Terminate())
        else:
            queue = asyncio.Queue(maxsize=maxsize,
                                  loop=self.loop)
            self.fu = self.loop.create_task(put_conf(queue))
            return LauncherCmd(Fire(rate=self.rate, step=self.step,
                                    num_steps=self.num_steps, timeout=10,
                                    conf_queue=queue),
                               opaque=True)


