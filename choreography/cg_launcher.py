# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
import asyncio

import logging
log = logging.getLogger(__name__)


class Fire(object):
    """
    Fire 'rate' clients every 'step' seconds for 'num_steps' times.
    """
    def __init__(self, rate: int, step: int, num_steps: int, timeout: int,
                 conf_awaitable):
        self.rate = rate
        self.step = step
        self.num_steps = num_steps
        self.conf_awaitable = conf_awaitable


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
    def __init__(self, name, config, loop=None):
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
    async def ask(self, resp: LauncherCmdResp=None, loop=None) -> LauncherCmd:
        log.debug('IdleLauncher resp:{}'.format(resp))
        return LauncherCmd(Idle(elapse=self.config.get('eclapse', 1)))


class OneShotLauncher(Launcher):
    """
    fire, terminate
    """

    async def conf_awaitable(self):
        return self.config


    async def ask(self, resp: LauncherCmdResp=None, loop=None) -> LauncherCmd:
        log.debug('OneShotLauncher resp:{}'.format(resp))
        if resp is None:
            return LauncherCmd(Terminate())
        else:
            return LauncherCmd(Fire(rate=self.config.get('rate', 1), step=1,
                                    num_steps=1, timeout=10,
                                    conf_awaitable=self.conf_awaitable),
                               opaque=True)


