
import abc
from typing import List, Union, NamedTuple
import uuid

import logging
log = logging.getLogger(__name__)

Fire = NamedTuple('Fire', [('rate', int), ('duration', int),
                           ('supervisor', str)])
Idle = NamedTuple('Idle', [])
Terminate = NamedTuple('Terminate', [])


RunnerHistoryItem = NamedTuple('RunnerHistoryItem',
                               [('at', int), ('succeeded', int),
                                ('failed', int)])


class LauncherResp(object):
    def __init__(self, action: Union[Fire, Idle], opaque=None):
        self.action = action
        self.opaque = opaque

    def is_fire(self):
        return isinstance(self.action, Fire)

    def is_idle(self):
        return isinstance(self.action, Idle)

    def is_terminate(self):
        return isinstance(self.action, Terminate)


class RunnerContext(object):
    def __init__(self, host: str, history: List[RunnerHistoryItem]):
        self.host = host
        self._history = [] if history is None else history
        self.opaque = None

    def update(self, prev_resp: LauncherResp, history: List[RunnerHistoryItem]):
        self._history = history
        self.opaque = prev_resp.opaque

    @staticmethod
    def new_conext(host: str=''):
        if not host:
            host = uuid.uuid1().hex
        return RunnerContext(host, [])


class Launcher(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def ask_next(self, runner_ctx: RunnerContext) -> LauncherResp:
        """

        :param runner_ctx:
        :return:
        """


class IdleLancher(Launcher):
    def ask_next(self, runner_ctx: RunnerContext) -> LauncherResp:
        log.debug('runner_ctx:{}'.format(runner_ctx))
        return LauncherResp(Idle())


class OneShotLancher(Launcher):
    def ask_next(self, runner_ctx: RunnerContext) -> LauncherResp:
        log.debug('runner_ctx:{}'.format(runner_ctx.__dict__))
        if runner_ctx.opaque is not None:
            return LauncherResp(Terminate())
        else:
            #return LauncherResp(Fire(rate=1, start=0, end=10, supervisor=''),
            #                    opaque=1)
            return LauncherResp(Fire(rate=1, duration=1, supervisor=''),
                                opaque=1)

