# vim:fileencoding=utf-8
import asyncio
from asyncio import BaseEventLoop
import abc
from typing import List, Union, NamedTuple
import logging
log = logging.getLogger(__name__)


Idle = NamedTuple('Idle', [('elapse', int)])
Terminate = NamedTuple('Terminate', [])


class Fire(metaclass=abc.ABCMeta):
    pass


class Subscribe(Fire):
    def __init__(self, topic, qos, elapse):
        super().__init__()
        self.topic = topic
        self.qos = qos
        self.elapse = elapse


class Publish(Fire):
    def __init__(self, topic, qos, elapse, msg):
        super().__init__()
        self.topic = topic
        self.qos = qos
        self.elapse = elapse
        self.msg = msg


CpRespItem = NamedTuple('CpRespItem',
                        [('at', int), ('succeeded', int), ('failed', int)])


class CpCmd(object):
    pass


class CpResp(object):
    def __init__(self, history: List[CpRespItem]=None):
        self._history = [] if history is None else history
        self.opaque = None

    def update(self, prev_cmd: CpCmd, history: List[CpRespItem]):
        self._history = history
        self.opaque = prev_cmd.opaque
    pass


class Companion(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def ask(self, resp: CpResp=None) -> CpCmd:
        """

        :param resp:
        :return:
        """


