# vim:fileencoding=utf-8
import abc
from typing import List, Union, NamedTuple
from choreography.choreograph import CgClient
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


ClientHistoryItem = NamedTuple('ClientHistoryItem',
                               [('at', int), ('succeeded', int),
                                ('failed', int)])


class ClientContext(object):
    def __init__(self, client: CgClient, history: List[ClientHistoryItem],
                 opaque=None):
        self.client = client
        self._history = [] if history is None else history
        self.opaque = opaque

    def update(self, prev_resp: CompanionResp, history: List[ClientHistoryItem]):
        self._history = history
        self.opaque = prev_resp.opaque


class CompanionResp(object):
    def __init__(self, action: Union[Fire, Idle, Terminate], opaque=None):
        self.action = action
        self.opaque = opaque


class Companion(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def ask(self, client_ctx: ClientContext=None) -> CompanionResp:
        """

        :param client_ctx:
        :return:
        """


class IdleCompanion(Companion):
    def ask(self, client_ctx: ClientContext = None) -> CompanionResp:
        return CompanionResp(Idle())