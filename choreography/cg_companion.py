
import abc
from typing import List, Union, NamedTuple
import logging
log = logging.getLogger(__name__)


Fire = NamedTuple('Fire', [('rate', int), ('start', int), ('end', int)])
Idle = NamedTuple('Idle', [])
Terminate = NamedTuple('Terminate', [])


ClientHistoryItem = NamedTuple('ClientHistoryItem',
                               [('at', int), ('succeeded', int),
                                ('failed', int)])

from choreography.choreograph import CgClient


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