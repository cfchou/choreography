
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


class ClientContext(object):
    def __init__(self, name: str, history: List[ClientHistoryItem],
                 opaque=None):
        self.name = name
        self.history = [] if history is None else history
        self.opaque = opaque


class SupervisorResp(object):
    def __init__(self, action: Union[Fire, Idle, Terminate], opaque=None):
        self.action = action
        self.opaque = opaque


class Supervisor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def ask_next(self, client_ctx: ClientContext=None) -> SupervisorResp:
        """

        :param client_ctx:
        :return:
        """



