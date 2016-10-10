# vim:fileencoding=utf-8
import asyncio
from asyncio import BaseEventLoop
import abc
from hbmqtt.session import IncomingApplicationMessage
from typing import List, Union, NamedTuple
import logging
log = logging.getLogger(__name__)


Idle = NamedTuple('Idle', [('elapse', int)])
Terminate = NamedTuple('Terminate', [])

class CpCmd(abc.ABC):
    @abc.abstractmethod
    def __init__(self):
        pass


class CpFire(CpCmd):
    """
    Fire once, only coming back to ask after 't' secs where:
        duration < t == fire is done(either succeeded or failed)
    """
    @abc.abstractmethod
    def __init__(self, duration: int=0):
        self.duration = duration


class Subscribe(CpFire):
    def __init__(self, topic: str, qos: int=0, duration: int=0):
        super().__init__(duration)
        self.topic = topic
        self.qos = qos


class Publish(CpFire):
    def __init__(self, topic: str, msg: bytes, qos: int=0, duration: int=0):
        super().__init__(duration)
        self.topic = topic
        self.qos = qos
        self.msg = msg


class Disconnect(CpFire):
    """
    Won't come back to ask
    """
    def __init__(self, duration: int=0):
        super().__init__(duration)


class CpTerminate(CpCmd):
    """
    Won't come back to ask
    """
    def __init__(self):
        pass


class CpIdle(CpCmd):
    """
    Come back after 'duration' seconds
    """
    def __init__(self, duration):
        self.duration = duration


class CpResp(object):
    def __init__(self, prev_cmd: CpCmd, succeeded):
        self.prev_cmd = prev_cmd
        self.succeeded = succeeded


class Companion(abc.ABC):
    """
    Each of 'ask' and 'received' would be called sequentially.
    However, implementation be careful about concurrency between them.
    For example,
    async def received(...):
        self.some_state = False

    async def received(...):
        self.some_state = True
        await some_thing
        if not self.some_state:
            print('received is called during await some_thing')
    """
    @abc.abstractmethod
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop=None):
        self.namespace = namespace
        self.plugin_name = plugin_name
        self.name = name
        self.config = config
        self.loop = asyncio.get_event_loop() if loop is None else loop

    @abc.abstractmethod
    async def ask(self, resp: CpResp=None) -> CpCmd:
        """

        It would be call sequentially by a CgClient. However, 'recevied' might
        be called concurrently
        :param resp:
        :return:
        """

    async def received(self, msg: IncomingApplicationMessage):
        """
        :param msg:
        :return:
        """
        return


class Publisher(Companion):
    pass
