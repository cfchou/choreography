# vim:fileencoding=utf-8
import asyncio
from asyncio import BaseEventLoop
import abc
from hbmqtt.session import IncomingApplicationMessage
from choreography.cg_exception import CgCompanionException
from typing import List, Tuple, Union, NamedTuple
import logging
log = logging.getLogger(__name__)


class CpCmd(abc.ABC):
    @abc.abstractmethod
    def __init__(self):
        pass


class CpFire(CpCmd):
    """
    Fire once, only coming back to ask after 't' secs where:
        duration < t == fire is done(either succeeded or failed)

    'duration' is usually 0 as we almost always want CgClient to come back
    immediately after fired.
    """
    @abc.abstractmethod
    def __init__(self, duration: int=0.):
        self.duration = duration


class CpSubscribe(CpFire):
    def __init__(self, topics: List[Tuple[str, int]], duration: float=0.):
        """

        :param topics: list of (topic, qos)
        :param duration:
        """
        super().__init__(duration)
        for _, qos in topics:
            if qos < 0 or qos > 2:
                raise CgCompanionException('invalid qos {}'.format(qos))
        self.topics = topics


class CpPublish(CpFire):
    """

    """
    def __init__(self, topic: str, msg: bytes, qos: int=0, retain: bool=False,
                 duration: float=0.):
        super().__init__(duration)
        if qos < 0 or qos > 2:
            raise CgCompanionException('invalid qos {}'.format(qos))
        self.topic = topic
        self.msg = msg
        self.qos = qos
        self.retain = retain


class CpDisconnect(CpFire):
    """
    Won't come back to ask. CgClient will Disconnect.
    """
    def __init__(self, duration: float=0.0):
        super().__init__(duration)


class CpTerminate(CpCmd):
    """
    Won't come back to ask. CgClient is still running(no disconnect).
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
    However, implementation must take care of concurrency, if any, between them.
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


class LinearPublisher(Companion):
    """
    LinearPublisher publishes at a steady pace:

    Given that:
        'num_steps' is the number of 'step's
        'rate' is the number of publishes per 'step'
    total = offset + rate * num_steps

    num_steps < 0 means infinite
    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop = None):
        super().__init__(namespace, plugin_name, name, config, loop)
        # parameters required
        self.topic = config['topic']
        self.msg = config['msg']
        # parameters optional
        self.qos = config.get('qos', 0)
        self.retain = config.get('retain', False)
        self.offset = config.get('offset', 0)
        self.rate = config.get('rate', 1)
        self.step = config.get('step', 1)
        self.num_steps = config.get('num_steps', 1)
        self.disconnect_when_done = config.get('disconnect_when_done', True)

        if self.qos < 0 or self.qos > 2:
            raise CgCompanionException('invalid qos {}'.format(self.qos))

        # stateful
        self.step_start = 0
        self.step_count = 0
        self.rate_count = 0
        self.total = 0

    def _msg_deco(self):
        self.total += 1
        return bytes((str(self.total) + ': ').encode('utf-8')) + self.msg

    async def ask(self, resp: CpResp = None) -> CpCmd:
        # publish 'offset' number of messages first
        while self.offset > 0:
            log.debug('offset: {}'.format(self.offset))
            self.offset -= 1
            return CpPublish(topic=self.topic, msg=self._msg_deco(),
                             qos=self.qos, retain=self.retain)

        if self.rate <= 0 or self.step_count >= self.num_steps >= 0:
            log.debug('companion done')
            if self.disconnect_when_done:
                return CpDisconnect()
            else:
                return CpTerminate()

        now = self.loop.time()
        if self.step_start == 0 or now >= self.step_start + self.step:
            self.step_count += 1
            self.step_start = now
            self.rate_count = 1
            log.debug('step {} starts at {}'.format(self.step_count, now))
            return CpPublish(topic=self.topic, msg=self._msg_deco(),
                             qos=self.qos, retain=self.retain)

        # now < self.step_start + self.step:
        if self.rate_count > self.rate:
            # step hasn't elapsed but rate has reached
            return CpIdle(duration=self.step_start + self.step - now)
        self.rate_count += 1
        return CpPublish(topic=self.topic, msg=self._msg_deco(),
                         qos=self.qos, retain=self.retain)



