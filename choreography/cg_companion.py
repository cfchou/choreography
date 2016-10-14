# vim:fileencoding=utf-8
import asyncio
from asyncio import BaseEventLoop
import abc
import pprint
from hbmqtt.session import IncomingApplicationMessage
from choreography.cg_exception import CgCompanionException
from choreography import cg_util
from typing import List, Tuple, Union, NamedTuple
from autologging import logged


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

    duration == 0 means CgClient keeps receiving messages, but won't come back
    to ask.
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


@logged
class LinearPublisher(Companion):
    """
    LinearPublisher, after 'delay' secs, publishes at a steady pace:

    Given that:
        'step' is the number of secs per step
        'rate' is the number of publishes per 'step'
        'num_steps' is the number of 'step's
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
        self.delay = config.get('delay', 0)
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
        self.__log.debug('offset({}) + rate({}) * num_steps({}); step({})'.
                         format(self.offset, self.rate, self.num_steps,
                                self.step))

        # short name for log
        self.sn = self.name[-16:]

        def _debug(s):
            self.__log.debug('{} {}'.format(self.sn, s))

        self._debug = _debug


    def _companion_done(self):
        #self.__log.debug('companion done {} steps'.format(self.step_count))
        self._debug('companion done {} steps'.format(self.step_count))
        if self.disconnect_when_done:
            return CpDisconnect()
        else:
            return CpTerminate()

    def _msg_mark(self):
        self.total += 1
        mark = '{:04} {}:'.format(self.total, self.loop.time())
        return bytes(mark.encode('utf-8')) + self.msg

    async def ask(self, resp: CpResp = None) -> CpCmd:
        if self.delay > 0:
            #self.__log.debug('Idle for {}'.format(self.delay))
            self._debug('Idle for {}'.format(self.delay))
            i = self.delay
            self.delay = 0
            return CpIdle(duration=i)

        # publish all 'offset' number of messages
        while self.offset > 0:
            #self.__log.debug('offset: {}'.format(self.offset))
            self._debug('offset: {}'.format(self.offset))
            self.offset -= 1
            return CpPublish(topic=self.topic, msg=self._msg_mark(),
                             qos=self.qos, retain=self.retain)

        if self.rate <= 0 or self.num_step <= 0 or self.step_count > self.num_steps:
            return self._companion_done()

        now = self.loop.time()
        if self.step_start == 0 or now >= self.step_start + self.step:
            # a step forward
            self.step_count += 1
            self.step_start = now
            self.rate_count = 0
            #self.__log.debug('step {} starts at {}'.format(self.step_count,
            #                                               now))
            self._debug('step {} starts at {}'.format(self.step_count, now))

        # the current step hasn't elapsed: now < self.step_start + self.step

        if self.rate_count >= self.rate:
            # the rate reached
            #self.__log.debug('step {} idle, fired {}'.format(self.step_count,
            #                                                 self.rate_count))
            self._log.debug('step {} idle, fired {}'.format(self.step_count,
                                                            self.rate_count))
            return CpIdle(duration=self.step_start + self.step - now)

        self.rate_count += 1
        #self.__log.debug('step {} ongoing, firing {}'.format(self.step_count,
        #                                                     self.rate_count))
        self._debug('step {} ongoing, firing {}'.format(self.step_count,
                                                        self.rate_count))
        return CpPublish(topic=self.topic, msg=self._msg_mark(),
                         qos=self.qos, retain=self.retain)


@logged
class LinearPublisher2(LinearPublisher):
    async def ask(self, resp: CpResp = None) -> CpCmd:
        if self.delay > 0:
            #self.__log.debug('Idle for {}'.format(self.delay))
            self._debug('Idle for {}'.format(self.delay))
            i = self.delay
            self.delay = 0
            return CpIdle(duration=i)

        # publish all 'offset' number of messages
        while self.offset > 0:
            #self.__log.debug('offset: {}'.format(self.offset))
            self._debug('offset: {}'.format(self.offset))
            self.offset -= 1
            return CpPublish(topic=self.topic, msg=self._msg_mark(),
                             qos=self.qos, retain=self.retain)

        # self.step_count is finished steps
        if self.rate <= 0 or self.step_count >= self.num_steps:
            return self._companion_done()

        now = self.loop.time()
        if self.rate_count >= self.rate:
            # the rate reached
            #self.__log.debug('step {} done, fired {}'.format(self.step_count,
            #                                                 self.rate_count))
            self._debug('step {} done, fired {}'.format(self.step_count,
                                                        self.rate_count))
            this_step = self.step_count
            self.step_count += 1
            self.rate_count = 0

            if self.step_start + self.step > now:
                idle = self.step_start + self.step - now
                #self.__log.debug('step {} idle {}'.format(this_step, idle))
                self._debug('step {} idle {}'.format(this_step, idle))
                return CpIdle(duration=idle)
            else:
                #self.__log.debug('step {} late, takes {} secs'.
                #                 format(this_step, now - self.step_start))
                self._debug('step {} late, takes {} secs'.
                            format(this_step, now - self.step_start))
                if self.step_count >= self.num_steps:
                    return self._companion_done()

        if self.rate_count == 0:
            self.step_start = now
            #self.__log.debug('step {} starts at {}'.format(self.step_count,
            #                                               now))
            self._debug('step {} starts at {}'.format(self.step_count, now))

        self.rate_count += 1
        #self.__log.debug('step {} ongoing, firing {}'.format(self.step_count,
        #                                                     self.rate_count))
        self._debug('step {} ongoing, firing {}'.format(self.step_count,
                                                        self.rate_count))
        return CpPublish(topic=self.topic, msg=self._msg_mark(),
                         qos=self.qos, retain=self.retain)





@logged
class OneShotSubscriber(Companion):
    """
    OneShotSubscriber, after 'delay' secs, subscribes a number of topics and
    listens for them for 'duration' secs.

    duration == 0 means infinite
    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop = None):
        super().__init__(namespace, plugin_name, name, config, loop)
        # parameters required
        try:
            self.topics = []
            topics = config.get('topics')
            if not topics:
                topics = [{
                    'topic': config['topic'],
                    'qos': config.get('qos', 0)
                }]

            for x in topics:
                topic = x.get('topic')
                qos = x.get('qos')
                if not topic or qos < 0 or qos > 2:
                    raise CgCompanionException('invalid topic, qos: {}, {}'.
                                               format(topic, qos))
                self.topics.append((topic, qos))
        except CgCompanionException as e:
            raise e
        except Exception as e:
            raise CgCompanionException from e

        # parameters optional
        self.delay = config.get('delay', 0)
        self.duration = config.get('duration', 0)
        self.__log.debug('{} args: delay={}, duration={}, topics={}'.
                  format(self.name, self.delay, self.duration, self.topics))
        # stateful
        self.subscribed = False
        self.duration_past = False

    async def ask(self, resp: CpResp = None) -> CpCmd:
        if self.delay > 0:
            self.__log.debug('Idle for {}'.format(self.delay))
            i = self.delay
            self.delay = 0
            return CpIdle(duration=i)

        if not self.subscribed:
            self.subscribed = True
            return CpSubscribe(self.topics)

        if self.duration <= 0:
            # CgClient won't come back to ask, but can keep receiving messages
            return CpIdle(duration=0.)

        if self.duration_past:
            return CpTerminate()

        self.duration_past = True
        return CpIdle(duration=self.duration)




