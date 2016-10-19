# vim:fileencoding=utf-8
import asyncio
from asyncio import BaseEventLoop
import abc
import pprint
from hbmqtt.session import IncomingApplicationMessage
from choreography.cg_exception import CgCompanionException
from choreography.cg_util import lorem_ipsum
from typing import List, Tuple, Union, NamedTuple
import random
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
    Each of 'ask' and 'received' are called sequentially.
    However, implementation must take care of concurrency, if any, between them.
    For example,
    async def received(...):
        self.some_state = False

    async def received(...):
        self.some_state = True
        await some_thing
        if not self.some_state:
            print('received was called during await some_thing')

    If concurrency is a concern, read 18.5.7. Synchronization primitives.
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

    Other configs:
    'topic' is required.
    'msg_len' is ignored if 'msg' is presented.

    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop = None):
        super().__init__(namespace, plugin_name, name, config, loop)
        # parameters required
        self.topic = config['topic']

        # parameters optional
        self.msg_len = config.get('msg_len', 0)
        self.msg = config.get('msg')    # ignore 'msg_len'
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

        # short name for log
        self.sn = 'cg_pub_' + self.name[-16:]

        def _debug(s):
            self.__log.debug('{} {}'.format(self.sn, s))

        def _info(s):
            self.__log.info('{} {}'.format(self.sn, s))

        def _warn(s):
            self.__log.warn('{} {}'.format(self.sn, s))

        self._debug = _debug
        self._info = _info
        self._warn = _warn

        self._info('offset({}) + rate({}) * num_steps({}); step({})'.
                   format(self.offset, self.rate, self.num_steps, self.step))

    def _companion_done(self):
        self._debug('step done {}, {}'.format(self.step_count, self.sn))
        if self.disconnect_when_done:
            return CpDisconnect()
        else:
            return CpTerminate()

    def msg_marked(self):
        self.total += 1
        mark = bytes('{:05} {} {}:'.
                     format(self.total, self.name, self.loop.time()).
                     encode('utf-8'))
        if self.msg is None:
            if self.msg_len > len(mark):
                return mark + lorem_ipsum(self.msg_len - len(mark))
            else:
                return mark
        else:
            return self.msg

    async def ask(self, resp: CpResp = None) -> CpCmd:
        if self.delay > 0:
            self._info('{}: step done {}, {}'.format(self.sn, self.step_count))
            i = self.delay
            self.delay = 0
            return CpIdle(duration=i)

        # publish all 'offset' number of messages
        while self.offset > 0:
            self._debug('{}: offset {}'.format(self.sn, self.offset))
            self.offset -= 1
            return CpPublish(topic=self.topic, msg=self.msg_marked(),
                             qos=self.qos, retain=self.retain)

        # self.step_count is finished steps
        if self.rate <= 0 or self.step_count >= self.num_steps >= 0:
            return self._companion_done()

        now = self.loop.time()
        if self.rate_count >= self.rate:
            # the rate reached
            self._info('{}: step {} done, fired {}'.
                        format(self.sn, self.step_count, self.rate_count))
            this_step = self.step_count
            self.step_count += 1
            self.rate_count = 0

            if self.step_start + self.step > now:
                idle = self.step_start + self.step - now
                self._info('{}: step {} idle {}'.format(self.sn, this_step,
                                                         idle))
                return CpIdle(duration=idle)
            else:
                self._info('{}: step {} late, takes {} secs'.
                            format(self.sn, this_step, now - self.step_start))
                if self.step_count >= self.num_steps >= 0:
                    return self._companion_done()

        if self.rate_count == 0:
            self.step_start = now
            self._info('{}: step {} starts at {}'.
                        format(self.sn, self.step_count, now))

        self.rate_count += 1
        self._debug('{}: step {} ongoing, firing {}'.
                    format(self.sn, self.step_count, self.rate_count))
        return CpPublish(topic=self.topic, msg=self.msg_marked(),
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


class DelayMixin(object):
    """
    """
    def get_delay(self, config):
        delay = config.get('delay', 0)
        delay_max = config.get('delay_max', delay)
        if delay_max >= delay >= 0:
            return random.uniform(delay, delay_max)
        else:
            return 0


from prometheus_client import Counter, Gauge, Summary, Histogram
fly_hist = Histogram('cg_pubsub_fly_hist', 'pubsub fly time')

@logged
class SelfSubscriber(DelayMixin, Companion):
    """
    SelfSubscriber, after 'delay'~'delay_max' secs, subscribes a topic,
    then acts like a LinearPublisher to publish to the topic.

    Given that:
        'step' is the number of secs per step
        'rate' is the number of publishes per 'step'
        'num_steps' is the number of 'step's
        total = offset + rate * num_steps

    num_steps < 0 means infinite

    Other configs:
    if no 'topic', use 'client_id' as the topic
    'msg_len' is ignored if 'msg' is presented.

    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop = None):
        super().__init__(namespace, plugin_name, name, config, loop)
        # parameters required
        self.topic = config.get('topic', name)

        # parameters optional
        self.msg_len = config.get('msg_len', 0)
        self.msg = config.get('msg')    # ignore 'msg_len'
        self.qos = config.get('qos', 0)
        self.retain = config.get('retain', False)
        self.delay = self.get_delay(config)

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
        self.subscribed = False

        # short name for log
        self.sn = 'cg_pub_' + self.name[-16:]

        def _debug(s):
            self.__log.debug('{} {}'.format(self.sn, s))

        def _info(s):
            self.__log.info('{} {}'.format(self.sn, s))

        def _warn(s):
            self.__log.warn('{} {}'.format(self.sn, s))

        def _exception(s):
            self.__log.warn('{} {}'.format(self.sn, s))

        self._debug = _debug
        self._info = _info
        self._warn = _warn
        self._exception = _exception

        self._info('offset({}) + rate({}) * num_steps({}); step({})'.
                   format(self.offset, self.rate, self.num_steps, self.step))

    def _companion_done(self):
        self._debug('step done {}, {}'.format(self.step_count, self.sn))
        if self.disconnect_when_done:
            return CpDisconnect()
        else:
            return CpTerminate()

    def msg_marked(self):
        self.total += 1
        mark = bytes('{:05} {} {}:'.
                     format(self.total, self.name, self.loop.time()).
                     encode('utf-8'))
        if self.msg is None:
            if self.msg_len > len(mark):
                return mark + lorem_ipsum(self.msg_len - len(mark))
            else:
                return mark
        else:
            return self.msg

    async def ask(self, resp: CpResp = None) -> CpCmd:
        if self.delay > 0:
            self._info('{}: step done {}'.format(self.sn, self.step_count))
            i = self.delay
            self.delay = 0
            return CpIdle(duration=i)

        if not self.subscribed:
            # TODO: check sub successful
            self.subscribed = True
            return CpSubscribe([(self.topic, self.qos)])

        # publish all 'offset' number of messages
        while self.offset > 0:
            self._debug('{}: offset {}'.format(self.sn, self.offset))
            self.offset -= 1
            return CpPublish(topic=self.topic, msg=self.msg_marked(),
                             qos=self.qos, retain=self.retain)

        # self.step_count is finished steps
        if self.rate <= 0 or self.step_count >= self.num_steps >= 0:
            return self._companion_done()

        now = self.loop.time()
        if self.rate_count >= self.rate:
            # the rate reached
            self._info('{}: step {} done, fired {}'.
                       format(self.sn, self.step_count, self.rate_count))
            this_step = self.step_count
            self.step_count += 1
            self.rate_count = 0

            if self.step_start + self.step > now:
                idle = self.step_start + self.step - now
                self._info('{}: step {} idle {}'.format(self.sn, this_step,
                                                        idle))
                return CpIdle(duration=idle)
            else:
                self._info('{}: step {} late, takes {} secs'.
                           format(self.sn, this_step, now - self.step_start))
                if self.step_count >= self.num_steps >= 0:
                    return self._companion_done()

        if self.rate_count == 0:
            self.step_start = now
            self._info('{}: step {} starts at {}'.
                       format(self.sn, self.step_count, now))

        self.rate_count += 1
        self._debug('{}: step {} ongoing, firing {}'.
                    format(self.sn, self.step_count, self.rate_count))
        return CpPublish(topic=self.topic, msg=self.msg_marked(),
                         qos=self.qos, retain=self.retain)

    async def received(self, msg: IncomingApplicationMessage):
        try:
            # see self.msg_marked()
            # mark = bytes('{:05} {} {}:'....)
            x = msg.publish_packet.data.decode('utf-8')
            i = x.find(' ')
            j = x.find(' ', i+1)
            k = x.find(':', j+1)
            diff = self.loop.time() - float(x[j+1:k])
            self._debug('fly: {}'.format(diff))
            fly_hist.observe(diff)
        except Exception as e:
            self._exception(e)
            # swallow



