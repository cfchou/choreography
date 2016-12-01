# vim:fileencoding=utf-8
import asyncio
from asyncio import BaseEventLoop
from asyncio import Queue, QueueEmpty, QueueFull
import abc
import pprint
from hbmqtt.session import IncomingApplicationMessage
from hbmqtt.client import MQTTClient, ClientException, ConnectException
from choreography.cg_exception import CgException
from choreography.cg_util import lorem_ipsum, get_delay
from choreography.cg_util import StepResponder, StepRespModel
from choreography.cg_client import CgClient
import abc
from choreography.cg_context import CgContext
import attr
from attr import validators

from typing import List, Tuple, Union, NamedTuple
from transitions import Machine
import random
import attr
from autologging import logged


class CgCompanionException(CgException):
    pass

class CpCmd(abc.ABC):
    @abc.abstractmethod
    def __init__(self):
        pass

@attr.s
class CpFire(CpCmd):
    """
    Fire once, only coming back to ask after 't' secs where:
        if 'timeout' is 0:
            duration < t
        otherwise:
            duration < t < max(duration, timeout)

    'duration' is usually 0 as we almost always want CgClient to come back
    immediately after fired.
    """
    duration = attr.ib(default=0)
    timeout = attr.ib(default=0)


class CpSubscribe(CpFire):
    def __init__(self, topics: List[Tuple[str, int]], duration: float=0,
                 timeout: float=0):
        """
        :param topics: list of (topic, qos)
        :param duration:
        """
        super().__init__(duration=duration, timeout=timeout)
        for _, qos in topics:
            if qos < 0 or qos > 2:
                raise CgCompanionException('invalid qos {}'.format(qos))
        self.topics = topics


class CpPublish(CpFire):
    def __init__(self, topic: str, msg: bytes, qos: int=0, retain: bool=False,
                 duration: float=0, timeout: float=0):
        super().__init__(duration=duration, timeout=timeout)
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
    def __init__(self, duration: float=0, timeout: float=0):
        super().__init__(duration=duration, timeout=timeout)


class CpTerminate(CpCmd):
    """
    Won't come back to ask. CgClient is still running(no disconnect).
    """
    def __init__(self):
        pass


@attr.s
class CpIdle(CpCmd):
    duration = attr.ib(default=0)


@attr.s
class CpResp(object):
    prev_cmd = attr.ib(validator=attr.validators.instance_of(CpCmd))

@attr.s
class CpFireResp(CpResp):
    result = attr.ib(default=None)
    exception = attr.ib(default=None)


class Companion(abc.ABC):
    context = attr.ib(validator=validators.instance_of(CgContext))
    config = attr.ib()

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

class CompanionX(abc.ABC):
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


@attr.s
class CompanionFactory(object):
    context = attr.ib(validator=validators.instance_of(CgContext))
    companion_cls = attr.ib()
    companion_conf = attr.ib()
    def new_instance(self):
        return self.companion_cls(context=self.context,
                                  config=self.companion_conf)


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



@logged
class SimpleSub(Companion):
    states = ['created', 'delaying', 'subscribing', 'receiving', 'done']

    """
    SimpleSub, after 'delay' secs, subscribes a number of topics and
    listens for them for 'duration' secs.

    duration == 0 means infinite
    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop = None):
        try:
            super().__init__(namespace, plugin_name, name, config, loop)
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

            # parameters optional
            self.delay = get_delay(config)

            # criteria to terminate
            self.duration = config.get('duration', -1)
            # TODO: terminate if some number of msgs received

            # only in use if duration > 0
            self.auto_disconnect = config.get('auto_disconnect', True)
            self.__log.debug('{} args: delay={}, duration={}, topics={}'.
                             format(self.name, self.delay, self.duration,
                                    self.topics))
            # stateful
            self.machine = Machine(model=self, states=SimpleSub.states,
                                   initial='created')
            self.machine.add_ordered_transitions(['created', 'delaying',
                                                  'subscribing', 'receiving'],
                                                 loop=False)
            self.machine.add_transition(trigger='next_state',
                                        source='receiving',
                                        dest='receiving',
                                        unless='is_done',
                                        after='keep_receiving')
            self.machine.add_transition(trigger='next_state',
                                        source='receiving',
                                        dest='done',
                                        condition='is_done')

            self.cp_cmd = None
            self.duration_start_t = 0

        except CgCompanionException as e:
            raise e
        except Exception as e:
            raise CgCompanionException from e

    def on_enter_delaying(self):
        if self.delay > 0:
            self.cp_cmd = CpIdle(duration=self.delay)
        else:
            self.machine.next()

    def on_enter_subscribing(self):
        self.cp_cmd = CpSubscribe(self.topics)

    def on_exit_subscribing(self):
        self.duration_start_t = self.loop.time()

    def on_enter_receiving(self):
        if self.duration >= 0:
            now = self.loop.time()
            self.cp_cmd = CpIdle(max(
                now - self.duration_start_t - self.duration, 0))
        else:
            self.machine.next_state()

    def on_enter_done(self):
        if self.auto_disconnect:
            self.cp_cmd = CpDisconnect()
        else:
            self.cp_cmd = CpTerminate()

    def is_done(self):
        if self.duration >= 0:
            now = self.loop.time()
            if now - self.duration_start_t - self.duration > 0:
                return True
        if self.received_count >= self.expect >= 0:
            return True
        return False

    async def ask(self, resp: CpResp = None) -> CpCmd:
        self.machine.next_state()
        return self.cp_cmd


from prometheus_client import Counter, Gauge, Summary, Histogram
fly_hist = Histogram('cg_pubsub_fly_hist', 'pubsub fly time')


@logged
class SelfPubSub(StepResponder, Companion):
    """
    SelfPubSub, after 'delay'~'delay_max' secs, subscribes a topic,
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
        try:
            super().__init__(namespace, plugin_name, name, config, loop)
            self.model = StepRespModel(responder=self,
                                       num_steps=self.config.get('num_steps', 1),
                                       step=self.config.get('step', 1),
                                       delay=get_delay(config),
                                       loop=loop)
            self.rate = self.config.get('rate', 1)
            if self.rate < 0:
                raise CgCompanionException('Invalid rate={}'.format(self.rate))
            # use 'client_id' as the default topic
            self.topic = config.get('topic', name)
            self.msg_len = config.get('msg_len', 0)
            self.msg = config.get('msg')    # ignore 'msg_len'
            self.qos = config.get('qos', 0)
            if self.qos < 0 or self.qos > 2:
                raise CgCompanionException('invalid qos {}'.format(self.qos))
            self.retain = config.get('retain', False)
            # only in use when num_steps >= 0
            self.auto_disconnect = config.get('auto_disconnect', True)

            self.subscribed = False

            # hack to skip model transition
            self.offset_countdown = 0
            self.rate_countdown = 0

            self.total = 0
            self.cp_cmd = None
            # short name for logging
            self.sn = 'cg_pub_' + self.name[-16:]

            self.__log.info('{}: offset({}) + rate({}) * num_steps({}); step({})'.
                            format(self.sn, self.model.offset, self.rate,
                                   self.model.num_steps, self.model.step))

        except CgCompanionException as e:
            raise e
        except Exception as e:
            raise CgCompanionException('Invalid configs') from e

    def msg_marked(self):
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

    def run_delay(self):
        self.__log.debug('{} delay for {}'.format(self.sn, self.model.delay))
        self.cp_cmd = CpIdle(duration=self.model.delay)

    def run_offset(self):
        self.__log.debug('{} fire offset {}'.format(self.sn, self.model.offset))
        assert self.model.offset > 0
        self.offset_countdown = self.model.offset - 1
        self.total += 1
        self.cp_cmd = CpPublish(topic=self.topic, msg=self.msg_marked(),
                                qos=self.qos, retain=self.retain)

    def run_step(self):
        self.__log.debug('{} fire at step {}'.
                         format(self.sn, self.rate, self.model.current_step()))
        self.rate_countdown = self.rate - 1
        self.total += 1
        self.cp_cmd = CpPublish(topic=self.topic, msg=self.msg_marked(),
                                qos=self.qos, retain=self.retain)

    def run_idle(self):
        now = self.loop.time()
        diff = max(self.model.step_start_t + self.model.step - now, 0)
        self.__log.debug('{} idle for {}'.format(self.sn, diff))
        self.cp_cmd = CpIdle(duration=diff)

    def run_done(self):
        assert self.model.num_steps >= 0
        self.__log.debug('{} step done {}'.format(self.sn, self.step_count))
        if self.auto_disconnect:
            self.cp_cmd = CpDisconnect()
        else:
            self.cp_cmd = CpTerminate()

    async def ask(self, resp: CpResp = None) -> CpCmd:
        if not self.subscribed:
            self.subscribed = True
            return CpSubscribe([(self.topic, self.qos)])

        # Here skipping model.ask() for a few times(xxx_countdown) to stay
        # in the same transition. This hack is ugly due to the fact that
        # StepModel is not general enough. However, I want to keep it simple.
        if self.offset_countdown > 0:
            self.offset_countdown -= 1
            self.total += 1
            self.cp_cmd = CpPublish(topic=self.topic, msg=self.msg_marked(),
                                    qos=self.qos, retain=self.retain)
        elif self.rate_countdown > 0:
            self.rate_countdown -= 1
            self.total += 1
            self.cp_cmd = CpPublish(topic=self.topic, msg=self.msg_marked(),
                                    qos=self.qos, retain=self.retain)
        else:
            self.model.ask()
        return self.cp_cmd

    async def received(self, msg: IncomingApplicationMessage):
        try:
            # see self.msg_marked()
            # mark = bytes('{:05} {} {}:'....)
            x = msg.publish_packet.data.decode('utf-8')
            i = x.find(' ')
            j = x.find(' ', i+1)
            k = x.find(':', j+1)
            diff = self.loop.time() - float(x[j+1:k])
            self.__log.debug('{} fly: {}'.format(self.sn, diff))
            fly_hist.observe(diff)
        except Exception as e:
            self.__log.exception('{} {}'.format(self.sn, e))
            # swallow


class CompanionRunner(abc.ABC):
    context = attr.ib(validator=validators.instance_of(CgContext))
    @abc.abstractmethod
    async def run(self, companion, client):
        pass


@logged
@attr.s
class CompanionRunnerDefault(CompanionRunner):
    async def __run_fire(self, companion, client, cmd: CpFire):
        """

        :param cmd:
        :return:
        :raises: :class:`asyncio.TimeoutError` if timeout occurs before a message is delivered
                 :class:`CgException` if client disconnected, reconnecting,...
        """
        log = self.__log
        loop = self.context.loop
        async def fire(client, _f: CpFire):
            if isinstance(_f, CpSubscribe):
                return CpFireResp(cmd,
                                  result=await client.subscribe(_f.topics))
            elif isinstance(_f, CpPublish):
                return CpFireResp(cmd,
                                  result=await client.publish(_f.topic, _f.msg,
                                                             _f.qos,
                                                             retain=_f.retain))
            elif isinstance(_f, CpDisconnect):
                return CpFireResp(cmd,
                                  result=await client.disconnect())
            else:
                raise CgException('unsupported command {}'.format(cmd))
        try:
            job = fire(client, cmd)
            jobs = [job]
            if cmd.duration > 0:
                jobs.append(asyncio.sleep(cmd.duration, loop=loop))
            await asyncio.wait(jobs, timeout=max(cmd.duration, cmd.timeout),
                               loop=loop)
            return job.result()
        except asyncio.InvalidStateError as e:
            # exceeds timeout, this exception is passed on to the companion
            return CpFireResp(cmd, exception=e)

    async def __receive(self, companion, client):
        try:
            msg = await client.deliver_message()
            await companion.received(msg)
            self.context.loop.create_task(self.__receive(companion, client))
        except CgException as e:
            await client.disconnect()
            raise e
        except Exception as e:
            await client.disconnect()
            raise CgException from e

    async def __ask(self, companion, client, cmd_resp: CpResp=None):
        log = self.__log
        loop = self.context.loop
        try:
            cmd = await companion.ask(cmd_resp)
            log.debug('{} {}'. format(client.client_id, cmd))
            if isinstance(cmd, CpTerminate):
                return
            elif isinstance(cmd, CpFire):
                new_resp = await self.__run_fire(companion, client, cmd)
                if isinstance(cmd, CpDisconnect):
                    return
                loop.create_task(self, self.__ask(companion, client, new_resp))
            else:
                raise CgException('unsupported command {}'.format(cmd))
        except CgException as e:
            await client.disconnect()
            raise e
        except Exception as e:
            await client.disconnect()
            raise CgException from e

    async def run(self, companion: Companion, client: CgClient):
        loop = self.context.loop
        loop.create_task(self.__receive(companion, client))
        await self.__ask(companion, client)

