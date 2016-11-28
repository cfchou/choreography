# vim:fileencoding=utf-8
import abc
import functools
import copy
import pprint
from typing import List, Tuple, Dict
import yaml
import random
import uuid
import asyncio
from asyncio import BaseEventLoop
from stevedore import NamedExtensionManager, ExtensionManager
from stevedore import DriverManager
from choreography.cg_exception import CgException
from choreography.cg_exception import CgModelException, CgConfigException
import collections
import socket
from consul.aio import Consul
from consul import Check
from autologging import logged
from transitions import Machine
import attr
from attr import validators


def deep_get(nesting, default, *keys):
    try:
        return functools.reduce(lambda d, k: d[k], keys, nesting)
    except (KeyError, TypeError) as e:
        return default


def ideep_get(nesting, *keys):
    return deep_get(nesting, None, *keys)


@logged
def future_successful_result(fu: asyncio.Future):
    """
    Return
    :param fu:
    :return:
    """
    if fu.cancelled():
        future_successful_result._log.info('future cancelled: {}'.format(fu))
        return None
    if fu.done() and fu.exception() is not None:
        future_successful_result._log.exception(fu.exception())
        return None
    return fu.result()


@logged
def convert_coro_exception(from_excp, to_excp):
    if from_excp is None:
        raise CgException('invalid from_excp {}'.format(from_excp))
    if to_excp is None or not issubclass(from_excp, BaseException):
        raise CgException('invalid to_excp {}'.format(to_excp))

    def decorate(coro):
        @functools.wraps(coro)
        async def converted(*args, **kwargs):
            try:
                return await coro(*args, **kwargs)
            except from_excp as e:
                convert_coro_exception._log.exception(e)
                raise to_excp from e
        return converted
    return decorate


@logged
def convert_exception(from_excp, to_excp):
    if from_excp is None:
        raise CgException('invalid from_excp {}'.format(from_excp))
    if to_excp is None or not issubclass(from_excp, BaseException):
        raise CgException('invalid to_excp {}'.format(to_excp))

    def decorate(func):
        @functools.wraps(func)
        def converted(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except from_excp as e:
                convert_exception._log.exception(e)
                raise to_excp from e
        return converted
    return decorate



@logged
def load_yaml(runner_yaml: str) -> dict:
    with open(runner_yaml) as fh:
        load_yaml._log.info('load_yaml {}'.format(runner_yaml))
        return yaml.load(fh)


def update(target, src):
    """
    'target' is recursively updated by 'src'
    :param target:
    :param src:
    :return:
    """
    for k, v in src.items():
        if isinstance(v, collections.Mapping):
            tv = target.get(k, {})
            if isinstance(tv, collections.Mapping):
                update(tv, v)
                target[k] = tv
            else:
                target[k] = v
        else:
            target[k] = v


def gen_client_id(prefix='cg_cli_'):
    """
    Generates random client ID
    Note that: '/', '#', '+' will not be chosen. Which makes the result a valid
    topic name.
    :return:
    """
    gen_id = prefix
    for i in range(7, 23):
        gen_id += chr(random.randint(0, 74) + 48)
    return gen_id


def get_delay(config):
    delay = config.get('delay', 0)
    delay_max = config.get('delay_max', delay)
    if delay_max >= delay >= 0:
        return random.uniform(delay, delay_max)
    else:
        raise CgConfigException('delay={}, delay_max={}'.
                                format(delay, delay_max))


def lorem_ipsum(length: int=0) -> bytes:
    # TODO: as it'll be called a hell a lot of time, there's room to improve.
    lorem = b"""Lorem ipsum dolor sit amet, consectetur adipiscing \
elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut \
enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut \
aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in \
voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint \
occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit \
anim id est laborum. """
    ll = len(lorem)
    if ll < length:
        q = length // ll
        m = length % ll
        return b''.join([lorem for _ in range(0, q)]) + lorem[:m]
    else:
        return lorem[:length]


def get_outbound_addr(addr: str, port: int) -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((addr, port))
    outbound = s.getsockname()[0]
    s.close()
    return outbound


class SdConsul(object):
    """
    Almost a copy-paste from prometheus_async.aio.sd.ConsulAgent.
    With a tweak that allows to set up host/port/scheme.
    """
    def __init__(self,
                 host='127.0.0.1',
                 port=8500,
                 scheme='http',
                 name="app-metrics", service_id=None, tags=(),
                 token=None, deregister=True):
        self.host = host
        self.port = port
        self.scheme = scheme
        self.name = name
        self.service_id = service_id or name
        self.tags = tags
        self.token = token
        self.deregister = deregister

    @asyncio.coroutine
    def register(self, metrics_server, loop):
        """
        :return: A coroutine callable to deregister or ``None``.
        """
        consul = Consul(host=self.host, port=self.port, scheme=self.scheme,
                        token=self.token, loop=loop)

        if not (yield from consul.agent.service.register(
                name=self.name,
                service_id=self.service_id,
                address=metrics_server.socket.addr,
                port=metrics_server.socket.port,
                tags=self.tags,
                check=Check.http(
                    metrics_server.url, "10s",
                )
        )):  # pragma: nocover
            return None

        @asyncio.coroutine
        def deregister():
            try:
                if self.deregister is True:
                    yield from consul.agent.service.deregister(self.service_id)
            finally:
                consul.close()

        return deregister


@logged
class StepModel(object):
    """
    A state machine that presents a monotonically increasing model.
    total = rate * num_steps + offset
    """
    states = ['created', 'delaying', 'offsetting', 'stepping', 'idling',
              'done']

    def __init__(self, num_steps=-1, step=1, offset=0, delay=0,
                 loop: BaseEventLoop=None):
        if step < 0 or delay < 0 or offset < 0:
            raise CgModelException('Invalid configs')

        # num_steps < 0 means infinite
        self.num_steps = int(num_steps)
        self.step = int(step)
        self.offset = int(offset)
        self.delay = delay
        self.loop = asyncio.get_event_loop() if loop is None else loop

        # internal
        self.__steps_remained = self.num_steps
        self.delay_start_t = 0
        self.step_start_t = 0

        self.machine = Machine(model=self, states=StepModel.states,
                               initial='created')

        # =====================
        # created -> delaying
        self.machine.add_transition(trigger='ask', source='created',
                                    dest='delaying',
                                    conditions=['has_delay'],
                                    after='run_delay')
        # created -> offsetting
        self.machine.add_transition(trigger='ask', source='created',
                                    dest='offsetting',
                                    conditions=['has_offset'],
                                    unless=['has_delay'],
                                    after='run_offset')
        # created -> stepping
        self.machine.add_transition(trigger='ask', source='created',
                                    dest='stepping',
                                    conditions=['has_steps'],
                                    unless=['has_offset', 'has_delay'],
                                    after='run_step')
        # created -> done
        self.machine.add_transition(trigger='ask', source='created',
                                    dest='done',
                                    unless=['has_offset', 'has_delay',
                                            'has_steps'],
                                    after='run_done')

        # =====================
        # delaying -> offsetting
        self.machine.add_transition(trigger='ask', source='delaying',
                                    dest='offsetting',
                                    conditions=['is_delay_elapsed',
                                                'has_offset'],
                                    after='run_offset')

        # delaying -> stepping
        self.machine.add_transition(trigger='ask', source='delaying',
                                    dest='stepping',
                                    conditions=['is_delay_elapsed',
                                                'has_steps'],
                                    unless=['has_offset'],
                                    after='run_step')
        # delaying -> done
        self.machine.add_transition(trigger='ask', source='delaying',
                                    dest='done',
                                    conditions=['is_delay_elapsed'],
                                    unless=['has_offset', 'has_steps'],
                                    after='run_done')
        # =====================
        # offsetting -> stepping
        self.machine.add_transition(trigger='ask', source='offsetting',
                                    dest='stepping',
                                    conditions=['has_steps'],
                                    after='run_step')
        # offsetting -> done
        self.machine.add_transition(trigger='ask', source='offsetting',
                                    dest='done',
                                    unless=['has_steps'],
                                    after='run_done')
        # =====================
        # stepping -> idling
        self.machine.add_transition(trigger='ask', source='stepping',
                                    dest='idling',
                                    conditions=['is_step_finished_early'],
                                    after='run_idle')
        # stepping -> stepping (explicitly transition to itself)
        self.machine.add_transition(trigger='ask', source='stepping',
                                    dest='stepping',
                                    conditions=['has_steps'],
                                    unless=['is_step_finished_early'],
                                    after='run_step')
        # stepping -> done
        self.machine.add_transition(trigger='ask', source='stepping',
                                    dest='done',
                                    unless=['is_step_finished_early',
                                            'has_steps'],
                                    after='run_done')
        # =====================
        # idling -> stepping
        self.machine.add_transition(trigger='ask', source='idling',
                                    dest='stepping',
                                    conditions=['has_steps'],
                                    unless=['is_step_finished_early'],
                                    after='run_step')
        # idling -> done
        self.machine.add_transition(trigger='ask', source='idling',
                                    dest='done',
                                    unless=['has_steps',
                                            'is_step_finished_early'],
                                    after='run_done')
        # =====================
        # done -> done (explicitly create a trivial transition)
        self.machine.add_transition(trigger='ask', source='done',
                                    dest='done',
                                    after='run_done')

    def on_enter_delaying(self):
        self.delay_start_t = self.loop.time()

    def on_enter_stepping(self):
        self.step_start_t = self.loop.time()
        self.__steps_remained -= 1

    def is_delay_elapsed(self):
        now = self.loop.time()
        return now >= self.delay_start_t + self.delay

    def is_step_finished_early(self):
        now = self.loop.time()
        return self.step_start_t + self.step > now

    def has_delay(self):
        return self.delay > 0

    def has_offset(self):
        return self.offset > 0

    def has_steps(self):
        return self.__steps_remained != 0

    def current_step(self):
        return self.num_steps - self.__steps_remained

    # ===== overrides ======
    def run_delay(self):
        pass

    def run_offset(self):
        pass

    def run_step(self):
        pass

    def run_idle(self):
        pass

    def run_done(self):
        pass



class StepResponder(abc.ABC):
    """
    Implement this interface to react whenever a transition happens.
    """
    @abc.abstractmethod
    def run_step(self):
        """
        :return:
        """

    @abc.abstractmethod
    def run_offset(self):
        """
        :return:
        """

    @abc.abstractmethod
    def run_delay(self):
        """
        :return:
        """

    @abc.abstractmethod
    def run_idle(self):
        """
        :return:
        """

    @abc.abstractmethod
    def run_done(self):
        """
        :return:
        """


class StepRespModel(StepModel):
    """
    Execute implementation of StepResponder triggered by StepModel
    """
    def __init__(self, responder: StepResponder, num_steps=-1, step=1,
                 offset=0, delay=0, loop: BaseEventLoop=None):
        super().__init__(num_steps, step, offset, delay, loop)
        self.responder = responder

    def run_delay(self):
        self.responder.run_delay()

    def run_offset(self):
        self.responder.run_offset()

    def run_step(self):
        self.responder.run_step()

    def run_idle(self):
        self.responder.run_idle()

    def run_done(self):
        self.responder.run_done()


