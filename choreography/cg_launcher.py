# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_exception import CgLauncherException
from choreography.cg_util import gen_client_id, MonoIncModel
import asyncio
from asyncio import BaseEventLoop
import random
import attr
from autologging import logged


class LcCmd(abc.ABC):
    @abc.abstractmethod
    def __init__(self):
        pass


class LcFire(LcCmd):
    """
    Fire 'rate' clients, only coming back to ask after 't' secs where:
        if 'timeout' is 0:
            duration < t == clients are all done(either connected or failed)
        otherwise:
            duration < t < max(duration, timeout)
    """
    def __init__(self, rate: int, conf_queue: asyncio.Queue, duration: float=0.,
                 timeout: float=0.):
        self.rate = rate
        self.duration = duration
        self.timeout = timeout
        self.conf_queue = conf_queue


class LcTerminate(LcCmd):
    """
    Don't ever come back to ask
    """
    def __init__(self):
        pass


class LcIdle(LcCmd):
    """
    Come back after 'duration' seconds

    duration == 0 means LcTerminate
    """
    def __init__(self, duration: float=1.):
        self.duration = duration


class LcResp(object):
    def __init__(self, prev_cmd: LcCmd, succeeded: int=0, failed: int=0):
        self.prev_cmd = prev_cmd
        self.succeeded = succeeded
        self.failed = failed


class Launcher(abc.ABC):
    @abc.abstractmethod
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop=None):
        self.namespace = namespace
        self.plugin_name = plugin_name
        self.name = name
        self.config = config
        self.loop = asyncio.get_event_loop() if loop is None else loop

    @abc.abstractmethod
    async def ask(self, resp: LcResp=None) -> LcCmd:
        """
        :param resp:
        :return:
        """


@logged
class IdleLauncher(Launcher):
    """
    idle, idle, idle...
    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop = None):
        super().__init__(namespace, plugin_name, name, config, loop)

    async def ask(self, resp: LcResp=None) -> LcCmd:
        self.__log.debug('IdleLauncher resp:{}'.format(resp))
        return LcIdle(duration=self.config.get('duration', 1.))


@logged
class OneInstanceLauncher(Launcher):
    """
    after 'delay' secs, launch one clients with 'client_id' within 'timeout' secs.
    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop = None):
        super().__init__(namespace, plugin_name, name, config, loop)
        # parameters optional
        self.timeout = self.config.get('timeout', 0.)
        self.delay = config.get('delay', 0)
        self.client_id = config.get('client_id')
        if self.client_id is None:
            client_id_prefix = config.get('client_id_prefix')
            if client_id_prefix is None:
                self.client_id = gen_client_id()
            else:
                self.client_id = gen_client_id(prefix=client_id_prefix)
        # stateful
        self.log = self.__log
        self.fu = None
        self.log.debug('{} args: client_id={}, timeout={}, delay={}'.
                       format(self.name, self.client_id, self.timeout,
                              self.delay))

    async def ask(self, resp: LcResp=None) -> LcCmd:
        # TODO: delay as a decorator
        if self.delay > 0:
            self.log.debug('Idle for {}'.format(self.delay))
            i = self.delay
            self.delay = 0
            return LcIdle(duration=i)

        # TODO retry if failed
        if self.fu is not None:
            self.fu.cancel()
            self.log.debug('LcTerminate')
            return LcTerminate()

        queue = asyncio.Queue(loop=self.loop)
        self.fu = self.loop.create_task(queue.put((self.client_id,
                                                   self.config)))
        self.log.debug('LcFire')
        return LcFire(rate=1, conf_queue=queue, duration=0,
                      timeout=self.timeout)


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

@logged
class OneShotLauncher(DelayMixin, Launcher):
    """
    fire, terminate
    after 'delay' secs, launch 'rate' number of clients within 'timeout' secs.

    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop=None):
        super().__init__(namespace, plugin_name, name, config, loop)
        # parameters optional
        self.rate = self.config.get('rate', 1)
        self.timeout = self.config.get('timeout', 0.)
        self.delay = self.get_delay(config)
        self.client_id_prefix = config.get('client_id_prefix')

        # stateful
        self.fu = None

        self.__log.debug('{} args: rate={}, timeout={}, delay={}'.
                         format(self.name, self.rate, self.timeout, self.delay))

    async def ask(self, resp: LcResp=None) -> LcCmd:
        if self.delay > 0:
            self.__log.debug('Idle for {}'.format(self.delay))
            i = self.delay
            self.delay = 0
            return LcIdle(duration=i)

        async def put_conf(q, maxsize):
            for _ in range(0, maxsize):
                if self.client_id_prefix is None:
                    # autogen when client_id is None
                    await q.put((gen_client_id(), self.config))
                else:
                    await q.put((gen_client_id(self.client_id_prefix),
                                 self.config))

        if self.fu is not None:
            self.fu.cancel()
            self.__log.debug('LcTerminate')
            return LcTerminate()

        queue = asyncio.Queue(maxsize=self.rate,
                              loop=self.loop)
        self.fu = self.loop.create_task(put_conf(queue, self.rate))
        self.__log.debug('LcFire')
        return LcFire(rate=self.rate, conf_queue=queue, duration=0,
                      timeout=self.timeout)


@logged
class OneShotLauncher2(DelayMixin, Launcher):
    """
    fire, terminate
    after 'delay' secs, create and connect 'rate' number of clients using
    'step' secs for 'num_steps' times.

    In each step, it may takes more then 'step' seconds. Moreover,
    'auto_reconnect' will affect the time well.
    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop=None):
        super().__init__(namespace, plugin_name, name, config, loop)
        # parameters optional
        self.rate = self.config.get('rate', 1)
        self.step = self.config.get('step', 1)
        self.num_steps = self.config.get('num_steps', 1)
        self.delay = self.get_delay(self.config)
        self.client_id_prefix = self.config.get('client_id_prefix')

        if self.rate < 0 or self.step < 0 or self.num_steps < 0 or \
           self.delay < 0:
            raise CgLauncherException('Invalid configs')

        # stateful
        self.step_count = 0
        self.step_start = 0
        self.fu = None
        self.fire_times = []
        for _ in range(0, self.rate):
            self.fire_times.append(random.uniform(0.0, self.step))
        self.fire_times.sort()

        self.__log.info('{} args: rate={}, step ={}, num_steps={}, delay={}'.
                         format(self.name, self.rate, self.step, self.num_steps,
                                self.delay))

    async def ask(self, resp: LcResp=None) -> LcCmd:
        if self.delay > 0:
            self.__log.info('Delay for {}'.format(self.delay))
            i = self.delay
            self.delay = 0
            return LcIdle(duration=i)

        async def put_conf(q, maxsize):
            for _ in range(0, maxsize):
                if self.client_id_prefix is None:
                    # autogen when client_id is None
                    await q.put((gen_client_id(), self.config))
                else:
                    await q.put((gen_client_id(self.client_id_prefix),
                                 self.config))

        if self.fu is not None:
            self.fu.cancel()
        if self.step_count >= self.num_steps:
            self.__log.info('Terminate {} steps all done'.
                            format(self.num_steps))
            return LcTerminate()

        now = self.loop.time()
        diff = self.step_start + self.step - now
        if diff > 0:
            self.__log.info('Idle for {}'.format(diff))
            return LcIdle(duration=diff)

        queue = asyncio.Queue(maxsize=self.rate, loop=self.loop)
        self.fu = self.loop.create_task(put_conf(queue, self.rate))
        self.step_count += 1
        self.step_start = now
        self.__log.info('LcFire {} at step: {}'.
                        format(self.rate, self.step_count))
        return LcFire(rate=self.rate, conf_queue=queue, duration=0)


class MonoIncLauncher(Launcher):
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


class MonoIncLauncherModel(MonoIncModel):
    def __init__(self, launcher: MonoIncLauncher, rate=1, num_steps=-1, step=1,
                 offset=0, delay=0, loop: BaseEventLoop = None):
        super().__init__(rate, num_steps, step, offset, delay, loop)
        self.launcher = launcher

    def run_step(self):
        self.launcher.run_step()

    def run_offset(self):
        self.launcher.run_offset()

    def run_delay(self):
        self.launcher.run_delay()

    def run_idle(self):
        self.launcher.run_idle()

    def run_done(self):
        self.launcher.run_done()


@logged
class OneShotLauncher3(DelayMixin, MonoIncLauncher):
    """
    fire, terminate
    after 'delay' secs, create and connect 'rate' number of clients using
    'step' secs for 'num_steps' times.

    In each step, it may takes more then 'step' seconds. Moreover,
    'auto_reconnect' will affect the time well.
    """
    def __init__(self, namespace, plugin_name, name, config,
                 loop: BaseEventLoop=None):
        try:
            super().__init__(namespace, plugin_name, name, config, loop)
            self.model = MonoIncLauncherModel(launcher=self,
                                              rate=self.config.get('rate', 1),
                                              num_steps=self.config.get('num_steps', 1),
                                              step=self.config.get('step', 1),
                                              delay=self.get_delay(self.config),
                                              loop=loop)
            self.client_id_prefix = self.config.get('client_id_prefix')
            self.fu = None
            self.__log.info('{} args: rate={}, step ={}, num_steps={}, delay={}'.
                            format(self.name, self.model.rate, self.model.step,
                                   self.model.num_steps, self.model.delay))
        except Exception as e:
            raise CgLauncherException('Invalid configs')


    async def ask(self, resp: LcResp=None) -> LcCmd:
        self.model.ask()

        if self.delay > 0:
            self.__log.info('Delay for {}'.format(self.delay))
            i = self.delay
            self.delay = 0
            return LcIdle(duration=i)

        async def put_conf(q, maxsize):
            for _ in range(0, maxsize):
                if self.client_id_prefix is None:
                    # autogen when client_id is None
                    await q.put((gen_client_id(), self.config))
                else:
                    await q.put((gen_client_id(self.client_id_prefix),
                                 self.config))

        if self.fu is not None:
            self.fu.cancel()
        if self.step_count >= self.num_steps:
            self.__log.info('Terminate {} steps all done'.
                            format(self.num_steps))
            return LcTerminate()

        now = self.loop.time()
        diff = self.step_start + self.step - now
        if diff > 0:
            self.__log.info('Idle for {}'.format(diff))
            return LcIdle(duration=diff)

        queue = asyncio.Queue(maxsize=self.rate, loop=self.loop)
        self.fu = self.loop.create_task(put_conf(queue, self.rate))
        self.step_count += 1
        self.step_start = now
        self.__log.info('LcFire {} at step: {}'.
                        format(self.rate, self.step_count))
        return LcFire(rate=self.rate, conf_queue=queue, duration=0)


