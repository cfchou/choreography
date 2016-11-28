# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_exception import CgException, CgLauncherException
from choreography.cg_client import CgClient
from choreography.cg_util import gen_client_id, get_delay
from choreography.cg_util import StepRespModel, StepResponder
from hbmqtt.client import ClientException
from hbmqtt.mqtt.connack import CONNECTION_ACCEPTED
import functools
import asyncio
from asyncio import BaseEventLoop
import random
import attr
from autologging import logged


class LcCmd(abc.ABC):
    pass


@attr.s(frozen=True)
class LcFire(LcCmd):
    """
    Fire 'len(cids)' clients, only coming back to ask after 't' secs where:
        if 'timeout' is 0:
            duration < t == clients are all done(either connected or failed)
        otherwise:
            duration < t < max(duration, timeout)
    """
    cids = attr.ib(default=attr.Factory(list))
    duration = attr.ib(default=0)
    timeout = attr.ib(default=0)


class LcTerminate(LcCmd):
    """
    Don't ever come back to ask
    """
    pass


@attr.s(frozen=True)
class LcIdle(LcCmd):
    """
    Come back after 'duration' seconds
    """
    duration = attr.ib(default=0)


@attr.s(frozen=True)
class LcResp(object):
    prev_cmd = attr.ib(validator=attr.validators.instance_of(LcCmd))
    succeeded = attr.ib(default=0)
    failed = attr.ib(default=0)


class Launcher(abc.ABC):
    @abc.abstractmethod
    def __init__(self, context: CgContext, config):
        self.context = context
        self.config = config
        self.loop = context

    @abc.abstractmethod
    async def ask(self, resp: LcResp=None) -> LcCmd:
        """
        :param resp:
        :return:
        """


class LauncherRunner(abc.ABC):
    def __init__(self, context: CgContext, launcher: Launcher):
        pass

    @abc.abstractmethod
    async def run(self, cmd_resp: LcResp=None):
        pass


@logged
class LauncherRunnerDefault(LauncherRunner):

    def __init__(self, context: CgContext, launcher: Launcher):
        self.context = context
        self.launcher = launcher

    async def __connect_task(self, client: CgClient, **kwargs):
        log = self.__log
        loop = client.get_loop()

        def connect_cb(fu: asyncio.Future):
            try:
                ret = fu.result()
                if ret == CONNECTION_ACCEPTED:
                    companion = self.context.companion_cls(
                        self.context, **self.context.companion_conf)
                    loop.create_task(client.run(companion=companion))
                else:
                    # will be disconnected from the server side
                    log.error('{} connect failed {}'.format(client.client_id,
                                                            ret))
            except ClientException as e:
                log.exception(e)
            except Exception as e:
                log.exception(e)
                loop.create_task(client.disconnect())

        task = loop.create_task(client.connect(**self.context.broker_conf))
        task.add_done_callback(functools.partial(connect_cb, loop=loop))

    async def run_fire(self, cmd: LcFire):
        clients = [CgClient(client_id=cid, config=self.context.client_conf,
                            loop=self.context.loop) for cid in cmd.cids]
        jobs = [self.__connect_task(c) for c in clients]
        if cmd.duration > 0:
            jobs.append(asyncio.sleep(cmd.duration))
        done, _ = await asyncio.wait(jobs)
        succeeded = 0
        for d in done:
            if d.exception() is None and d.result() == CONNECTION_ACCEPTED:
                succeeded += 1
        return succeeded

    async def run(self, cmd_resp: LcResp=None):
        try:
            log = self.__log
            cmd = await asyncio.wait_for(self.launcher.ask(cmd_resp))
            if isinstance(cmd, LcTerminate):
                log.debug('terminate {}'. format(self.launcher.name))
            elif isinstance(cmd, LcIdle):
                log.debug('idle {}'. format(self.launcher.name))
                if cmd.duration >= 0:
                    await asyncio.sleep(cmd.duration, loop=self.context.loop)
                self.context.loop.create_task(self.run(LcResp(cmd)))
            elif isinstance(cmd, LcFire):
                log.debug('fire {}'. format(self.launcher.name))
                succeeded = await self.run_fire(cmd)
                self.context.loop.create_task(self.run(
                    LcResp(cmd, succeeded=succeeded,
                           failed=len(cmd.cids) - succeeded)))
            else:
                raise CgLauncherException('unsupported command {}'.format(cmd))
        except CgException as e:
            raise e
        except Exception as e:
            raise CgException from e


