# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_exception import CgException, CgLauncherException
from choreography.cg_client import CgClient
from hbmqtt.client import ClientException
from hbmqtt.mqtt.connack import CONNECTION_ACCEPTED
import functools
import asyncio
import attr
from attr import validators
from autologging import logged


class LcCmd(abc.ABC):
    pass


@attr.s(frozen=True)
class LcFire(LcCmd):
    """
    Fire 'len(cids)' clients, only coming back to ask after 't' secs where:
        duration < 't' == clients are all done(either connected or failed)
    """
    cids = attr.ib(default=attr.Factory(list))
    duration = attr.ib(default=0)

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


@attr.s(frozen=True)
class LcFireResp(LcResp):
    result = attr.ib()


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
@attr.s
class LauncherRunnerDefault(LauncherRunner):
    context = attr.ib(validator=validators.instance_of(CgContext))
    launcher = attr.ib(validator=validators.instance_of(Launcher))

    async def __connect_and_run(self, client: CgClient, **kwargs):
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
        return await asyncio.wait_for(task, loop=loop)

    async def __run_fire(self, cmd: LcFire):
        clients = [CgClient(client_id=cid, config=self.context.client_conf,
                            loop=self.context.loop) for cid in cmd.cids]
        jobs = [self.__connect_and_run(c) for c in clients]
        if cmd.duration > 0:
            jobs.append(asyncio.sleep(cmd.duration))
        done, _ = await asyncio.wait(jobs, loop=self.context.loop)
        succeeded = 0
        for d in done:
            if d.exception() is None and d.result() == CONNECTION_ACCEPTED:
                succeeded += 1
        return succeeded

    async def run(self, cmd_resp: LcResp=None):
        try:
            log = self.__log
            cmd = await asyncio.wait_for(self.launcher.ask(cmd_resp),
                                         loop=self.context.loop)
            log.debug('{} {}'. format(self.launcher.name, cmd))
            if isinstance(cmd, LcTerminate):
                return
            elif isinstance(cmd, LcIdle):
                if cmd.duration >= 0:
                    await asyncio.sleep(cmd.duration, loop=self.context.loop)
                self.context.loop.create_task(self.run(LcResp(cmd)))
            elif isinstance(cmd, LcFire):
                succeeded = await self.__run_fire(cmd)
                self.context.loop.create_task(self.run(
                    LcFireResp(cmd, result=succeeded)))
            else:
                raise CgLauncherException('unsupported command {}'.format(cmd))
        except CgException as e:
            raise e
        except Exception as e:
            raise CgException from e


