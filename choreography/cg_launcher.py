# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_exception import CgException
from choreography.cg_client import CgClient
from choreography.cg_companion import CompanionRunner, CompanionFactory
from hbmqtt.client import ClientException
from hbmqtt.mqtt.connack import CONNECTION_ACCEPTED
import functools
import asyncio
import attr
from attr import validators
from autologging import logged

class CgLauncherException(CgException):
    pass

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
class LcResp(object):
    prev_cmd = attr.ib(validator=attr.validators.instance_of(LcCmd))


@attr.s(frozen=True)
class LcFireResp(LcResp):
    result = attr.ib()

@attr.s
class Launcher(abc.ABC):
    context = attr.ib(validator=validators.instance_of(CgContext))
    config = attr.ib()

    @abc.abstractmethod
    async def ask(self, resp: LcResp=None) -> LcCmd:
        """
        :param resp:
        :return:
        """


@attr.s
class LauncherRunner(abc.ABC):
    context = attr.ib(validator=validators.instance_of(CgContext))
    companion_factory = attr.ib(
        validator=validators.instance_of(CompanionFactory))
    companion_runner = attr.ib(
        validator=validators.instance_of(CompanionRunner))

    @abc.abstractmethod
    async def run(self, launcher):
        pass


@logged
class LauncherRunnerDefault(LauncherRunner):
    async def __connect_and_run(self, client: CgClient, **kwargs):
        log = self.__log
        loop = client.get_loop()

        def connect_cb(fu: asyncio.Future):
            try:
                ret = fu.result()
                if ret == CONNECTION_ACCEPTED:
                    companion = self.companion_factory.new_instance()
                    loop.create_task(self.companion_runner.run(companion,
                                                               client))
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
        task.add_done_callback(connect_cb)
        return await asyncio.wait_for(task, loop=loop)

    async def __run_fire(self, launcher, cmd: LcFire):
        log = self.__log
        loop = self.context.loop
        clients = [CgClient(client_id=cid, config=self.context.client_conf,
                            loop=loop) for cid in cmd.cids]
        jobs = [self.__connect_and_run(c) for c in clients]
        if cmd.duration > 0:
            jobs.append(asyncio.sleep(cmd.duration))
        done, _ = await asyncio.wait(jobs, loop=loop)
        succeeded = 0
        for d in done:
            if d.exception() is None and d.result() == CONNECTION_ACCEPTED:
                succeeded += 1
        return succeeded

    async def __run(self, launcher: Launcher, cmd_resp: LcResp=None):
        try:
            log = self.__log
            loop = self.context.loop
            cmd = await self.launcher.ask(cmd_resp)
            log.debug('{} {}'. format(self.launcher.name, cmd))
            if isinstance(cmd, LcTerminate):
                return
            elif isinstance(cmd, LcFire):
                succeeded = await self.__run_fire(launcher, cmd)
                loop.create_task(self.__run(launcher,
                                            LcFireResp(cmd, result=succeeded)))
            else:
                raise CgLauncherException('unsupported command {}'.format(cmd))
        except CgException as e:
            raise e
        except Exception as e:
            raise CgException from e

    async def run(self, launcher):
        await self.__run(launcher)


