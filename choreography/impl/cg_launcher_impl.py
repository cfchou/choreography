# vim:fileencoding=utf-8

from choreography.cg_context import CgContext
from choreography.cg_client import CgClient
from choreography.cg_exception import CgException
from choreography.cg_launcher import Launcher, LauncherFactory
from choreography.cg_launcher import LauncherRunner
from choreography.cg_launcher import LcFire, LcResp, LcFireResp, LcTerminate
from hbmqtt.client import ClientException
from hbmqtt.mqtt.connack import CONNECTION_ACCEPTED
from stevedore import DriverManager
import asyncio
from autologging import logged


@logged
class LauncherEntryPointFactory(LauncherFactory):
    '''
    A singleton factory.
    '''
    def __init__(self, context: CgContext):
        super().__init__()
        self.context = context
        self.__log.info('load and create choreography.launcher_plugins:{}'.
                        format(context.launcher_conf['plugin']))
        self.launcher_cls = DriverManager(
            namespace='choreography.launcher_plugins',
            name=context.launcher_conf['plugin'],
            invoke_on_load=False).driver
        self.launcher_singleton = None

    def get_instance(self):
        if self.launcher_singleton is None:
            config = self.context.launcher_conf.get('config', dict())
            self.launcher_singleton = self.launcher_cls(context=self.context,
                                                        config=config)
        return self.launcher_singleton


@logged
class LauncherRunnerDefault(LauncherRunner):
    async def __connect_and_run(self, client: CgClient, **kwargs):
        log = self.__log
        loop = client.get_loop()

        def connect_cb(fu: asyncio.Future):
            try:
                ret = fu.result()
                if ret == CONNECTION_ACCEPTED:
                    companion = self.companion_factory.get_instance()
                    loop.create_task(self.companion_runner.run(companion,
                                                               client))
                    log.debug('{} connected'.format(client.client_id))
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

    async def __run_fire(self, launcher: Launcher, cmd: LcFire):
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
            cmd = await launcher.ask(cmd_resp)
            log.debug('{} {}'. format(type(launcher), cmd))
            if isinstance(cmd, LcTerminate):
                return
            elif isinstance(cmd, LcFire):
                succeeded = await self.__run_fire(launcher, cmd)
                loop.create_task(self.__run(launcher,
                                            LcFireResp(cmd, result=succeeded)))
            else:
                raise CgException('unsupported command {}'.format(cmd))
        except CgException as e:
            raise e
        except Exception as e:
            raise CgException from e

    async def run(self, launcher):
        await self.__run(launcher)


