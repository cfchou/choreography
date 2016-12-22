# vim:fileencoding=utf-8

from choreography.cg_context import CgContext
from choreography.cg_client import CgClient
from choreography.cg_exception import CgException
from choreography.cg_companion import Companion, CompanionRunner
from choreography.cg_companion import CpFire, CpFireResp, CpResp
from choreography.cg_companion import CpSubscribe, CpPublish
from choreography.cg_companion import CpTerminate, CpDisconnect
import asyncio
from autologging import logged
import attr


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



