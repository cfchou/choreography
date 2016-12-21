# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_context import CgContext
from choreography.cg_util import gen_client_id, get_delay
from choreography.plugin.step import StepModel, StepModelResponder
from choreography.cg_launcher import Launcher, LcResp, CgLauncherException
from choreography.cg_launcher import LcCmd, LcFire, LcTerminate
import asyncio
from asyncio import BaseEventLoop
import random
import attr
from autologging import logged

@attr.s
class IdleCmd(object):
    duration = attr.ib(0)


@logged
class MonoIncLauncher(StepModelResponder, Launcher):
    def __init__(self, context: CgContext, config):
        try:
            super().__init__(context, config)
            self.model = StepModel(responder=self,
                                   num_steps=self.config.get('num_steps', 1),
                                   step=self.config.get('step', 1),
                                   delay=get_delay(config),
                                   loop=context.loop)

            self.rate = self.config.get('rate', 1)
            if self.rate < 0:
                raise CgLauncherException('Invalid rate={}'. format(self.rate))

            self.client_id_prefix = self.config.get('client_id_prefix')
            self.lc_cmd = None
            self.__log.info(self)
        except CgLauncherException as e:
            raise e
        except Exception as e:
            raise CgLauncherException('Invalid configs') from e

    def __str__(self, *args, **kwargs):
        return 'MonoIncLauncher(model={}, rate={}, client_id_prefix={})'.\
            fomat(self.model, self.rate, self.client_id_prefix)

    async def ask(self, resp: LcResp=None) -> LcCmd:
        self.model.ask()
        while isinstance(self.lc_cmd, IdleCmd):
            await asyncio.sleep(self.lc_cmd.duration, loop=self.context.loop)
            self.model.ask()
        return self.lc_cmd

    def run_done(self):
        self.__log.debug('terminate')
        self.lc_cmd = LcTerminate()

    def run_step(self, nth_step):
        self.__log.debug('fire {} at step {}'.format(self.rate, nth_step))
        self.lc_cmd = LcFire(
            cids=(gen_client_id() for _ in range(0, self.rate)))

    def run_idle(self, nth_step, duration):
        self.__log.debug('idle at {} step for {}'.format(nth_step, duration))
        self.lc_cmd = IdleCmd(duration=duration)



