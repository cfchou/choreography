# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_exception import CgException, CgLauncherException
from choreography.cg_util import gen_client_id, get_delay
from choreography.cg_util import StepRespModel, StepResponder
from choreography.cg_launcher import Launcher, LcResp
from choreography.cg_launcher import LcCmd, LcFire, LcIdle, LcTerminate
import asyncio
from asyncio import BaseEventLoop
import random
import attr
from autologging import logged

@logged
class MonoIncLauncher(StepResponder, Launcher):
    def __init__(self, context: CgContext, config):
        try:
            super().__init__(context, config)
            self.model = StepRespModel(responder=self,
                                       num_steps=self.config.get('num_steps', 1),
                                       step=self.config.get('step', 1),
                                       delay=get_delay(config),
                                       loop=context.loop)

            self.rate = self.config.get('rate', 1)
            if self.rate < 0:
                raise CgLauncherException('Invalid rate={}'.format(self.rate))
            self.client_id_prefix = self.config.get('client_id_prefix')
            self.lc_cmd = None
            self.__log.info('{} args: rate={}, step ={}, num_steps={}, delay={}'.
                            format(self.name, self.rate, self.model.step,
                                   self.model.num_steps, self.model.delay))
        except CgLauncherException as e:
            raise e
        except Exception as e:
            raise CgLauncherException('Invalid configs') from e

    # StepResponder implementation
    def run_delay(self):
        self.__log.debug('delay for {}'.format(self.model.delay))
        self.lc_cmd = LcIdle(duration=self.model.delay)

    def run_offset(self):
        self.__log.debug('fire offset {}'.format(self.model.offset))
        self.lc_cmd = LcFire(
            cids=(gen_client_id() for _ in range(0, self.model.offset)))

    def run_step(self):
        self.__log.debug('fire {} at step {}'.format(self.rate,
                                                     self.model.current_step()))
        self.lc_cmd = LcFire(
            cids=(gen_client_id() for _ in range(0, self.rate)))

    def run_idle(self):
        now = self.loop.time()
        diff = max(self.model.step_start_t + self.model.step - now, 0)
        self.__log.debug('idle for {}'.format(diff))
        self.lc_cmd = LcIdle(duration=diff)

    def run_done(self):
        self.__log.debug('terminate')
        self.lc_cmd = LcTerminate()

    # Launcher implementation
    async def ask(self, resp: LcResp=None) -> LcCmd:
        self.model.ask()
        return self.lc_cmd


