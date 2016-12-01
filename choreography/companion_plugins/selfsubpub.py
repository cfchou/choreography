# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_util import gen_client_id, get_delay
from choreography.cg_util import StepRespModel, StepResponder
from choreography.cg_companion import Companion, CgCompanionException
from choreography.cg_companion import CpResp, CpFireResp, CpCmd, CpTerminate
import asyncio
from asyncio import BaseEventLoop
import random
import attr
from autologging import logged

@logged
class SelfSubPub(Companion):
    def __init__(self, context: CgContext, config):
        try:
            log = self.__log
            super().__init__(context, config)
        except CgCompanionException as e:
            raise e
        except Exception as e:
            raise CgCompanionException('Invalid configs') from e

    async def ask(self, resp: CpResp = None) -> CpCmd:
        log = self.__log
        log.debug('SelfSubPub')
        return CpTerminate()
