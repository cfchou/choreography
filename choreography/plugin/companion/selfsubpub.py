# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_util import gen_client_id, get_delay
from choreography.plugin.step import StepRespModel, StepResponder
from choreography.cg_companion import Companion, CgCompanionException
from choreography.cg_companion import CpResp, CpFireResp, CpCmd
from choreography.cg_companion import CpIdle, CpTerminate
import asyncio
from asyncio import BaseEventLoop
import random
import attr
from autologging import logged


@attr.s
class IdleCmd(object):
    duration = attr.ib(0)


@logged
class SelfSubPub(StepResponder, Companion):
    """
    SelfPubSub, after 'delay' secs, subscribes a topic,
    then acts like a LinearPublisher to publish to the topic.

    Given that:
        'step' is the number of secs per step
        'rate' is the number of publishes per 'step'
        'num_steps' is the number of 'step's
        total = offset + rate * num_steps

    num_steps < 0 means infinite

    Other configs:
    if no 'topic', use 'client_id' as the topic
    'msg_len' is ignored if 'msg' is presented.

    """
    def __init__(self, context: CgContext, config, client_id):
        try:
            super().__init__(context, config, client_id)
            self.model = StepRespModel(responder=self,
                                       num_steps=self.config.get('num_steps', 1),
                                       step=self.config.get('step', 1),
                                       delay=get_delay(config),
                                       loop=context.loop)

            self.rate = self.config.get('rate', 1)
            if self.rate < 0:
                raise CgCompanionException('Invalid rate={}'.format(self.rate))
            # if no 'topic', use 'client_id' as the default topic
            self.topic = config.get('topic', client_id)
            self.msg = config.get('msg')
            if self.msg is None:
                self.msg_len = config.get('msg_len', 0)
            else:
                self.msg_len = len(self.msg)
            self.qos = config.get('qos', 0)
            if self.qos < 0 or self.qos > 2:
                raise CgCompanionException('invalid qos {}'.format(self.qos))
            self.retain = config.get('retain', False)
            # only in use when num_steps >= 0
            self.auto_disconnect = config.get('auto_disconnect', True)

            self.subscribed = False
            self.offset_countdown = 0
            self.rate_countdown = 0
            self.total = 0
            self.cp_cmd = None
            # short name for logging
            self.sn = 'cg_pub_' + self.client_id[-16:]

        except CgCompanionException as e:
            raise e
        except Exception as e:
            raise CgCompanionException('Invalid configs') from e

    async def ask(self, resp: CpResp = None) -> CpCmd:
        self.__log.debug('SelfSubPub')
        return CpTerminate()

    def run_delay(self):
        self.__log.debug('{} delay for {}'.format(self.sn, self.model.delay))
        self.cp_cmd = IdleCmd(duration=self.model.delay)

    def run_step(self):
        super().run_step()

    def run_idle(self):
        super().run_idle()

    def run_done(self):
        super().run_done()

    def run_offset(self):
        super().run_offset()


