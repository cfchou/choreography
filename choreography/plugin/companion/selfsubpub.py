# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_util import gen_client_id, get_delay, lorem_ipsum
from choreography.plugin.step import StepModel, StepModelResponder
from choreography.cg_companion import Companion, CgCompanionException
from choreography.cg_companion import CpResp, CpFireResp, CpCmd
from choreography.cg_companion import CpTerminate, CpDisconnect
from choreography.cg_companion import CpSubscribe, CpPublish
import asyncio
import attr
from autologging import logged

from prometheus_client import Counter, Gauge, Summary, Histogram
fly_hist = Histogram('cg_pubsub_fly_hist', 'pubsub fly time')

@attr.s
class IdleCmd(object):
    duration = attr.ib(0)


@logged
class SelfSubPub(StepModelResponder, Companion):
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
            num_steps = self.config.get('num_steps', 1)
            # add one more step for sub
            num_steps = num_steps + 1 if num_steps >= 0 else num_steps
            self.model = StepModel(responder=self,
                                   num_steps=num_steps,
                                   step=self.config.get('step', 1),
                                   delay=get_delay(config),
                                   loop=context.loop)

            self.rate = self.config.get('rate', 1)
            if self.rate < 0:
                raise CgCompanionException('Invalid rate={}'.format(self.rate))
            self.msg = config.get('msg')

            if self.msg is None:
                self.msg = lorem_ipsum(config.get('msg_len', 0))
            else:
                if not isinstance(self.msg, bytes):
                    raise CgCompanionException('msg should be bytes')

            self.qos = config.get('qos', 0)
            if self.qos < 0 or self.qos > 2:
                raise CgCompanionException('invalid qos {}'.format(self.qos))
            self.retain = config.get('retain', False)
            # only meaningful when num_steps >= 0
            self.auto_disconnect = config.get('auto_disconnect', True)

            self.cp_cmd = None
            # shorter name for logging
            self.sn = 'cg_pub_' + self.client_id[-16:]
            self.__log.info('{}: {}'.format(self.sn, self))

        except CgCompanionException as e:
            raise e
        except Exception as e:
            raise CgCompanionException('Invalid configs') from e

    def __str__(self, *args, **kwargs):
        return 'SelfSubPub(client_id={}, model={}, rate={}, ' \
               'auto_disconnect={}, qos={}, retain={}, len(msg)={})'.\
            format(self.client_id, self.model, self.rate, self.auto_disconnect,
                   self.qos, self.retain, len(self.msg))

    async def ask(self, resp: CpResp = None) -> CpCmd:
        self.model.ask()
        while isinstance(self.lc_cmd, IdleCmd):
            await asyncio.sleep(self.lc_cmd.duration, loop=self.context.loop)
            self.model.ask()
        return self.lc_cmd

    def run_done(self):
        self.__log.debug('{} step done {}'.format(self.sn, self.step_count))
        if self.auto_disconnect:
            self.cp_cmd = CpDisconnect()
        else:
            self.cp_cmd = CpTerminate()

    def run_step(self, nth_step):
        if nth_step == 1:
            self.__log.debug('{} Sub at step 0'.format(self.sn))
            self.cp_cmd = CpSubscribe([(self.client_id, self.qos)])
        else:
            self.__log.debug('{} Pub at step {}'.format(self.sn, nth_step))
            self.cp_cmd = CpPublish(topic=self.client_id, msg=self.msg,
                                    qos=self.qos, retain=self.retain)

    def run_idle(self, nth_step, duration):
        self.__log.debug('{} idle at {} step for {}'.
                         format(self.sn, nth_step, duration))
        self.cp_cmd = IdleCmd(duration=duration)




