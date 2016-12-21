# vim:fileencoding=utf-8

from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_util import gen_client_id, get_delay, lorem_ipsum
from choreography.plugin.step import StepModel, StepModelResponder
from choreography.cg_companion import Companion, CgCompanionException
from choreography.cg_companion import CpResp, CpFireResp, CpCmd
from choreography.cg_companion import CpIdle, CpTerminate, CpDisconnect
from choreography.cg_companion import CpSubscribe, CpPublish
import asyncio
import attr
from autologging import logged


@logged
class SimpleSub(StepModelResponder, Companion):
    def __init__(self, context: CgContext, config, client_id):
        try:
            super().__init__(context, config, client_id)
            self.delay = get_delay(config),

            topics = config.get('topics')
            topic = config.get('topic')
            qos = config.get('qos', 0)
            if topic is not None:
                if topics is None:
                    topics = [{ 'topic': topic, 'qos': qos }]
                else:
                    topics.append({ 'topic': topic, 'qos': qos })
            for x in topics:
                t, q = x.get('topic'), x.get('qos', 0)
                if topic is None or qos < 0 or qos > 2:
                    raise CgCompanionException('invalid topic, qos: {}, {}'.
                                               format(t, q))
                self.topics.append((t, q))


        except CgCompanionException as e:
            raise e
        except Exception as e:
            raise CgCompanionException('Invalid configs') from e

    async def ask(self, resp: CpResp = None) -> CpCmd:
        pass
