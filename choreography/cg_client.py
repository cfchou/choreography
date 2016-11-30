# vim:fileencoding=utf-8


from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.errors import HBMQTTException
from hbmqtt.client import mqtt_connected
from choreography.cg_util import gen_client_id
import choreography.cg_util
from choreography.cg_metrics import *
from choreography.cg_exception import *
import asyncio
from asyncio import BaseEventLoop
from autologging import logged

@logged
class CgClient(MQTTClient):
    """

    hbmqtt.client.ClientException derives from BaseException which cannot be
    captured by asyncio therefore event loop could be aborted.
    CgClient replaces ClientException with CgClientException that derives
    Exception.
    """
    def __init__(self, client_id=None, config=None,
                 loop: BaseEventLoop=None):
        #default_config = {
        #    'keep_alive': 60,
        #    'ping_delay': 1,
        #    'default_qos': 0,
        #    'default_retain': False,
        #    'auto_reconnect': False,
        #    'check_hostname': False
        #}
        #default_broker = {
        #    'cleansession': True
        #}
        #if config is not None:
        #    default_config.update(config)
        #    default_broker.update(config.get('broker', {}))
        #default_config['broker'] = default_broker
        if client_id is None:
            client_id = gen_client_id()
        #super().__init__(client_id, default_config, loop)
        super().__init__(client_id, config, loop)

    def get_loop(self):
        return self._loop

    def is_connected(self):
        return self._connected_state.is_set()

    # override MqttClient

    async def connect(self, uri=None, cleansession=None, cafile=None,
                      capath=None, cadata=None):
        try:
            self.__log.debug('connect to {}'.format(uri))
            ret = await super().connect(uri, cleansession, cafile, capath,
                                        cadata)
            connections_total.inc()
            return ret
        except ClientException as e:
            self.__log.exception(e)
            raise CgClientException from e

    @time(connect_hist)
    async def _connect_coro(self):
        await super()._connect_coro()

    async def handle_connection_close(self):
        try:
            ret = await super().handle_connection_close()
            if not self.is_connected():
                # disconnect passively
                connections_total.dec()
            return ret
        except (HBMQTTException, ClientException, Exception) as e:
            raise CgClientException from e

    async def deliver_message(self, timeout=None):
        try:
            msg = await mqtt_connected(super().deliver_message)(timeout)
            data = msg.publish_packet.data
            received_bytes_total.inc(len(data))
            received_total.inc()
            return msg
        except (HBMQTTException, ClientException, Exception) as e:
            raise CgClientException from e

    @time(subscribe_hist)
    async def subscribe(self, topics):
        try:
            ret = await super().subscribe(topics)
            return ret
        except (HBMQTTException, ClientException, Exception) as e:
            self.__log.exception(e)
            raise CgSubException from e

    @time(publish_hist)
    async def publish(self, topic, message, qos=None, retain=None):
        try:
            ret = await super().publish(topic, message, qos, retain)
            published_bytes_total.inc(len(message))
            return ret
        except (HBMQTTException, ClientException, Exception) as e:
            self.__log.exception(e)
            raise CgPubException from e

    async def disconnect(self):
        try:
            ret = await super().disconnect()
            # disconnect actively
            connections_total.dec()
            return ret
        except (HBMQTTException, ClientException, Exception) as e:
            self.__log.exception(e)
            raise CgSubException from e

