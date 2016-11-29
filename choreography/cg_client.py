# vim:fileencoding=utf-8


from hbmqtt.client import MQTTClient, ClientException, ConnectException
from hbmqtt.errors import HBMQTTException
from hbmqtt.client import mqtt_connected
from hbmqtt.session import IncomingApplicationMessage
from choreography.cg_companion import Companion, CpResp
from choreography.cg_companion import CpFire, CpIdle, CpPublish, CpSubscribe, CpDisconnect, CpTerminate
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
    captured by asyncio so that the event loop will be aborted. Leave us no
    chance to examine a Future's result.
    CgClient replaces ClientException with CgException that derives
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

        # since wildcards in subscribe/unsubscribe, we can only try our best to
        # avoid unnecessary __do_receive.
        self.companion = None
        self.recv = None

    def get_loop(self):
        return self._loop

    def is_connected(self):
        return self._connected_state.is_set()

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
            raise CgConnectException from e

    @time(connect_hist)
    async def _connect_coro(self):
        await super()._connect_coro()

    async def handle_connection_close(self):
        ret = await super().handle_connection_close()
        if not self.is_connected():
            # disconnect passively
            connections_total.dec()
        return ret

    async def deliver_message(self, timeout=None):
        msg = await super().deliver_message(timeout)
        data = msg.publish_packet.data
        received_bytes_total.inc(len(data))
        received_total.inc()
        return msg

    async def __do_receive(self):
        self.__log.debug('constantly receiving msg: {}'.format(self.client_id))
        count = 0
        try:
            while True:
                try:
                    # copy from @hbmqtt.client.mqtt_connected
                    if not self._connected_state.is_set():
                        self.__log.warning("{} not connected, waiting for it".
                                           format(self.client_id))
                        await self._connected_state.wait()
                    msg = await self.deliver_message()
                    data = msg.publish_packet.data
                    data_len = len(data)
                    data_uc = data.decode('utf-8')
                    count += 1
                    self.__log.info('{} receives {} bytes at {}'.
                                    format(self.client_id, data_len,
                                           self._loop.time()))
                    prefix = data_uc[:min(48, data_len)]
                    self.__log.info('msg_{} = {}'.format(count, prefix))
                    await self.companion.received(msg)
                except asyncio.CancelledError as e:
                    self.__log.exception(e)
                    self.__log.info('{} cancelled'.format(self.client_id))
                    break
                except ConnectException as e:
                    # assert issubclass(ConnectException, ClientException)
                    self.__log.exception(e)
                    raise CgConnectException from e
                except (HBMQTTException, ClientException, Exception) as e:
                    # assert issubclass(ClientException, BaseException)
                    self.__log.exception(e)
                    # retry
        finally:
            self.__log.warn(' {} leaves')

    @time(subscribe_hist)
    async def subscribe(self, topics):
        try:
            ret = await super().subscribe(topics)
            self.recv = self._loop.create_task(self.__do_receive())
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

    async def __do_fire(self, fire: CpFire):
        async def __fire(_f):
            if isinstance(_f, CpSubscribe):
                await self.subscribe(_f.topics)
                self.__log.debug('{} subscribed'.format(self.client_id))
                return True
            elif isinstance(_f, CpPublish):
                await self.publish(_f.topic, _f.msg, _f.qos,
                                   retain=_f.retain)
                self.__log.debug('{} published'.format(self.client_id))
                return True
            elif isinstance(_f, CpDisconnect):
                await self.disconnect()
                self.__log.debug('{} disconnected'.format(self.client_id))
                return True
            else:
                self.__log.error('{} unknown {}'.format(self.client_id, _f))
                return False

        tasks = [__fire(fire)]
        if fire.duration > 0:
            tasks.append(asyncio.sleep(fire.duration))
        done, _ = await asyncio.wait(tasks)
        for d in done:
            result = cg_util.future_successful_result(d)
            if result is True:
                return True
        return False

    async def run(self, companion: Companion):
        @cg_util.convert_coro_exception(Exception, CgCompanionException)
        async def _asking(coro):
            return await coro
        self.__log.debug('running: {} with {}'.format(self.client_id, companion))
        self.companion = companion
        idle_forever = False
        try:
            cmd_prev = None
            cmd_resp = None
            while True:
                self.__log.debug("ask companion: {}".format(self.client_id))
                # TODO: wait receive(deliver_message) in the same loop here
                done, _ = await asyncio.wait([_asking(companion.ask(cmd_resp))])
                cmd = cg_util.future_successful_result(done.pop())
                if cmd is None:
                    self.__log.warn('can\'t retrieve cmd')
                    # ask again with cmd_prev
                    cmd_resp = CpResp(cmd_prev, succeeded=False)
                    continue
                if isinstance(cmd, cg_companion.CpTerminate):
                    self.__log.debug('terminate {}'.format(self.client_id))
                    break
                if isinstance(cmd, cg_companion.CpIdle):
                    self.__log.debug('idle {}'.format(self.client_id))
                    if cmd.duration >= 0:
                        await asyncio.sleep(cmd.duration, loop=self._loop)
                        cmd_resp = CpResp(cmd, succeeded=True)
                        cmd_prev = cmd
                        continue
                    else:
                        idle_forever = True
                        break
                result = await self.__do_fire(cmd)
                if isinstance(cmd, cg_companion.CpDisconnect):
                    self.__log.debug('disconnect & terminate {}'.
                                     format(self.client_id))
                    break
                cmd_resp = CpResp(cmd, succeeded=result)
                cmd_prev = cmd
        finally:
            # if idle_forever then CgClient keep receiving messages
            if not idle_forever and self.recv:
                self.__log.debug('neither companion.ask nor receive')
                self.recv.cancel()
            elif self.recv:
                self.__log.debug('won\'t companion.ask but keep receiving')

