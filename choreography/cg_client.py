# vim:fileencoding=utf-8


from hbmqtt.client import MQTTClient, ClientException, ConnectException
from hbmqtt.errors import HBMQTTException
from hbmqtt.client import mqtt_connected
from hbmqtt.session import Session
from hbmqtt.adapters import StreamReaderAdapter, StreamWriterAdapter, WebSocketsReader, WebSocketsWriter
from hbmqtt.mqtt.protocol.client_handler import ClientProtocolHandler
from hbmqtt.mqtt.connack import CONNECTION_ACCEPTED
from hbmqtt.mqtt.constants import *
from urllib.parse import urlparse, urlunparse
import ssl
import websockets
from websockets.uri import InvalidURI
from websockets.handshake import InvalidHandshake
from choreography.cg_util import gen_client_id
import choreography.cg_util
from choreography.cg_metrics import *
from choreography.cg_exception import *
from collections import deque
import functools
import asyncio
import enum
from asyncio import BaseEventLoop, Event
from autologging import logged
import collections

class _RetryCounter(object):
    def __init__(self, max_retries=0, max_retry_interval=0):
        if max_retries < 0 or max_retry_interval < 0:
            raise ValueError('invalid parameters')
        self._max_retries = max_retries
        self._max_retry_interval = max_retry_interval
        self._retried = 0
        self._event = asyncio.Event()

    @property
    def max_retries(self):
        return self._max_retries

    @property
    def max_retry_interval(self):
        return self._max_retry_interval

    @property
    def retried(self):
        return self._retried

    async def wait_retry_ceased(self):
        return await self._event

    def retry_ceased(self):
        return self._event.is_set()

    def reset(self):
        self._event.clear()
        self._retried = 0

    def auto_reconnect_enabled(self):
        return self._max_retries > 0

    def retried_inc(self):
        """
        Increase retried count if it can.
        Notify if it's already max_retries.
        :return:
        """
        if self._retried < self._max_retries:
            self._retried += 1
            return self._retried
        assert not self._event.is_set()
        self._event.set()
        return -1

    def next_retry_interval(self):
        assert self.auto_reconnect_enabled()
        return min(2 ** self._retried, self._max_retry_interval)

class _CgClientState(enum.Enum):
    # Never connect() successfully, therefore session remains None.
    NEVER_CONNECTED = 1
    # A connection established, but it might be undergoing re-connections.
    # Nevertheless, session is shared across all re-connections.
    ONGOING = 2
    # A series of re-connections have failed. The user can initiate another
    # connect(), which if success will set the session anew.
    RETRY_CEASED = 3

@logged
class CgClient(object):
    """
        MQTT client implementation based on :class:`hbmqtt.MQTTClient`.

    """
    _defaults = {
        # real keep alive = max(keep_alive - ping_delay,
        #                       _defaults.keep_alive_minimum)
        'keep_alive': 10,
        'ping_delay': 1,
        'keep_alive_minimum': 1, # always takes precedence over self.config

        'default_qos': 0,
        'default_retain': False,
        'auto_reconnect': True,
        'reconnect_max_interval': 10,
        'reconnect_retries': 2,
    }

    def __init__(self, client_id=None, config=None,
                 loop: BaseEventLoop=None):
        self.config = CgClient._defaults.copy()
        if config is not None:
            self.config.update(config)
        if client_id is None:
            self.__log.debug("Using generated client ID : %s" % self.client_id)
            client_id = gen_client_id()
        self.client_id = client_id
        self._loop = loop if loop else asyncio.get_event_loop()

        self.session = None
        self._handler = None
        self._disconnect_task = None
        self._connected_state = asyncio.Event()

        self._retry_counter = CgClient._new_retry_counter(config)

        # prevent re-entrance
        self._connecting_lock = asyncio.Lock()

        # no more retry
        self._bye = asyncio.Event()
        self._connected = asyncio.Event()
        self._delivery_lock = asyncio.Lock()
        self._delivery_cancel = asyncio.Event()
        self._delivery_cancel.set()


        # TODO plugins manager
        #context = ClientContext()
        #context.config = self.config
        #self.plugins_manager = PluginManager('hbmqtt.client.plugins', context)

        #self.client_tasks = deque()

    @staticmethod
    def _new_retry_counter(config):
        if config.get('auto_reconnect', False):
            return _RetryCounter()
        else:
            max_retries = config.get('reconnect_retries', CgClient._defaults['reconnect_retries'])
            max_interval = config.get('reconnect_max_interval', CgClient._defaults['reconnect_max_interval'])
            return _RetryCounter(max_retries=max_retries, max_retry_interval=max_interval)

    def _state(self):
        if not self.session:
            return _CgClientState.NEVER_CONNECTED
        elif not self._retry_counter.retry_ceased():
            return _CgClientState.ONGOING
        else:
            return _CgClientState.RETRY_CEASED

    def _is_connection_ongoing(self):
        """
        An ongoing connection spans across multiple auto-re-connections until no
        more retries.

        Note that the state might flip after next await.
        :return:
        """
        return self.session and not self._retry_counter.max_tries_reached()

    async def connect(self,
                uri=None,
                cleansession=None,
                cafile=None,
                capath=None,
                cadata=None):
        """
            Connect to a remote broker.

            At first, a network connection is established with the server using
            the given protocol (``mqtt``, ``mqtts``, ``ws`` or ``wss``). Once
            the socket is connected, a `CONNECT <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718028>`_ message is sent with the requested informations.

            This method is a *coroutine*.

            :param uri: Broker URI connection, conforming to `MQTT URI scheme <https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme>`_. Uses ``uri`` config attribute by default.
            :param cleansession: MQTT CONNECT clean session flag
            :param cafile: server certificate authority file (optional, used for secured connection)
            :param capath: server certificate authority path (optional, used for secured connection)
            :param cadata: server certificate authority data (optional, used for secured connection)
            :return: `CONNACK <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718033>`_ return code
            :raise: :class:`hbmqtt.client.ClientException`
        """
        await self._connect(uri, True, cleansession, cafile, capath, cadata)

    def _new_session(self, uri=None, cleansession=None, cafile=None,
                             capath=None, cadata=None):
        try:
            assert self._connecting_lock.locked()
            if self._state() == _CgClientState.ONGOING:
                # Ban connect() attempt from the user
                raise ClientException('ONGOING: Please try later')
            elif self._state() == _CgClientState.NEVER_CONNECTED:
                self.__log.info('NEVER_CONNECTED')
                return CgClient._initsession(self.client_id, self.config,
                                             uri, cleansession, cafile, capath,
                                             cadata)
            elif self._state() == _CgClientState.RETRY_CEASED:
                self.__log.info('RETRY_CEASED')
                return CgClient._initsession(self.client_id, self.config,
                                             uri, cleansession, cafile,
                                             capath, cadata)
            else:
                assert False
        except Exception as e:
            raise ClientException from e

    def _reuse_session(self):
        try:
            assert self._connecting_lock.locked()
            if self._state() == _CgClientState.ONGOING:
                # the same session will be shared by auto-re-connections
                self.__log.info('ONGOING: retrying #{} ...'.
                                format(self._retry_counter.retried))
                return self.session
            else:
                assert False
        except Exception as e:
            raise ClientException from e

    async def _connect(self, new_session=False, uri=None, cleansession=None,
                       cafile=None, capath=None, cadata=None):
        with await self._connecting_lock.acquire():
            try:
                if new_session:
                    session = self._new_session(uri, cleansession, cafile,
                                                capath, cadata)
                else:
                    session = self._reuse_session()
                self.logger.debug("Connect to: %s" % session.uri)
                handler, disconnect_task = await self._do_connect(session)
            except ConnectException as e:
                self.logger.warning("Connection failed: %r" % e)
                if not self.config.get('auto_reconnect', False):
                    raise ConnectException from e
                else:
                    # TODO
                    # return (await self.reconnect())
                    raise ConnectException from e
            except Exception as e:
                raise
            else:
                # successful
                # TODO: if new_session then create_task(cleanup_old_session)
                self.session = session
                self._handler = handler
                self._disconnect_task = disconnect_task
                # retry counter resets when every time connect() succeeds
                self._retry_counter.reset()

    @staticmethod
    def get_retain_and_qos(config, topic=None, qos=None, retain=None):
        if qos:
            assert qos in (QOS_0, QOS_1, QOS_2)
            _qos = qos
        else:
            _qos = self.config.get('default_qos',
                                   CgClient._defaults['default_qos'])
            try:
                _qos = self.config['topics'][topic]['qos']
            except KeyError:
                pass
        if retain:
            _retain = retain
        else:
            _retain = self.config.get('default_retain',
                                      CgClient._defaults['default_retain'])
            try:
                _retain = self.config['topics'][topic]['retain']
            except KeyError:
                pass
        return _qos, _retain

    async def publish(self, topic, message, qos=None, retain=None, timeout=None):
        if self.state() == _CgClientState.NEVER_CONNECTED:
            raise ClientException('NEVER_CONNECTED')
        orig_session = self.session
        #t_get = self._loop.create_task(orig_session.delivered_message_queue.get())

        t_bye = self._loop.create_task(self._retry_counter.wait_retry_ceased())
        done, _ = await asyncio.wait([t_get, t_bye],
                                     loop=self._loop,
                                     return_when=asyncio.FIRST_EXCEPTION,
                                     timeout=timeout)

        (app_qos, app_retain) = get_retain_and_qos()
        return (yield from self._handler.mqtt_publish(topic, message, app_qos, app_retain))
        pass

    async def _do_connect(self, session):
        assert self._connecting_lock.locked()
        try:
            reader, writer = await self._do_connect_get_stream(session)
            handler = ClientProtocolHandler(self.plugins_manager, session,
                                            reader, writer, loop=self._loop)
            return_code = await handler.mqtt_connect()
            if return_code is not CONNECTION_ACCEPTED:
                self.__log.warning("Connection rejected with code '%s'".format(return_code))
                raise ConnectException("Connection rejected by broker")
            disconnect_task = asyncio.ensure_future(
                self._handle_connection_close(), loop=self._loop)
            return handler, disconnect_task
        except Exception as e:
            if writer:
                self.__log.error('close Transport')
                asyncio.ensure_future(writer.close())
            raise ConnectException from e

    async def _handle_connection_close(self):
        with await self._connecting_lock.acquire():
            # acquired only it's not during connect()
            try:
                # So that no new connect() can proceed unless awaited.
                await self._handler.wait_disconnect()
                if self.config.get('auto_reconnect', False):


            except asyncio.CancelledError:
                # caused by disconnect()
                pass
            except Exception:
                pass

        try:
            self.logger.debug("Watch broker disconnection")
            # Wait for disconnection from broker (like connection lost)
            yield from self._handler.wait_disconnect()
            self.logger.warning("Disconnected from broker")

            # Block client API
            self._connected_state.clear()

            # stop an clean handler
            #yield from self._handler.stop()
            self._handler.detach()
            self.session.transitions.disconnect()

            if self.config.get('auto_reconnect', False):
                # Try reconnection
                self.logger.debug("Auto-reconnecting")
                try:
                    yield from self.reconnect()
                except CancelledError as e:
                    # cancelled caused by disconnect(), handle later
                    raise
                except Exception as e:
                    self.logger.exception(e)
                    # Cancel client pending tasks
                    while self.client_tasks:
                        self.client_tasks.popleft().set_exception(ClientException("Connection lost"))
                    self._bye = True
            else:
                # Cancel client pending tasks
                self.logger.debug('_handle_connection_close no auto_reconnect')
                while self.client_tasks:
                    self.client_tasks.popleft().set_exception(ClientException("Connection lost"))
                self._bye = True
        except CancelledError:
            self.logger.info('cancelled caused by disconnect()')
            assert(self._bye)
            raise

    def _create_ssl_context(self, session):
        assert session.scheme in ('mqtts', 'wss')
        # two-way authentication if required
        # server's CA
        sc = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                        cafile=session.cafile,
                                        capath=session.capath,
                                        cadata=session.cadata)
        # client's certs
        if 'certfile' in self.config and 'keyfile' in self.config:
            sc.load_cert_chain(self.config['certfile'], self.config['keyfile'])
        if 'check_hostname' in self.config and isinstance(self.config['check_hostname'], bool):
            sc.check_hostname = self.config['check_hostname']
        return sc

    async def _do_connect_get_stream(self, session):
        try:
            assert self._connecting_lock.locked()
            kwargs = dict()
            if session.scheme in ('mqtts', 'wss'):
                kwargs['ssl'] = self._create_ssl_context(session)
            if session.scheme in ('mqtt', 'mqtts'):
                reader, writer = await asyncio.open_connection(
                    session.remote_address, session.remote_port,
                    loop=self._loop, **kwargs)
                return StreamReaderAdapter(reader), StreamWriterAdapter(writer)
            else:
                assert session.scheme in ('ws', 'wss')
                websocket = await websockets.connect(
                    session.broker_uri, subprotocols=['mqtt'], loop=self._loop,
                    **kwargs)
                return WebSocketsReader(websocket), WebSocketsWriter(websocket)
        except Exception as e:
            if writer:
                self.__log.error('close Tcp Transport')
                writer.close()
            if websocket:
                self.__log.error('close Websocket Transport')
                asyncio.ensure_future(websocket.close())
            raise ConnectException from e

    async def deliver_message(self, timeout=None):
        """
            Deliver next received message.

            Deliver next message received from the broker. If no message is available, this methods waits until next message arrives or ``timeout`` occurs.

            This method is a *coroutine*.

            :param timeout: maximum number of seconds to wait before returning. If timeout is not specified or None, there is no limit to the wait time until next message arrives.
            :return: instance of :class:`hbmqtt.session.ApplicationMessage` containing received message information flow.
            :raises: :class:`asyncio.TimeoutError` if timeout occurs before a message is delivered
                     :class:`ClientException` if no message but all auto-retries have done.
        """
        # _deliver_lock essentially serializes calls of deliver_message.
        async with self._delivery_lock:
            if self._state() != _CgClientState.ONGOING:
                raise ClientException('await connect() first')
            # self.session might be reassigned if subsequent connect() succeeds.
            # In that case, _retry_counter might be triggered or reset.
            # It doesn't guarantee the user got data from the same session. The
            # user has to await all deliver_message before connect() again.
            orig_session = self.session
            t_get = self._loop.create_task(orig_session.delivered_message_queue.get())
            t_bye = self._loop.create_task(self._retry_counter.wait_retry_ceased())
            done, _ = await asyncio.wait([t_get, t_bye],
                                               loop=self._loop,
                                               return_when=asyncio.FIRST_EXCEPTION,
                                               timeout=timeout)
            if t_get in done:
                # take precedence over t_bye
                return t_get.result()
            elif t_bye in done:
                # TODO exception of another kind
                raise ClientException('Reconnection ceased')
            else:
                raise asyncio.TimeoutError

    @staticmethod
    def _initsession(client_id, config, uri=None, cleansession=None,
                     cafile=None, capath=None, cadata=None) -> Session:
        s = Session()
        s.broker_uri = uri if uri else config['broker']
        s.cafile = cafile if cafile else config.get('cafile')
        s.capath = capath if capath else config.get('capath')
        s.cadata = cadata if cadata else config.get('cadata')
        # Decode URI attributes
        uri_attributes = urlparse(s.broker_uri)

        if not uri_attributes.scheme in ('mqtts', 'wss', 'mqtt', 'ws'):
            raise ClientException('scheme {} not supported'.format(uri_attributes.scheme))

        s.scheme = uri_attributes.scheme
        if s.scheme in ('mqtts', 'wss'):
            if not s.cafile is None:
                raise ClientException("no certificate file (.cert) given")

        s.remote_address = uri_attributes.hostname
        s.remote_port = uri_attributes.port
        if s.scheme in ('mqtt', 'mqtts') and not s.remote_port:
            s.remote_port = 8883 if s.scheme == 'mqtts' else 1883
        if s.scheme in ('ws', 'wss') and not s.remote_port:
            s.remote_port = 443 if s.scheme == 'wss' else 80

        if s.scheme in ('ws', 'wss'):
            # Rewrite URI to conform to https://tools.ietf.org/html/rfc6455#section-3
            uri = (s.scheme, s.remote_address + ":" + str(s.remote_port),
                   uri_attributes[2], uri_attributes[3], uri_attributes[4],
                   uri_attributes[5])
            s.broker_uri = urlunparse(uri)

        s.username = uri_attributes.username
        s.password = uri_attributes.password
        s.client_id = client_id
        s.clean_session = cleansession if cleansession \
            else config.get('cleansession', True)
        k = config.get('keep_alive', CgClient._defaults['keep_alive']) -\
            config.get('ping_delay', CgClient._defaults['ping_delay'])
        s.keep_alive = max(k, CgClient._defaults['keep_alive_minimum'])

        if 'will' in config:
            s.will_flag = True
            s.will_topic = config['will']['topic']
            s.will_message = config['will']['message']
            s.will_retain = config['will']. \
                get('retain', CgClient._defaults['default_retain'])
            s.will_qos = config['will']. \
                get('qos', CgClient._defaults['default_qos'])
        else:
            s.will_flag = False
            s.will_topic = None
            s.will_message = None
            s.will_retain = False
        return s


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
        except CgClientException as e:
            # raised by overwritten reconnect()
            assert self._disconnected_event.is_set()
            raise e
        except (BaseException, Exception) as e:
            self._disconnected_event.set()
            raise from e

    async def reconnect(self, cleansession=None):
        try:
            super().reconnect(cleansession)
        except ConnectException as e:
            self._disconnected_event.set()
            raise CgClientException from e



