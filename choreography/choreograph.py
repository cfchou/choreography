# vim:fileencoding=utf-8

import asyncio
from hbmqtt.client import MQTTClient, ConnectException, ClientException
from hbmqtt.mqtt import connack

from choreography.cg_companion import Companion
from typing import List
import asyncio
from asyncio import BaseEventLoop
import logging
log = logging.getLogger(__name__)


class CgClient(MQTTClient):

    def __init__(self, client_id=None, config=None,
                 loop: BaseEventLoop=None):
        default_config = {
            'keep_alive': 60,
            'ping_delay': 1,
            'default_qos': 0,
            'default_retain': False,
            'auto_reconnect': False,
            'check_hostname': False
        }
        default_broker = {
            'cleansession': True
        }
        if config is not None:
            default_config.update(config)
            default_broker.update(config.get('broker', {}))
        default_config['broker'] = default_broker
        super().__init__(client_id, default_config, loop)

    def is_connected(self):
        return self.session.transitions.is_connected()

    async def connect(self, uri=None, cleansession=None, cafile=None,
                      capath=None, cadata=None):
        try:
            log.debug('connect to {}'.format(uri))
            return await super().connect(uri, cleansession, cafile, capath,
                                         cadata)
        except ConnectException as e:
            #log.exception(e)
            return connack.SERVER_UNAVAILABLE

    async def run(self, companion: Companion):
        log.debug('{} run...'.format(self.client_id))


