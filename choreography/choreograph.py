# vim:fileencoding=utf-8

import asyncio
from hbmqtt.client import MQTTClient, ConnectException, ClientException
from hbmqtt.mqtt import connack

from choreography.cg_util import PluginConf
from typing import List
import logging
log = logging.getLogger(__name__)


class CgClient(MQTTClient):

    def __init__(self, companion_plugins: List[PluginConf]=[],
                 client_id=None, config=None, loop=None):
        default_config = {
            'auto_reconnect': False
        }
        if config is not None:
            default_config.update(config)
        super().__init__(client_id, default_config, loop)

    async def connect(self, uri=None, cleansession=None, cafile=None,
                      capath=None, cadata=None):
        try:
            log.debug('connect to {}'.format(uri))
            return await super().connect(uri, cleansession, cafile, capath,
                                         cadata)
        except ConnectException as e:
            #log.exception(e)
            return connack.SERVER_UNAVAILABLE



