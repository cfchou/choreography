
import sys
import asyncio
import copy
import os
from hbmqtt.client import MQTTClient, ConnectException, ClientException
from hbmqtt.mqtt import connack
from hbmqtt.errors import MQTTException
from hbmqtt.version import get_version
from docopt import docopt
from hbmqtt.mqtt.constants import QOS_0
from hbmqtt.utils import read_yaml_config
import types

from choreography.cg_launcher import OneShotLancher
from choreography.cg_launcher import load_launchers
from choreography.choreograph import launcher_runner
import yaml
import logging.config
import logging
log = logging.getLogger(__name__)


async def sleeper():
    loop = asyncio.get_event_loop()
    while True:
        log.debug('sleep at {}'.format(loop.time()))
        await asyncio.sleep(10)


def run():
    launcher = OneShotLancher()
    loop = asyncio.get_event_loop()
    runner = asyncio.ensure_future(launcher_runner(loop=loop,
                                                   launcher=launcher))
    log.debug("run")
    loop.run_until_complete(asyncio.wait([runner, sleeper()]))


def run3():
    launchers = load_launchers('launchers.yaml')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait([launcher_runner(launcher=lc)
                                          for lc in launchers]))






async def foo():
    try:
        await bar()
    except ClientException as e:
        print(repr(e))

    #raise Exception('WWWWW')

async def bar():
    await asyncio.sleep(2)
    raise ClientException('WWWWW')

def run2():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait([foo(), sleeper()]))



if __name__ == '__main__':
    with open('log_config.yaml') as fh:
        try:
            logging.config.dictConfig(yaml.load(fh))
            run()
        finally:
            logging.shutdown()
