# vim:fileencoding=utf-8

from choreography.choreograph import launcher_runner
from choreography.cg_launcher import OneShotLauncher
import copy
import asyncio

import logging
import logging.config
log = logging.getLogger(__name__)

import yaml

config = {
    'default': {
        'launcher': {
            'broker': {
                'uri': 'mqtt://127.0.0.1',
                # 'cafile':
                # 'capath':
                # 'cadata':
                'cleansession': True
            },
            'keep_alive': 60,
            'ping_delay': 1,
            'default_qos': 0,
            'default_retain': False,
            'auto_reconnect': True,
            #'reconnect_max_interval': 10,
            'reconnect_retries': 3,
            # 'certfile:
            # 'keyfile:
            'check_hostname': False,
            'will': {
                'topic': 'WILL_TOPIC',
                'message': b'WILL_MESSAGE',
                'qos': 1,
                'retain': False
            },
            'rate': 1,
            'timeout': 1,
            'duration': 1
        },
        'companion': {
        }
    },
    'launchers': [
        {
            'plugin': 'OneShotLauncher',
            'name': 'one_001',
            'args': {
                'rate': 1,
                'duration': 1,
                'timeout': 3
            }
        }
    ]
}

def test_launcher_runner():
    lc_conf = copy.deepcopy(config['default']['launcher'])
    conf = config['launchers'][0]
    lc_conf.update(conf['args'])
    lc = OneShotLauncher(conf['name'], lc_conf)
    loop = asyncio.get_event_loop()

    tasks = asyncio.wait([launcher_runner(lc, companion_plugins=[]),
                         asyncio.sleep(20)])
    loop.run_until_complete(tasks)
    print('*****Done*****')

if __name__ == '__main__':
    with open('log_config.yaml') as fh:
        try:
            logging.config.dictConfig(yaml.load(fh))
            test_launcher_runner()
        finally:
            logging.shutdown()

