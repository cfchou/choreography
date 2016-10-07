# vim:fileencoding=utf-8


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
            'auto_reconnect': False,
            # 'reconnect_max_interval': 10,
            # 'reconnect_retries': 2
            # 'certfile:
            # 'keyfile:
            'check_hostname': False,
            'will': {
                'topic': 'WILL_TOPIC',
                'message': 'WILL_MESSAGE',
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
                'rate': 2,
                'duration': 1,
                'timeout': 3
            }
        }
    ]
}


from choreography.cg_util import *
from choreography.cg_launcher import OneShotLauncher
import copy
import asyncio


def test_launcher_runner():
    lc_conf = copy.deepcopy(config['default']['launcher'])
    conf = config['launchers'][0]
    lc_conf.update(conf['args'])
    lc = OneShotLauncher(conf['name'], lc_conf)
    loop = asyncio.get_event_loop()
    coro = launcher_runner(lc, companion_plugins=[])
    loop.run_until_complete(coro)


if __name__ == '__main__':
    test_launcher_runner()


