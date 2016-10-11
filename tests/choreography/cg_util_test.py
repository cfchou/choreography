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
            'timeout': 1.0,
            'duration': 1.0
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
                'duration': 1.0,
                'timeout': 3.0
            }
        }
    ]
}



