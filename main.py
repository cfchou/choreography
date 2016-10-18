# vim:fileencoding=utf-8
import sys
import asyncio
import copy

from choreography.choreograph import CompanionPluginConf
from choreography.cg_companion import LinearPublisher, OneShotSubscriber
from choreography.cg_launcher import OneShotLauncher, OneInstanceLauncher
from choreography.choreograph import launcher_runner
from choreography import cg_util
import yaml
import click
from prometheus_async.aio import web
from prometheus_client import Counter, Gauge, Summary, Histogram
from prometheus_async.aio import count_exceptions, time, track_inprogress


import logging.config
import logging
log = logging.getLogger(__name__)


config = {
    'service_discovery': {
        'host': '172.31.29.195',
        'port': '8500'
    },
    'prometheus_exposure': {
        'host': '172.31.29.195',
        'port': '28080'
    },
    'default': {
        'launcher': {
            'broker': {
                'uri': 'mqtt://127.0.0.1',
                'cafile': 'server.pem',
                #'capath':
                #'cadata':
                'cleansession': True
            },
            'certfile': 'client.crt',
            'keyfile': 'client.key',
            'check_hostname': False,
            'keep_alive': 60,
            'ping_delay': 1,
            'default_qos': 0,
            'default_retain': False,
            'auto_reconnect': True,
            #'reconnect_max_interval': 11,
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
            # will be updated by launchers[i]['args']
            'rate': 1,
            'timeout': 1
        },
        'companion': {
            # will be updated by launchers[i]['companions'][j]['args']
        }
    },
    'launchers': [
        {
            'plugin': 'OneShotLauncher',
            'name': 'launch_pub',
            'args': {
                # after 'delay' secs, launch 'rate' number of clients within
                # 'timeout' secs.
                'delay': 5,    # delay to allow subscribers to go first
                'rate': 20,      # number of publishers
                'timeout': 10,
                'client_id_prefix': 'cg_pub_'
            },
            'companions': [
                {
                    'plugin': 'LinearPublisher',
                    'name': 'plugin_pub_0001',
                    #'weight': 1
                    'args': {
                        'topic': 'cg_topic',
                        'msg_len': 128,
                        #'msg': b'===== whatever ===== you ===== say =====',

                        # after 'delay' seconds, publish 'offset' msgs ASAP,
                        # then for every 'step' secs, publish 'rate' clients.
                        #
                        # total = 'offset' + 'rate' * 'num_steps'
                        #'delay': 3
                        'qos': 1,
                        'offset': 0,
                        'step': 1,
                        'num_steps': 2,
                        'rate': 2
                    }
                }
            ]
        },
        {
            'plugin': 'OneInstanceLauncher',
            'name': 'launch_sub',
            'args': {
                'broker': {
                    'cleansession': False,
                },
                'keep_alive': 600,
                'client_id_prefix': 'cg_sub_'
                #'client_id': 'cg_sub_001'
            },
            'companions': [
                {
                    'plugin': 'OneShotSubscriber',
                    'name': 'plugin_sub_0001',
                    'weight': 1,
                    'args': {
                        # after 'delay' secs, subscribe 'topics' ASAP,
                        # then idle for 'duration' for receiving messages.
                        # disconnect when 'duration' eclapsed.
                        # if 'duration' == 0 then idle forever.
                        'topics': [
                            {
                                'topic': 'cg_topic',
                                'qos': 1
                            }
                        ],
                        'delay': 0,
                        'duration': 0
                    }
                }
                #,{
                #    'plugin': 'OneShotSubscriber',
                #    'name': 'plugin_sub_0002',
                #    'weight': 1,
                #    'args': {
                #        'topics': [
                #            {
                #                'topic': 'cg_topic2',
                #                'qos': 1
                #            }
                #        ],
                #        'delay': 0,
                #        'duration': 0
                #    }
                #}
            ]
        }
    ]
}


def init_launcher(namespace, default, launcher_conf, lc_cls, cp_cls, loop):
    lc_conf = copy.deepcopy(default['launcher'])
    cg_util.update(lc_conf, launcher_conf['args'])
    lc = lc_cls(namespace, launcher_conf['name'], launcher_conf['name'],
                lc_conf, loop)

    all_cp_confs = []
    for companion_conf in launcher_conf['companions']:
        cp_conf = copy.deepcopy(default['companion'])
        cg_util.update(cp_conf, companion_conf['args'])
        all_cp_confs.append(CompanionPluginConf('test_run',
                                                companion_conf['name'],
                                                cp_cls, cp_conf))
    return lc, all_cp_confs


@click.command()
@click.option('--pub', 'role', flag_value='pub', default=True, help='either act as a publisher(default)')
@click.option('--sub', 'role', flag_value='sub', help='or act as a subscriber')
@click.option('--sd', type=(str, int), default=(None, None), help='host port of service discovery service')
@click.option('--exposure', type=(str, int), default=(None, None), help='host port to expose metrics')
def run(role, sd, exposure):
    loop = asyncio.get_event_loop()

    log.info('*****Reading config*****')

    if role:
        lc1, pub_confs = init_launcher('test_run', config['default'],
                                       config['launchers'][0], OneShotLauncher,
                                       LinearPublisher, loop)
        loop.create_task(launcher_runner(lc1, companion_plugins=pub_confs))
    else:
        lc2, sub_confs = init_launcher('test_run', config['default'],
                                       config['launchers'][1], OneInstanceLauncher,
                                       OneShotSubscriber, loop)
        loop.create_task(launcher_runner(lc2, companion_plugins=sub_confs))

    sd_host = config['service_discovery']['host'] if sd[0] is None else sd[0]
    sd_port = config['service_discovery']['port'] if sd[1] is None else sd[1]

    ex_host = config['prometheus_exposure']['host'] if exposure[0] is None \
        else exposure[0]
    ex_port = config['prometheus_exposure']['port'] if exposure[1] is None \
        else exposure[1]

    agent = cg_util.SdConsul(name='cg_metrics', host=sd_host, port=sd_port)

    loop.create_task(web.start_http_server(addr=ex_host, port=ex_port,
                                           loop=loop, service_discovery=agent))
    log.info('*****Running*****')
    loop.run_forever()
    log.info('*****Done*****')


if __name__ == '__main__':
    with open('log_config.yaml') as fh:
        try:
            logging.config.dictConfig(yaml.load(fh))
            run()
        finally:
            logging.shutdown()

