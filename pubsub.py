# vim:fileencoding=utf-8
import sys
import asyncio
import copy

from choreography.choreograph import CompanionPluginConf
from choreography.cg_companion import SelfSubscriber
from choreography.cg_launcher import OneShotLauncher2
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
        #'host': '172.31.29.195',
        'host': '10.1.204.14',
        'port': '8500'
    },
    'prometheus_exposure': {
        #'host': '172.31.29.195',
        'host': '10.1.204.14',
        'port': '28080'
    },
    'default': {
        'launcher': {
            'broker': {
                'uri': 'mqtts://127.0.0.1',
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
            #'will': {
            #    'topic': 'WILL_TOPIC',
            #    'message': b'WILL_MESSAGE',
            #    'qos': 1,
            #    'retain': False
            #},
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
            'plugin': 'OneShotLauncher2',
            'name': 'launch_pub',
            'args': {
                #after 'delay' secs, create and connect 'rate' number of clients
                # using 'step' secs for 'num_steps' times.
                #
                # In each step, it may takes more then 'step' seconds. Moreover,
                # 'auto_reconnect' will affect the time well.
                'delay': 0,         # delay
                'delay_max': 0,     # delay for random.uniform(delay, delay_max)
                'rate': 4,
                'step': 1,          # connect 'rate' clients using 'step' secs
                'num_steps': 70,     # total = 'offset' + 'rate' * 'num_steps'
                'client_id_prefix': 'cg_pub_'
            },
            'companions': [
                {
                    'plugin': 'SelfSubscriber',
                    'name': 'plugin_subpub_0001',
                    #'weight': 1
                    'args': {
                        'msg_len': 150,
                        #'msg': b'===== whatever ===== you ===== say =====',

                        # after 'delay' seconds, publish 'offset' msgs ASAP,
                        # then for every 'step' secs, publish 'rate' clients.
                        #
                        # total = 'offset' + 'rate' * 'num_steps'
                        'delay': 1,
                        'delay_max': 10,
                        'qos': 1,
                        'offset': 0,
                        'step': 60,
                        'num_steps': -1,
                        'rate': 1
                    }
                }
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


def _run(sd, sd_id, exposure):
    log.info('*****sd:{}, exposure:{}*****'.format(sd, exposure))
    loop = asyncio.get_event_loop()
    sub, sub_confs = init_launcher('test_run', config['default'],
                                   config['launchers'][0],
                                   OneShotLauncher2, SelfSubscriber, loop)
    loop.create_task(launcher_runner(sub, companion_plugins=sub_confs))

    sd_host = config['service_discovery']['host'] if sd[0] is None else sd[0]
    sd_port = config['service_discovery']['port'] if sd[1] is None else sd[1]
    log.info('*****Service discovery service {}:{}*****'.format(sd_host,
                                                                sd_port))

    ex_host = config['prometheus_exposure']['host'] if exposure[0] is None \
        else exposure[0]
    ex_port = config['prometheus_exposure']['port'] if exposure[1] is None \
        else exposure[1]
    log.info('*****Metrics exposed at {}:{}*****'.format(ex_host, ex_port))

    agent = cg_util.SdConsul(name='cg_metrics', service_id=sd_id, host=sd_host,
                             port=sd_port)

    loop.create_task(web.start_http_server(addr=ex_host, port=ex_port,
                                           loop=loop, service_discovery=agent))
    log.info('*****Running*****')
    loop.run_forever()
    log.info('*****Done*****')


@click.command()
@click.option('--sd', type=(str, int), default=(None, None), help='host port of service discovery service')
@click.option('--sd_id', default='cg_client', help='service id(must be unique to service discovery agent')
@click.option('--exposure', type=(str, int), default=(None, None), help='host port to expose metrics')
@click.option('--log_config', default='log_config.yaml', help='log_config.yaml')
def run(sd, sd_id, exposure, log_config):
    print('*****Reading config*****')
    print('*****sd:{}, sd_id:{}, exposure:{}, log_config:{}*****'.
          format(sd, sd_id, exposure, log_config))
    with open(log_config) as fh:
        try:
            logging.config.dictConfig(yaml.load(fh))
            _run(sd, sd_id, exposure)
        finally:
            logging.shutdown()
    print('*****Exits*****')


if __name__ == '__main__':
    run()

