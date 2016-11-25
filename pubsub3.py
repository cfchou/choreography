# vim:fileencoding=utf-8
import sys
import asyncio
import copy

from choreography.choreograph import cg_run
from choreography import cg_util
import yaml
import click
from prometheus_async.aio import web
from prometheus_client import Counter, Gauge, Summary, Histogram
from prometheus_async.aio import count_exceptions, time, track_inprogress

import resource
import tracemalloc

import logging.config
import logging
log = logging.getLogger(__name__)


config = {
    'service_discovery': {
        'service_id': 'runner_001', # should be unique
        #'host': '172.31.29.195',
        #'host': '192.168.1.36',
        'host': '10.1.204.14',
        'port': '8500'
    },
    'prometheus_exposure': {
        #'host': '172.31.29.195',
        #'host': '192.168.1.36',
        'host': '10.1.204.14',
        'port': '28080'
    },
    'custom_loop': 'uvloop',
    'broker': {
        'uri': 'mqtt://127.0.0.1',
        'cafile': 'server.pem',
        #'capath':
        #'cadata':
    },
    'client': {
        'cleansession': True
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
        'timeout': 1,
        'companion': {
            # will be updated by launchers[i]['companions'][j]['args']
        }
    },
    'launcher': {
        'plugin': 'TestLauncher',
        'config': {
            #after 'delay' secs, create and connect 'rate' number of clients
            # using 'step' secs for 'num_steps' times.
            #
            # In each step, it may takes more then 'step' seconds. Moreover,
            # 'auto_reconnect' will affect the time well.
            'delay': 0,         # delay
            'delay_max': 10,     # delay for random.uniform(delay, delay_max)
            'offset': 0,
            'rate': 2,
            'step': 1,          # connect 'rate' clients using 'step' secs
            'num_steps': 2,     # total = 'offset' + 'rate' * 'num_steps'
            'client_id_prefix': 'cg_pub_'
        },
    },
    # TODO: client_id_formatter
    'companion': {
        'plugin': 'TestCompanion',
        'config': {
            'msg_len': 150,
            #'msg': b'===== whatever ===== you ===== say =====',

            # after 'delay' seconds, publish 'offset' msgs ASAP,
            # then for every 'step' secs, publish 'rate' clients.
            #
            # total = 'offset' + 'rate' * 'num_steps'
            'delay': 1,
            'delay_max': 10,
            'offset': 0,
            'rate': 1,
            'step': 2,
            'num_steps': -1,
            'qos': 1,
        }
    }
}




snapshot1 = None
snapshot2 = None
rss_prev = 0
async def log_mem(t, loop):
    global snapshot1, snapshot2, rss_prev
    while True:
        res = resource.getrusage(resource.RUSAGE_SELF)
        log.debug("ru_maxrss: {}".format(res.ru_maxrss))

        if snapshot1 is None:
            rss_prev = res.ru_maxrss
            snapshot1 = tracemalloc.take_snapshot()
            snapshot2 = snapshot1
        else:
            if rss_prev != res.ru_maxrss:
                # compare when rss is increasing
                rss_prev = res.ru_maxrss
                snapshot1 = snapshot2
                snapshot2 = tracemalloc.take_snapshot()
                top_stats = snapshot2.compare_to(snapshot1, 'lineno')
                #top_stats = snapshot2.statistics('lineno')
                log.debug("[ Top 20 ] ============================= ")
                for stat in top_stats[:10]:
                    log.debug(stat)
                log.debug("============================= ")
        await asyncio.sleep(t, loop=loop)

def other_work(loop, conf):
    tracemalloc.start()
    #loop.create_task(log_mem(2, loop))


def _run():
    cg_run(config, func=other_work)
    log.info('*****Done*****')


@click.command()
@click.option('--log_config', default='log_config.yaml', help='log_config.yaml')
def run(log_config):
    print('*****Reading config*****')
    with open(log_config) as fh:
        try:
            logging.config.dictConfig(yaml.load(fh))
            _run()
        finally:
            logging.shutdown()
    print('*****Exits*****')


if __name__ == '__main__':
    run()

