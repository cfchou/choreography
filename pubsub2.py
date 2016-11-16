# vim:fileencoding=utf-8

import click
import asyncio
from hbmqtt.client import MQTTClient, ConnectException
import random
from choreography.cg_util import lorem_ipsum
import yaml
import resource
import tracemalloc

import logging.config
import logging
log = logging.getLogger(__name__)

config = {
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

    # result in a retry pattern of 2, 4, 8, 8
    'reconnect_max_interval': 8,
    'reconnect_retries': 4,

    #'will': {
    #    'topic': 'WILL_TOPIC',
    #    'message': b'WILL_MESSAGE',
    #    'qos': 1,
    #    'retain': False
    #},
    # will be updated by launchers[i]['args']
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


async def connect_all(cs, config, delay=0, delay_max=0, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    conf = config['broker']
    uri = conf['uri']
    cleansession = conf.get('cleansession')
    cafile = conf.get('cafile')
    capath = conf.get('capath')
    cadata = conf.get('cadata')
    for c in cs:
        log.info("%s Connecting to broker" % c.client_id)
        await c.connect(uri=uri,
                        cleansession=cleansession,
                        cafile=cafile,
                        capath=capath,
                        cadata=cadata)
        s = random.uniform(delay, delay_max)
        await asyncio.sleep(s, loop=loop)


async def sub(c, qos=0):
    topics = [(c.client_id, qos)]
    await c.subscribe(topics)


async def recv(c, timeout=None):
    while True:
        try:
            msg = await c.deliver_message(timeout)
            data = msg.publish_packet.data
            log.debug('recv: {} bytes'.format(len(data)))
        except Exception as e:
            log.exception(e)
            break


async def pub_self(c, qos=0, msg='', retain=False, idle=1, num=-1):
    topic = c.client_id
    while num != 0:
        await asyncio.sleep(idle)
        log.debug('pub: {} to {}'.format(c.client_id, topic))
        await c.publish(topic, msg, qos, retain)
        num -= 1


async def do_tasks(cs, config, msg='', loop=None):
    await connect_all(cs, config, delay=0, delay_max=1, loop=loop)
    log.debug('all connected')
    sub_tasks = []
    for c in cs:
        sub_tasks.append(loop.create_task(sub(c, qos=1)))
    done, pending = await asyncio.wait(sub_tasks)
    if pending:
        raise Exception('some failed to subscribe')
    log.debug('all subscribed')

    for c in cs:
        loop.create_task(recv(c))
        loop.create_task(pub_self(c, qos=1, msg=msg, retain=False, idle=1,
                                  num=-1))
    log.debug('all pubs scheduled')


@click.command()
@click.option('--nc', type=int, default=20, help='num of clients')
@click.option('--log_config', default='log_config2.yaml', help='log_config.yaml')
def run(nc, log_config):
    with open(log_config) as fh:
        try:
            tracemalloc.start()
            logging.config.dictConfig(yaml.load(fh))
            loop = asyncio.get_event_loop()
            cs = [MQTTClient(client_id='cg_{:05}'.format(i), config=config, loop=loop)
                  for i in range(0, nc)]
            msg = lorem_ipsum(1024)
            loop.create_task(do_tasks(cs, config, msg=msg, loop=loop))
            loop.create_task(log_mem(2, loop))
            log.debug('Run.......')
            loop.run_forever()
            log.debug('Exit.......')
        finally:
            logging.shutdown()


if __name__ == '__main__':
    run()
