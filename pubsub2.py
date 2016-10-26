# vim:fileencoding=utf-8

import click
import asyncio
from hbmqtt.client import MQTTClient, ConnectException
import random
from choreography.cg_util import lorem_ipsum

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
    topics=[(c.client_id, qos)]
    await c.subscribe(topics)

async def pub(c, qos=0, msg='', retain=False):
    topic = c.client_id
    await c.publish(topic, msg, qos, retain)


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
    while True:
        pub_tasks = []
        for c in cs:
            pub_tasks.append(loop.create_task(pub(c, qos=1, msg=msg, retain=False)))
            done, pending = await asyncio.wait(pub_tasks)
            if pending:
                log.debug('some failed to publish')


@click.command()
@click.option('--nc', type=int, default=1, help='num of clients')
def run(nc):
    loop = asyncio.get_event_loop()
    cs = [MQTTClient(client_id='cg_{:05}'.format(i), config=config, loop=loop)
          for i in range(0, nc)]
    msg = lorem_ipsum(150)
    do_tasks(cs, config, msg=msg, loop=loop)
    log.debug('Run.......')
    loop.run_forever()
    log.debug('Exit.......')




if __name__ == '__main__':
    run()
