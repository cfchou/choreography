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
from prometheus_async.aio import web
from prometheus_client import Counter, Gauge, Summary, Histogram
from prometheus_async.aio import count_exceptions, time, track_inprogress


import logging.config
import logging
log = logging.getLogger(__name__)


config = {
    'prometheus': {
        'addr': '0.0.0.0',
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
                'client_id': 'cg_sub_001'
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

from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.errors import MQTTException

async def do_sub(client):
    await client.connect(uri='mqtt://192.168.1.35',
                         cleansession=False,
                         cafile='server.pem')
    log.debug('- - - - - connected')
    filters = [('cg_topic', 1)]
    await client.subscribe(filters)
    log.debug('- - - - - subscribed')
    count = 0
    while True:
        log.debug('10008 = = = = =')
        try:
            if not client._connected_state.is_set():
                log.warning("10009 Client not connected, waiting for it".format(client.client_id))
                await client._connected_state.wait()
            log.warning("10010 Client connected, waiting")
            message = await client.deliver_message()
            count += 1
            data = message.publish_packet.data
            log.debug('= = = = = msg_{} ={}'.format(count, data.decode('utf-8')))

        except MQTTException:
            log.debug("Error reading packet")
        except asyncio.CancelledError as e:
            log.debug("= = = = = = = CancelledError")
            log.exception(e)
            break
        except ConnectException as e:
            log.exception(e)
            raise e from e
        except Exception as e:
            log.debug("= = = = = = = Exception")
            log.exception(e)
            # retry
        except KeyboardInterrupt:
            log.debug("= = = = = = = KeyboardInterrupt")
            break
    await client.disconnect()
    log.debug('- - - - - disconnected')


async def sub(loop):
    config = {
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
    }
    client = MQTTClient(client_id='cg_sub_001', config=config, loop=loop)
    await do_sub(client)


def test_launcher_runner():
    loop = asyncio.get_event_loop()


    # launcher 1
    lc_conf = copy.deepcopy(config['default']['launcher'])
    conf = config['launchers'][0]
    #lc_conf.update(conf['args'])
    cg_util.update(lc_conf, conf['args'])
    lc = OneShotLauncher('test_run', conf['name'], conf['name'], lc_conf,
                         loop=loop)


    pub_confs = []
    for cc_conf in config['launchers'][0]['companions']:
        cp_conf = copy.deepcopy(config['default']['companion'])
        cp_conf.update(cc_conf['args'])
        pub_confs.append(CompanionPluginConf('test_run', cc_conf['name'],
                                             LinearPublisher, cp_conf))


    # launcher 2
    lc_conf = copy.deepcopy(config['default']['launcher'])
    conf = config['launchers'][1]
    #lc_conf.update(conf['args'])
    cg_util.update(lc_conf, conf['args'])
    lc2 = OneInstanceLauncher('test_run', conf['name'], conf['name'], lc_conf,
                              loop=loop)

    sub_confs = []
    for cc_conf in config['launchers'][1]['companions']:
        cp_conf = copy.deepcopy(config['default']['companion'])
        cp_conf.update(cc_conf['args'])
        sub_confs.append(CompanionPluginConf('test_run', cc_conf['name'],
                                             OneShotSubscriber, cp_conf))

    agent = cg_util.SdConsul(name='cg_metrics')

    #loop.create_task(launcher_runner(lc, companion_plugins=[pub_conf, sub_conf]))
    loop.create_task(launcher_runner(lc, companion_plugins=pub_confs))
    loop.create_task(launcher_runner(lc2, companion_plugins=sub_confs))
    #loop.create_task(sub(loop))
    loop.create_task(web.start_http_server(addr='192.168.1.35', port=28080, loop=loop,
                                           service_discovery=agent))
    loop.run_forever()
    print('*****Done*****')



if __name__ == '__main__':
    with open('log_config.yaml') as fh:
        try:
            logging.config.dictConfig(yaml.load(fh))
            test_launcher_runner()
        finally:
            logging.shutdown()

