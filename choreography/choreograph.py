# vim:fileencoding=utf-8

from choreography.cg_launcher import LauncherRunner, LauncherFactory
from choreography.impl.cg_launcher_impl import LauncherRunnerDefault
from choreography.impl.cg_launcher_impl import LauncherEntryPointFactory

from choreography import cg_util
from choreography.cg_exception import CgException
from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_companion import CompanionFactory, CompanionRunner
from choreography.impl.cg_companion_impl import CompanionEntryPointFactory
from choreography.impl.cg_companion_impl import CompanionRunnerDefault
from prometheus_async.aio import web
import asyncio
from prometheus_async.aio import count_exceptions, time, track_inprogress
from uuid import uuid1
import attr
from attr import validators
import abc
import functools
import itertools
import pprint
from autologging import logged


@logged
def cg_custom_loop(package_name) -> asyncio.BaseEventLoop:
    try:
        if package_name == 'uvloop':
            import uvloop
            policy = uvloop.EventLoopPolicy()
            asyncio.set_event_loop_policy(policy=policy)
        else:
            cg_custom_loop._log('{} not supported'.format(package_name))
        return asyncio.get_event_loop()
    except Exception as e:
        raise CgException from e


@logged
def cg_create_metrics_task(config, service_id, loop=None):
    try:
        log = cg_create_metrics_task._log
        if loop is None:
            loop = asyncio.get_event_loop()
        # service discovery
        sd_host = config['service_discovery']['host']
        sd_port = config['service_discovery']['port']
        log.info('*****Service discovery service {} at {}:{}*****'.
                 format(service_id, sd_host, sd_port))

        agent = cg_util.SdConsul(name='cg_metrics', service_id=service_id, host=sd_host,
                                 port=sd_port)
        # metrics exposure
        ex_host = config['prometheus_exposure']['host']
        ex_port = config['prometheus_exposure']['port']
        log.info('*****Metrics exposed at {}:{}*****'. format(ex_host, ex_port))

        # TODO: initialise custom metrics and update context
        loop.create_task(web.start_http_server(addr=ex_host, port=ex_port,
                                               loop=loop, service_discovery=agent))
        return CgMetrics()
    except Exception as e:
        raise CgException from e


@logged
def cg_create_context(config, service_id, metrics=None, loop=None):
    try:
        if loop is None:
            loop = asyncio.get_event_loop()
        # prepare context
        return CgContext(service_id=service_id, metrics=metrics,
                         broker_conf=config['broker'],
                         launcher_conf=config['launcher'],
                         client_conf=config['client'],
                         companion_conf=config['companion'], loop=loop)
    except Exception as e:
        raise CgException from e


@logged
def cg_create_launcher_task(context: CgContext,
                            launcher_factory: LauncherFactory=None,
                            companion_factory: CompanionFactory=None,
                            launcher_runner: LauncherRunner=None,
                            companion_runner: CompanionRunner=None):
    try:
        log = cg_create_launcher_task._log

        log.info('load companion factory')
        if companion_factory is None:
            companion_factory = CompanionEntryPointFactory(context)

        log.info('load launcher factory and create a launcher')
        if launcher_factory is None:
            launcher = LauncherEntryPointFactory(context).get_instance()

        if companion_runner is None:
            companion_runner = CompanionRunnerDefault(context)
        if launcher_runner is None:
            launcher_runner = LauncherRunnerDefault(context, companion_factory,
                                                    companion_runner)
        return context.loop.create_task(launcher_runner.run(launcher))
    except Exception as e:
        raise CgException from e


@logged
def cg_run_forever(config):
    log = cg_run_forever._log
    def get_loop(custom_loop):
        if custom_loop == 'uvloop':
            log.info('custom loop {}'.format(custom_loop))
            return cg_custom_loop('uvloop')
        else:
            return asyncio.get_event_loop()

    service_id = config.get('service_id', uuid1())
    loop = get_loop(config.get('custom_loop'))
    #metrics = cg_create_metrics_task(config, service_id, loop)
    #context = cg_create_context(config, service_id, metrics=metrics, loop=loop)
    context = cg_create_context(config, service_id, loop=loop)
    cg_create_launcher_task(context)
    loop.run_forever()


