# vim:fileencoding=utf-8
import functools
import copy
import pprint
import yaml
import random
from hbmqtt.mqtt import connack
from choreography.cg_util import ideep_get
from choreography.cg_exception import CgException
from stevedore.named import NamedExtensionManager
from typing import List, Union, NamedTuple, Tuple, Dict
from choreography import cg_launcher
from choreography.cg_launcher import Launcher
from choreography.cg_launcher import LauncherCmdResp, LauncherCmdRespItem
from choreography.cg_companion import Companion
from choreography.choreograph import CgClient
import uuid
import asyncio
from asyncio import BaseEventLoop
import logging

log = logging.getLogger(__name__)


def deep_get(nesting, default, *keys):
    try:
        return functools.reduce(lambda d, k: d[k], keys, nesting)
    except (KeyError, TypeError) as e:
        return default


def ideep_get(nesting, *keys):
    return deep_get(nesting, None, *keys)


def load_launchers(yaml_conf, launcher_mgr) -> List[Launcher]:
    def new_launcher(lc, default):
        plugin = lc['plugin']
        log.info('new_launcher: {}'.format(plugin))
        exts = [ext for ext in launcher_mgr.extensions if ext.name == plugin]
        if len(exts) == 0:
            raise CgException('plugin {} doesn\'t exist'.format(plugin))
        if len(exts) > 1:
            raise CgException('duplicated plugins {} found'.format(plugin))

        conf = copy.deepcopy(default)
        conf.update(lc.get('args', {}))
        return exts[0].plugin(conf)
    return [new_launcher(lc, yaml_conf['default'])
            for lc in yaml_conf['launchers']]


def load_plugins(launchers_conf) \
        -> Tuple[NamedExtensionManager, NamedExtensionManager]:
    def on_missing_launcher_plugin(name):
        raise CgException('missing launcher plugin {}'.format(name))

    def on_missing_companion_plugin(name):
        raise CgException('missing companion plugin {}'.format(name))

    log.debug(pprint.pformat(launchers_conf))
    names = set()
    launcher_plugins = set()
    companion_plugins = set()
    for lc in launchers_conf:
        if lc['name'] in names:
            raise CgException('names should be globally unique')
        names.add(lc['name'])
        launcher_plugins.add(lc['plugin'])

        companions = lc.get('companions', [])
        companion_names = set([c['name'] for c in companions])
        if len(names & companion_names) != 0:
            raise CgException('names should be globally unique')

        names |= companion_names
        companion_plugins |= set([ideep_get(c, 'companion', 'plugin')
                                  for c in companions])

    log.info('trying to load launcher plugins: {}'.format(launcher_plugins))
    launcher_mgr = NamedExtensionManager(
        namespace='choreography.launcher_plugins',
        on_missing_entrypoints_callback=on_missing_launcher_plugin,
        names=launcher_plugins)

    log.info('trying to load companion plugins: {}'.
             format(companion_plugins))
    companion_mgr = NamedExtensionManager(
        namespace='choreography.launcher_plugins',
        on_missing_entrypoints_callback=on_missing_companion_plugin,
        names=companion_plugins)
    return launcher_mgr, companion_mgr


def load_yaml(runner_yaml: str) -> dict:
    with open(runner_yaml) as fh:
        log.info('load_yaml {}'.format(runner_yaml))
        return yaml.load(fh)


def validate_runner_conf(conf):
    pass


class PluginConf(object):
    def __init__(self, name: str, plugin, plugin_args: dict):
        self.name = name
        self.plugin = plugin
        self.plugin_args = plugin_args

    def load(self):
        return self.plugin(self.plugin_args)


class LauncherPluginConf(PluginConf):
    pass


class CompanionPluginConf(PluginConf):
    def __init__(self, name: str, plugin, plugin_args: dict, weight: int):
        super().__init__(name, plugin, plugin_args)
        self.weight = weight


class RunnerContext(object):
    def __init__(self, name: str, default: dict,
                 launcher_plugins: [LauncherPluginConf],
                 launcher_companion_plugins: Dict[str, List[CompanionPluginConf]],
                 loop: BaseEventLoop=None):
        lp_names = launcher_companion_plugins.keys()
        if any([lp.name not in lp_names for lp in launcher_plugins]):
            raise CgException('missing companion plugins')

        self.name = name
        self.default = default
        self.launcher_plugins = launcher_plugins
        self.launcher_companion_plugins = launcher_companion_plugins
        self.loop = asyncio.get_event_loop() if loop is None else loop


    @staticmethod
    def build(runner_yaml: str, name: str = None):
        runner_conf = load_yaml(runner_yaml)
        validate_runner_conf(runner_conf)
        lmgr, cmgr = load_plugins(runner_conf['launchers'])
        launchers_default = deep_get(runner_conf, {}, 'default', 'launcher')

        def find_plugin(mgr, plugin_name):
            ps = [e.plugin for e in mgr.extensions if e.name == plugin_name]
            if len(ps) != 1:
                raise CgException('number of plugin {}: {}'.
                                  format(plugin_name, len(ps)))
            return ps[0]

        launcher_plugins = []
        launcher_companion_plugins = {}
        for lc in runner_conf['launchers']:
            # locally overwrite default
            lc_args = copy.deepcopy(launchers_default)
            lc_args.update(lc.get('args', {}))

            launcher_plugins.append(
                LauncherPluginConf(lc['name'], find_plugin(lmgr, lc['plugin']),
                                   lc_args))

            launcher_companion_plugins[lc['name']] = \
                [CompanionPluginConf(cc['name'],
                                     find_plugin(cmgr, cc['plugin']),
                                     cc.get('args', {}), cc['weight'])
                 for cc in lc.get('companions', [])]

        if name is None:
            name = runner_conf.get('name', uuid.uuid1())
        return RunnerContext(name, runner_conf.get('default'), launcher_plugins,
                             launcher_companion_plugins)

    async def run(self, loop: BaseEventLoop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        coros = []
        for lp in self.launcher_plugins:
            lc = lp.plugin(lp.name, lp.args, loop=loop)
            coro = launcher_runner(lc, self.launcher_companion_plugins[lp.name],
                                   loop)
            coros.append(coro)
        await asyncio.wait(coros)


def to_cdf(weights: list):
    if not weights:
        raise CgException('len(weights) must > 0')
    if len([w for w in weights if w < 0]):
        raise CgException('weights must >= 0')
    cdf = []
    for w in weights:
        cdf.append(w) if len(cdf) == 0 else cdf.append(cdf[-1] + w)
    return cdf


def random_cdf_index(cdf: list):
    # cdf most be a monotonically increasing list
    r = random.randrange(1, cdf[-1])
    for i, v in enumerate(cdf):
        if r > v:
            return i


def _run_client(companion_plugins: List[CompanionPluginConf],
                fire: cg_launcher.Fire, loop: BaseEventLoop) -> asyncio.Task:
    cdf = to_cdf([cp.weight for cp in companion_plugins])
    async def connect():
        conf = await fire.conf_queue.get()
        cc = CgClient(config=conf, loop=loop)
        log.debug('CgClient {} await connect...'.format(cc.client_id))
        await cc.connect()
        log.debug('CgClient {} connect awaited'.format(cc.client_id))
        return cc

    def connect_cb(fu: asyncio.Future):
        if fu.cancelled():
            log.info('connect cancelled: {}'.format(fu))
            return
        if fu.exception() is not None:
            log.info('connect exception: {}'.format(fu.exception()))
            return
        cc = fu.result()
        if not cc.is_connected():
            log.info('connect is not connected: {}'.format(cc))
            return
        log.info('connect result: {}'.format(cc))
        i = random_cdf_index(cdf)
        p = companion_plugins[i].load()
        loop.create_task(cc.run(p))

    task = loop.create_task(connect())
    task.add_done_callback(connect_cb)
    return task


async def _do_fire(companion_plugins: List[CompanionPluginConf],
                   fire: cg_launcher.Fire, loop: BaseEventLoop):
    def is_running_client(task: asyncio.Task):
        if task.cancelled():
            log.info('task cancelled: {}'.format(task))
            return False
        if task.exception() is not None:
            log.info('task exception: {}'.format(task.exception()))
            return False
        cc = task.result()
        if not isinstance(cc, CgClient):
            return False
        return cc.is_connected()

    log.debug('_do_fire for {} secs'.format(fire.step * fire.num_steps))
    history = []
    for i in range(0, fire.num_steps):
        fire_at = loop.time()
        log.debug('_do_fire {} clients at step {}'.format(fire.rate, i))
        futs = [_run_client(companion_plugins, fire, loop)
                for i in range(0, fire.rate)]
        futs.append(asyncio.sleep(fire.step))

        # NOTE: timeout might surpass fire.step
        done, _ = await asyncio.wait(futs, loop=loop,
                                     timeout=fire.timeout)

        running = len([d for d in done if is_running_client(d)])
        log.debug('_dofire: done:{}, running:{}'.format(len(done), running))
        history.append(LauncherCmdRespItem(at=fire_at, succeeded=running,
                                           failed=fire.rate - running))
    return history


async def _do_idle(idle: cg_launcher.Idle, loop: BaseEventLoop=None):
    await asyncio.sleep(idle.steps * idle.num_steps, loop=loop)


# TODO: an Launcher instance should only be run by launcher_runner only once
# might need to embed a state in Launcher.
# that ensures that launcher.ask is re-entrant free.
async def launcher_runner(launcher: Launcher,
                          companion_plugins: List[PluginConf],
                          loop: BaseEventLoop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    cmd_resp = LauncherCmdResp()
    while True:
        log.debug("ask launcher {}".format(launcher.name))
        cmd = await launcher.ask(cmd_resp, loop)
        if cmd.is_terminate():
            log.debug('terminate launcher {}'.format(launcher.name))
            break
        if cmd.is_idle():
            log.debug('idle launcher {}'.format(launcher.name))
            await _do_idle(cmd.action, loop)
            continue
        # cmd.is_fire
        before = loop.time()
        log.debug('before:{}'.format(before))
        history = await _do_fire(launcher, companion_plugins, cmd.action, loop)
        log.debug('len(hist):{}'.format(len(history)))
        after = loop.time()
        log.debug('after:{}'.format(after))
        cmd_resp.update(cmd, history)

