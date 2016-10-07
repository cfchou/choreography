# vim:fileencoding=utf-8
import functools
import copy
import pprint
import yaml
import random
from choreography.cg_exception import CgException
from stevedore.named import NamedExtensionManager, ExtensionManager
from typing import List, Tuple, Dict
from choreography import cg_launcher
from choreography.cg_launcher import Launcher, LcResp
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


def find_plugin(mgr: ExtensionManager, plugin_name):
    ps = [e.plugin for e in mgr.extensions if e.name == plugin_name]
    if len(ps) != 1:
        raise CgException('number of plugin {}: {}'.
                          format(plugin_name, len(ps)))
    return ps[0]


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


class PluginConf(PluginConf):
    pass


class CompanionPluginConf(PluginConf):
    def __init__(self, name: str, plugin, plugin_args: dict, weight: int):
        super().__init__(name, plugin, plugin_args)
        self.weight = weight


class RunnerContext(object):
    def __init__(self, name: str, default: dict,
                 launcher_plugins: [PluginConf],
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

        launcher_plugins = []
        launcher_companion_plugins = {}
        for lc in runner_conf['launchers']:
            # locally overwrite default
            lc_args = copy.deepcopy(launchers_default)
            lc_args.update(lc.get('args', {}))
            lp = find_plugin(lmgr, lc['plugin'])
            launcher_plugins.append(PluginConf(lc['name'], lp, lc_args),
                                    lc_args)

            companions = lc.get('companions', [])
            n_weights = sum([1 for c in companions
                             if c.get('weight') is not None])
            if n_weights != 0 and n_weights != len(companions):
                raise CgException('all or none weights')
            cps = []
            for c in lc.get('companions', []):
                w = c.get('weight')
                if w is not None and w < 0:
                    raise CgException('weight must >= 0')
                cp = CompanionPluginConf(c['name'],
                                         find_plugin(cmgr, c['plugin']),
                                         c.get('args', {}), w)
                cps.append(cp)

            launcher_companion_plugins[lc['name']] = cps

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


def weighted_indexing(weights: list, max_next):
    if not weights or max <= 0:
        raise CgException('weights:{}, max_next: {}'.format(weights, max_next))

    def gen(_v, _nv):
        return (x for x in [_v] * _nv)

    idx = 0
    n_next = 0
    while n_next < max_next:
        for i in gen(idx, weights[idx]):
            yield i
            n_next += 1
        idx = 0 if idx + 1 == len(weights) else idx + 1


def to_cdf(weights: list):
    if not weights:
        raise CgException('len(weights) must > 0')
    if len([w for w in weights if w < 0]):
        raise CgException('all weights must >= 0')
    cdf = []
    for w in weights:
        cdf.append(w) if len(cdf) == 0 else cdf.append(cdf[-1] + w)
    return cdf


def cdf_index(cdf: list, r: int=None):
    if cdf is None or len(cdf) <= 0:
        raise CgException('invalid cdf {}'.format(cdf))
    # cdf must be a monotonically increasing list
    if r is None:
        r = random.randrange(1, cdf[-1])
    for i, v in enumerate(cdf):
        if r > v:
            return i

#def _run_client2(companion_plugins: List[CompanionPluginConf],
#                fire: cg_launcher.Fire, loop: BaseEventLoop) -> asyncio.Task:
#
#    def get_index_func(cps):
#        if len(cps) <= 0:
#            return -1
#        ws = sum([1 for cp in cps if cp.weight is not None])
#        if ws != 0 and ws != len(cps):
#            raise CgException('all or none weights')
#        if ws == 0:
#            return lambda: random.randrange(0, len(cps))
#        else:
#            cdf = to_cdf([cp.weight for cp in cps])
#            return lambda: cdf_index(cdf)
#
#    get_idx = get_index_func(companion_plugins)
#
#    async def connect():
#        conf = await fire.conf_queue.get()
#        cc = CgClient(config=conf, loop=loop)
#        log.debug('CgClient {} await connect...'.format(cc.client_id))
#        await cc.connect()
#        log.debug('CgClient {} connect awaited'.format(cc.client_id))
#        return cc
#
#    def connect_cb(fu: asyncio.Future):
#        if fu.cancelled():
#            log.info('connect cancelled: {}'.format(fu))
#            return
#        if fu.exception() is not None:
#            log.info('connect exception: {}'.format(fu.exception()))
#            return
#        cc = fu.result()
#        if not cc.is_connected():
#            log.info('connect is not connected: {}'.format(cc))
#            return
#        log.info('connect result: {}'.format(cc))
#        if len(companion_plugins) <= 0:
#            log.info('skip running because no companion plugs')
#            return
#        i = get_idx()
#        cp = companion_plugins[i].load()
#        log.debug('use companion {}:{} to run'.format(i, cp.name))
#        loop.create_task(cc.run(cp))
#
#    task = loop.create_task(connect())
#    task.add_done_callback(connect_cb)
#    return task


def _pick_plugin_conf_func(cps):
    if len(cps) <= 0:
        return lambda _: None
    weights = [cp.weight for cp in cps]
    ws = sum([1 for w in weights if w is not None])
    if ws == 0:
        # no weights at all
        return lambda _: cps[random.randrange(0, len(cps))]
    if ws != len(cps):
        raise CgException('all or none weights')
    # all weights are presented
    cdf = to_cdf(weights)
    return lambda i: cps[cdf_index(cdf, i)]


def _run_client(uri: str,
                plugin_conf: CompanionPluginConf,
                fire: cg_launcher.Fire,
                loop: BaseEventLoop,
                cleansession: bool=None,
                cafile: str=None,
                capath: str=None,
                cadata: str=None,
                ) -> asyncio.Task:

    async def connect():
        client_id, conf = await fire.conf_queue.get()
        cc = CgClient(client_id=client_id, config=conf, loop=loop)
        log.debug('CgClient {} await connect...'.format(cc.client_id))
        await cc.connect(uri=uri, cleansession=cleansession, cafile=cafile,
                         capath=capath, cadata=cadata)
        log.debug('CgClient {} connect awaited'.format(cc.client_id))
        return cc

    def connect_cb(_fu: asyncio.Future):
        if _fu.cancelled():
            log.info('connect cancelled: {}'.format(_fu))
            return
        if _fu.exception() is not None:
            log.info('connect exception: {}'.format(_fu.exception()))
            return
        cc = _fu.result()
        if not cc.is_connected():
            log.info('connect is not connected: {}'.format(cc))
            return
        log.info('connect result: {}'.format(cc))
        if plugin_conf is None:
            log.info('skip running because no companion plugs')
            return
        cp = plugin_conf.load()
        log.debug('use companion {} to run'.format(cp.name))
        loop.create_task(cc.run(cp))

    task = loop.create_task(connect())
    task.add_done_callback(connect_cb)
    return task


async def _do_fire(uri: str,
                   companion_plugins: List[CompanionPluginConf],
                   fire: cg_launcher.Fire,
                   loop: BaseEventLoop,
                   cleansession: bool=None,
                   cafile: str=None,
                   capath: str=None,
                   cadata: str=None,
                   ):

    def is_running_client(_task: asyncio.Task):
        if _task.cancelled():
            log.info('task cancelled: {}'.format(_task))
            return False
        if _task.exception() is not None:
            log.info('task exception: {}'.format(_task.exception()))
            return False
        cc = _task.result()
        if not isinstance(cc, CgClient):
            return False
        return cc.is_connected()

    pick_func = _pick_plugin_conf_func(companion_plugins)

    def run_client(_i,):
        p = pick_func(_i)
        return _run_client(uri, p, fire, loop, cleansession, cafile, capath,
                           cadata)

    log.debug('at {} _do_fire for {} secs'.format(loop.time, fire.duration))
    futs = [run_client(i, fire, loop) for i in range(0, fire.rate)]
    futs.append(asyncio.sleep(fire.duration))
    timeout = max(fire.duration, fire.timeout)
    done, _ = await asyncio.wait(futs, loop=loop, timeout=timeout)

    running = len([d for d in done if is_running_client(d)])
    log.debug('_dofire: done:{}, running:{}'.format(len(done), running))
    return running, fire.rate - running


async def _do_idle(idle: cg_launcher.Idle, loop: BaseEventLoop=None):
    await asyncio.sleep(idle.duration, loop=loop)


# TODO: an Launcher instance should only be run by launcher_runner only once
# might need to embed a state in Launcher.
# that ensures that launcher.ask is re-entrant free.
async def launcher_runner(launcher: Launcher,
                          companion_plugins: List[CompanionPluginConf],
                          loop: BaseEventLoop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    cmd_resp = LcResp()
    while True:
        log.debug("ask launcher {}".format(launcher.name))
        cmd = await launcher.ask(cmd_resp)
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
        succeeded, failed = await _do_fire(launcher.broker_uri(),
                                           companion_plugins, cmd.action, loop)
        after = loop.time()
        log.debug('after:{}'.format(after))
        cmd_resp.update(cmd, succeeded, failed)

