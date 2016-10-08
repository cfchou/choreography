# vim:fileencoding=utf-8
import functools
import copy
import pprint
from typing import List, Tuple, Dict
import yaml
import random
import uuid
import asyncio
from asyncio import BaseEventLoop
from stevedore.named import NamedExtensionManager, ExtensionManager
from choreography.cg_exception import CgException
from choreography.cg_launcher import Launcher
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

    #async def run(self, loop: BaseEventLoop=None):
    #    if loop is None:
    #        loop = asyncio.get_event_loop()
    #    coros = []
    #    for lp in self.launcher_plugins:
    #        lc = lp.plugin(lp.name, lp.args, loop=loop)
    #        coro = launcher_runner(lc, self.launcher_companion_plugins[lp.name],
    #                               loop)
    #        coros.append(coro)
    #    await asyncio.wait(coros)


def to_cd(weights: list) -> list:
    """
    Create a cumulative distribution.
    """
    if not weights:
        raise CgException('len(weights) must > 0')
    if len([w for w in weights if w < 0]):
        raise CgException('all weights must >= 0')
    cd = []
    for w in weights:
        cd.append(w) if len(cd) == 0 else cd.append(cd[-1] + w)
    return cd


def cdf_from_cd(cd: list, x: int=None) -> int:
    """
    A CDF(cumulative distribution function) parameterized by a cumulative
    distribution.
    If 'x' is None or <= 0, x = randrange(1, cd[-1])
    Otherwise, x %= cd[-1].

    :param cd:
    :param x:
    :return:
    """
    if cd is None or len(cd) < 0:
        raise CgException('invalid cd {}'.format(cd))

    if x is None or x <= 0:
        x = random.randrange(1, cd[-1])
    else:
        x %= len(cd[-1])
    for i, v in enumerate(cd):
        if x > v:
            return i


def cdf_from_weights(weights: list, x: int=None) -> int:
    """
    A CDF(cumulative distribution function) parameterized by weights.
    :param weights:
    :param x:
    :return:
    """
    cd = to_cd(weights)
    return cdf_from_cd(cd, x)

