# vim:fileencoding=utf-8
import functools
import copy
import pprint
import yaml
from choreography.cg_util import ideep_get
from choreography.cg_exception import CgException
from stevedore.named import NamedExtensionManager
from typing import List, Union, NamedTuple, Tuple, Dict
from choreography.cg_launcher import Launcher
from choreography.cg_companion import Companion
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


# launcher

def load_companions(companions_conf, companion_mgr) -> List[Companion]:
    pass


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
    def __init__(self, name: str, plugin: str, plugin_args: dict):
        self.name = name
        self.plugin = plugin
        self.plugin_args = plugin_args


class RunnerContext(object):
    def __init__(self, name: str, default: dict,
                 launcher_mgr: NamedExtensionManager,
                 companion_mgr: NamedExtensionManager,
                 launchers_plugin_conf: Dict[str, PluginConf],
                 companions_plugin_conf: Dict[str, List[PluginConf]]):
        self.name = name
        self.default = default
        self.launcher_mgr = launcher_mgr
        self.companion_mgr = companion_mgr
        self.launchers_plugin_conf = launchers_plugin_conf
        self.companions_plugin_conf = companions_plugin_conf

    @staticmethod
    def build(runner_yaml: str):
        runner_conf = load_yaml(runner_yaml)
        validate_runner_conf(runner_conf)
        lmgr, cmgr = load_plugins(runner_conf['launchers'])
        launchers_default = deep_get(runner_conf, {},'default', 'launcher')

        launchers_plugin_conf = {}
        companions_plugin_conf = {}
        for lc in runner_conf['launchers']:
            lc_args = copy.deepcopy(launchers_default)
            # locally overwrite default
            launchers_plugin_conf[lc['name']] = \
                PluginConf(lc['name'], lc['plugin'], lc_args)
            companions_plugin_conf[lc['name']] = \
                [PluginConf(cc['name'], cc['plugin'], cc.get('args', {}))
                 for cc in lc.get('companions', [])]

        return RunnerContext(runner_conf['name'], runner_conf.get('default'),
                             lmgr, cmgr, launchers_plugin_conf,
                             companions_plugin_conf)


