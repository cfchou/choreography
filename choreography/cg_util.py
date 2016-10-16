# vim:fileencoding=utf-8
import abc
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
from choreography.cg_exception import CgException, CgLauncherException
import collections
from autologging import logged


def deep_get(nesting, default, *keys):
    try:
        return functools.reduce(lambda d, k: d[k], keys, nesting)
    except (KeyError, TypeError) as e:
        return default


def ideep_get(nesting, *keys):
    return deep_get(nesting, None, *keys)


def to_cd(weights: list) -> list:
    """
    Create a cumulative distribution.
    weights must all > 0
    """
    if not weights:
        raise CgException('len(weights) must > 0')
    cd = []
    for w in weights:
        if w < 0:
            raise CgException('all weights must >= 0')
        cd.append(w) if len(cd) == 0 else cd.append(cd[-1] + w)
    return cd


def cdf_from_cd(cd: list, x: int=None) -> int:
    """
    A CDF(cumulative distribution function) parameterized by a cumulative
    distribution.
    If 'x' is None or <= 0, x = randrange(1, cd[-1] + 1)
    Otherwise, x = x % cd[-1] + 1.

    :param cd: should be a monotonically increasing list of positive integers.
               Only a prefix of elements can be 0. However, we won't validate it
               thoroughly.
    :param x:
    :return:
    """
    if cd is None or len(cd) == 0:
        raise CgException('invalid cd {}'.format(cd))
    if cd[-1] <= 0:
        raise CgException('invalid cd {}'.format(cd))

    if x is None or x <= 0:
        x = random.randrange(1, cd[-1])
    else:
        m = x % cd[-1]
        x = cd[-1] if m == 0 else m
    # 1 <= x <= cd[-1]
    for i, v in enumerate(cd):
        if x <= v:
            return i
    raise CgException('Can\'t be here')


def cdf_from_weights(weights: list, x: int=None) -> int:
    """
    A CDF(cumulative distribution function) parameterized by weights.
    :param weights:
    :param x:
    :return:
    """
    cd = to_cd(weights)
    return cdf_from_cd(cd, x)


@logged
def future_successful_result(fu: asyncio.Future):
    """
    Return
    :param fu:
    :return:
    """
    if fu.cancelled():
        future_successful_result._log.info('future cancelled: {}'.format(fu))
        return None
    if fu.done() and fu.exception() is not None:
        future_successful_result._log.exception(fu.exception())
        return None
    return fu.result()


@logged
def convert_coro_exception(from_excp, to_excp):
    if from_excp is None:
        raise CgException('invalid from_excp {}'.format(from_excp))
    if to_excp is None or not issubclass(from_excp, BaseException):
        raise CgException('invalid to_excp {}'.format(to_excp))

    def decorate(coro):
        @functools.wraps(coro)
        async def converted(*args, **kwargs):
            try:
                return await coro(*args, **kwargs)
            except from_excp as e:
                convert_coro_exception._log.exception(e)
                raise to_excp from e
        return converted
    return decorate


@logged
def convert_exception(from_excp, to_excp):
    if from_excp is None:
        raise CgException('invalid from_excp {}'.format(from_excp))
    if to_excp is None or not issubclass(from_excp, BaseException):
        raise CgException('invalid to_excp {}'.format(to_excp))

    def decorate(func):
        @functools.wraps(func)
        def converted(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except from_excp as e:
                convert_exception._log.exception(e)
                raise to_excp from e
        return converted
    return decorate


def find_plugin(mgr: ExtensionManager, plugin_name):
    ps = [e.plugin for e in mgr.extensions if e.name == plugin_name]
    if len(ps) != 1:
        raise CgException('number of plugin {}: {}'.
                          format(plugin_name, len(ps)))
    return ps[0]


@logged
def load_plugin_managers(launchers_conf) \
        -> Tuple[NamedExtensionManager, NamedExtensionManager]:
    def on_missing_launcher_plugin(name):
        raise CgException('missing launcher plugin {}'.format(name))

    def on_missing_companion_plugin(name):
        raise CgException('missing companion plugin {}'.format(name))

    load_plugin_managers._log.debug(pprint.pformat(launchers_conf))
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
            raise CgException('name should be globally unique')

        names |= companion_names
        companion_plugins |= set([ideep_get(c, 'companion', 'plugin')
                                  for c in companions])

    load_plugin_managers._log.info('trying to load launcher plugins: {}'.
                                   format(launcher_plugins))
    launcher_mgr = NamedExtensionManager(
        namespace='choreography.launcher_plugins',
        on_missing_entrypoints_callback=on_missing_launcher_plugin,
        names=launcher_plugins)

    load_plugin_managers.log.info('trying to load companion plugins: {}'.
                                  format(companion_plugins))
    companion_mgr = NamedExtensionManager(
        namespace='choreography.launcher_plugins',
        on_missing_entrypoints_callback=on_missing_companion_plugin,
        names=companion_plugins)
    return launcher_mgr, companion_mgr


@logged
def load_yaml(runner_yaml: str) -> dict:
    with open(runner_yaml) as fh:
        load_yaml._log.info('load_yaml {}'.format(runner_yaml))
        return yaml.load(fh)


def validate_runner_conf(conf):
    pass


class PluginConf(abc.ABC):
    """
    Check plugin_args used by the framework. Plugin should go further to check
    plugin_args used by itself.
    """
    @abc.abstractmethod
    def __init__(self, namespace: str, name: str, plugin, plugin_args: dict):
        self.namespace = namespace
        self.name = name
        self.plugin = plugin
        self.plugin_args = plugin_args

    @convert_exception(Exception, CgException)
    def new_instance(self, loop, name: str=''):
        if not name:
            name = uuid.uuid1()
        return self.plugin(namespace=self.namespace, plugin_name=self.name,
                           name=name, config=self.plugin_args, loop=loop)


class LauncherPluginConf(PluginConf):
    def __init__(self, namespace: str, name: str, plugin, plugin_args: dict):
        msg = ideep_get(plugin_args, 'will', 'message')
        if not isinstance(msg, bytes):
            if isinstance(msg, str):
                plugin_args['will']['message'] = msg.encode('utf-8')
            else:
                raise CgException('will message can\'t be encoded to bytes')
        super().__init__(namespace, name, plugin, plugin_args)


class CompanionPluginConf(PluginConf):
    def __init__(self, namespace: str, name: str, plugin, plugin_args: dict,
                 weight: int=None):
        if weight is not None and not isinstance(weight, int) and weight < 0:
            raise CgException('weight must an int >= 0')
        super().__init__(namespace, name, plugin, plugin_args)
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
    def build(runner_yaml: str, namespace: str = None):
        runner_conf = load_yaml(runner_yaml)
        if namespace is None:
            namespace = runner_conf.get('namespace', uuid.uuid1())
        names = set()
        validate_runner_conf(runner_conf)
        lmgr, cmgr = load_plugin_managers(runner_conf['launchers'])
        launchers_default = deep_get(runner_conf, {}, 'default', 'launcher')
        companions_default = deep_get(runner_conf, {}, 'default', 'companion')

        launcher_plugins = []
        launcher_companion_plugins = {}
        for lc in runner_conf['launchers']:
            lc_args = copy.deepcopy(launchers_default)
            lc_args.update(lc.get('args', {}))
            lc_class = find_plugin(lmgr, lc['plugin'])
            lc_name = lc['name']
            if lc_name in names:
                raise CgException('name {} should be globally unique'.
                                  format(lc_name))
            names.add(lc_name)
            launcher_plugins.append(LauncherPluginConf(namespace, lc_name,
                                                       lc_class, lc_args))

            companions = lc.get('companions', [])
            n_weights = sum([1 for c in companions
                             if c.get('weight') is not None])
            if n_weights != 0 and n_weights != len(companions):
                raise CgException('all or none weights')

            cps = []
            for cp in lc.get('companions', []):
                cp_args = copy.deepcopy(companions_default)
                cp_args.update(cp.get('args', {}))
                cp_class = find_plugin(cmgr, cp['plugin'])
                cp_name = cp['name']
                if cp_name in names:
                    raise CgException('name {} should be globally unique'.
                                      format(cp_name))
                names.add(cp_name)
                cp = CompanionPluginConf(namespace, cp_name, cp_class,
                                         cp_args, cp.get('weight'))
                cps.append(cp)

            launcher_companion_plugins[lc_name] = cps

        return RunnerContext(namespace, runner_conf.get('default'),
                             launcher_plugins, launcher_companion_plugins)




def update(target, src):
    """
    'target' is recursively updated by 'src'
    :param target:
    :param src:
    :return:
    """
    for k, v in src.items():
        if isinstance(v, collections.Mapping):
            tv = target.get(k, {})
            if isinstance(tv, collections.Mapping):
                update(tv, v)
                target[k] = tv
            else:
                target[k] = v
        else:
            target[k] = v


def gen_client_id(prefix='cg_cli/'):
    """
    Generates random client ID
    :return:
    """
    gen_id = prefix

    for i in range(7, 23):
        gen_id += chr(random.randint(0, 74) + 48)
    return gen_id


def lorem_ipsum(length: int=0) -> bytes:
    lorem = b"""Lorem ipsum dolor sit amet, consectetur adipiscing \
elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut \
enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut \
aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in \
voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint \
occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit \
anim id est laborum. """
    ll = len(lorem)
    if ll < length:
        q = length // ll
        m = length % ll
        return b''.join([lorem for _ in range(0, q)]) + lorem[:m]
    else:
        return lorem[:length]
