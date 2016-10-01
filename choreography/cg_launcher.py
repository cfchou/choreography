
import abc
from typing import List, Union, NamedTuple, Tuple
import copy
import uuid
import pprint
import yaml
from choreography.cg_util import ideep_get
from choreography.cg_exception import CgException
from stevedore.named import NamedExtensionManager

import logging
log = logging.getLogger(__name__)

# fire 'rate' clients every 'step' seconds for 'num_steps' times.
Fire = NamedTuple('Fire', [('rate', int), ('step', int), ('num_steps', int),
                           ('companion', str)])

# don't ever come back to ask
Terminate = NamedTuple('Terminate', [])

# come back after 'step' * 'num_steps' seconds elapse
Idle = NamedTuple('Idle', [('step', int), ('num_steps', int)])


RunnerHistoryItem = NamedTuple('RunnerHistoryItem',
                               [('at', int), ('succeeded', int),
                                ('failed', int)])


class LauncherResp(object):
    def __init__(self, action: Union[Fire, Idle], opaque=None):
        self.action = action
        self.opaque = opaque

    def is_fire(self):
        return isinstance(self.action, Fire)

    def is_idle(self):
        return isinstance(self.action, Idle)

    def is_terminate(self):
        return isinstance(self.action, Terminate)


class RunnerContext(object):
    def __init__(self, host: str, history: List[RunnerHistoryItem]):
        self.host = host
        self._history = [] if history is None else history
        self.opaque = None

    def update(self, prev_resp: LauncherResp, history: List[RunnerHistoryItem]):
        self._history = history
        self.opaque = prev_resp.opaque

    @staticmethod
    def new_conext(host: str=''):
        if not host:
            host = uuid.uuid1().hex
        return RunnerContext(host, [])


class Launcher(metaclass=abc.ABCMeta):
    def __init__(self, config):
        self.config = config

    @abc.abstractmethod
    def ask(self, runner_ctx: RunnerContext) -> LauncherResp:
        """

        :param runner_ctx:
        :return:
        """


def load_launchers(launchers_conf, launcher_mgr) -> List[Launcher]:
    def new_launcher(lc):
        plugin = lc['plugin']
        log.info('new_launcher: {}'.format(plugin))
        exts = [ext for ext in launcher_mgr.extensions if ext.name == plugin]
        if len(exts) == 0:
            raise CgException('plugin {} doesn\'t exist'.format(plugin))
        if len(exts) > 1:
            raise CgException('duplicated plugins {} found'.format(plugin))
        lc_copy = copy.deepcopy(lc)
        return exts[0].plugin(lc_copy)
    return [new_launcher(lc) for lc in launchers_conf]


# TODO: validate yaml by schema
def load_plugins(launchers_conf):
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
        companions = lc.get('companions')
        if companions is None or len(companions) == 0:
            # some Launchers such as IdleLauncher don't need companions
            log.info('no companion for launcher {}'.format(lc['name']))
        else:
            companion_names = set([c['name'] for c in companions])
            if len(names & companion_names) != 0:
                raise CgException('names should be globally unique')
            plugins = set([ideep_get(c, 'companion', 'plugin') for c in companions])
            companion_plugins |= plugins
        launcher_plugins.add(lc['plugin'])

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


def load_yaml(launchers_yaml: str) \
        -> Tuple[NamedExtensionManager, NamedExtensionManager]:
    """
    launchers_yaml formatted as:
    launchers:
      - name: launcher_1
        launcher_plugin: LinearLauncher

    :param launchers_yaml:
    :return:
    """
    def _load(f):
        log.info('load_launchers from {}'.format(f))
        with open(launchers_yaml) as fh:
            return yaml.load(fh)
    cg_conf = _load(launchers_yaml)
    lmgr, cmgr = load_plugins(cg_conf['launchers'])




def foo(launcher_yaml: str):
    lmgr, cmgr = load_plugins_from_yaml(launcher_yaml)



class IdleLancher(Launcher):
    def ask(self, runner_ctx: RunnerContext) -> LauncherResp:
        log.debug('runner_ctx:{}'.format(runner_ctx))
        return LauncherResp(Idle())


class OneShotLancher(Launcher):
    def ask(self, runner_ctx: RunnerContext) -> LauncherResp:
        log.debug('runner_ctx:{}'.format(runner_ctx.__dict__))
        if runner_ctx.opaque is not None:
            return LauncherResp(Terminate())
        else:
            return LauncherResp(Fire(rate=1, step=1, num_steps=1,
                                     companion=''), opaque=1)

