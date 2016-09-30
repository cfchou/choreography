
import abc
from typing import List, Union, NamedTuple
import copy
import uuid
import yaml
from stevedore.named import NamedExtensionManager

import logging
log = logging.getLogger(__name__)

# fire 'rate' clients every 'step' seconds for 'num_steps' times.
Fire = NamedTuple('Fire', [('rate', int), ('step', int), ('num_steps', int),
                           ('supervisor', str)])

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


launcher_config_default = {
    'begin': 0,         # begin immediately
    'end': -1,          # runs infinitely
    'offset': 0,        # number of clients to start with
    'rate': 0,          # number of clients increased when reaching a step
    'step': 1,          # number of seconds
    'supervisor': ''    #
}


def load_launchers(launchers_yaml: str) -> List[Launcher]:
    """
    launchers_yaml formatted as:
    launchers:
      - name: launcher_1
        launcher_plugin: LinearLauncher

    :param launchers_yaml:
    :return:
    """
    log.info('load_launchers from {}'.format(launchers_yaml))
    with open(launchers_yaml) as fh:
        try:
            launchers_config = yaml.load(fh)
            laucher_names = [lc['launcher_plugin']
                             for lc in launchers_config['launchers']]
            log.info('trying to load plugins: {}'.format(laucher_names))

            launcher_mgr = NamedExtensionManager(
                namespace='choreography.launcher_plugins', names=laucher_names)
            log.info('available launcher_plugins:{}'.format(
                launcher_mgr.extensions))

            def new_launcher(lc):
                try:
                    name = lc['launcher_plugin']
                    log.info('new_launcher: {}'.format(name))
                    exts = [ext for ext in launcher_mgr.extensions
                            if ext.name == name]
                    if len(exts) == 0:
                        log.error('plugin {} doesn\'t exist'.format(name))
                        return None
                    if len(exts) > 1:
                        log.error('duplicated plugins {} found'.format(name))
                        return None
                    lc_copy = copy.deepcopy(lc)
                    lc_copy.update(launcher_config_default)
                    return exts[0].plugin(lc_copy)
                except Exception as e:
                    log.exception(e)
                    return None
                    # swallowed

            return [lc for lc in [new_launcher(lc)
                                  for lc in launchers_config['launchers']]
                    if lc is not None]
        except Exception as e:
            log.exception(e)
            raise e


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
                                     supervisor=''), opaque=1)

