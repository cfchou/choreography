
from choreography.cg_launcher import Launcher, RunnerContext, LauncherResp
from choreography.cg_launcher import Fire, Terminate, RunnerHistoryItem

import logging
log = logging.getLogger(__name__)

launcher_config_default = {
    'begin': 0,         # begin immediately
    'end': -1,          # runs infinitely
    'offset': 0,        # number of clients to start with
    'rate': 0,          # number of clients increased when reaching a step
    'step': 1,          # number of seconds
    'companion': ''     #
}


class LinearLauncher(Launcher):
    """
    Monotonically linearly increase the number of clients
    """
    def __init__(self, config):
        super().__init__(config)
        # sanity check
        assert self.config.get('launcher_plugin') == self.__class__.__name__

        self.config.get('start') and self.config.get('end') and self.config

    def ask(self, runner_ctx: RunnerContext) -> LauncherResp:
        return super().ask_next(runner_ctx)