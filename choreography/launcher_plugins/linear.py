
from choreography.launcher import Launcher, RunnerContext, LauncherResp
from choreography.launcher import Fire, Terminate, RunnerHistoryItem

import logging
log = logging.getLogger(__name__)


class LinearLauncher(Launcher):

    def __init__(self, config):
        super().__init__(config)
        # sanity check
        self.config.get('start') and self.config.get('end') and self.config

    def ask_next(self, runner_ctx: RunnerContext) -> LauncherResp:
        return super().ask_next(runner_ctx)