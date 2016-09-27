
from choreography.launcher import Launcher, RunnerContext, LauncherResp
from choreography.launcher import Fire, Terminate, RunnerHistoryItem

import logging
log = logging.getLogger(__name__)


class LinearLauncher(Launcher):
    def __init__(self, begin, end, step, rate, supervisor):
        super().__init__()

    def ask_next(self, runner_ctx: RunnerContext) -> LauncherResp:
        return super().ask_next(runner_ctx)