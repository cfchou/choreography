# vim:fileencoding=utf-8

import os
from choreography.cg_launcher import Launcher, LcResp, LcCmd
from autologging import logged


print('====== lc is loaded at {}'.format(os.getpid()))
shared_list = ['a']

@logged
class TestLauncher(Launcher):
    def __init__(self, context, **kwargs):
        super().__init__()
        print('{} TestLc {}'.format(os.getpid(), context))

    async def ask(self, resp: LcResp = None) -> LcCmd:
        return super().ask(resp)

