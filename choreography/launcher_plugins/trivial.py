# vim:fileencoding=utf-8

import multiprocessing
import os
from autologging import logged


print('====== lc is loaded at {}'.format(os.getpid()))
shared_list = ['a']

@logged
class TestLauncher(object):
    def __init__(self, name=''):
        super().__init__()
        print('{} TestLc {}'.format(os.getpid(), name))

    def work(self):
        print('working')
