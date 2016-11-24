# vim:fileencoding=utf-8

import multiprocessing
import os
from autologging import logged


print('====== cp is loaded at {}'.format(os.getpid()))
shared_list = ['a']

@logged
class TestCompanion(object):
    def __init__(self, name=''):
        super().__init__()
        print('{} TestCp {}'.format(os.getpid(), name))

    def work(self):
        print('working')


