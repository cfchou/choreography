# vim:fileencoding=utf-8

import abc
from typing import List, Union, NamedTuple
from choreography.cg_context import CgContext, CgMetrics
from choreography.cg_exception import CgException
from choreography.cg_companion import CompanionRunner, CompanionFactory
import attr
from attr import validators


class CgLauncherException(CgException):
    """
    For a Launcher implementation to raise
    """


class LcCmd(abc.ABC):
    pass


@attr.s(frozen=True)
class LcFire(LcCmd):
    """
    Fire 'len(cids)' clients, only coming back to ask after 't' secs where:
        duration < 't' == clients are all done(either connected or failed)
    """
    cids = attr.ib(default=attr.Factory(list))
    duration = attr.ib(default=0)

class LcTerminate(LcCmd):
    """
    Don't ever come back to ask
    """
    pass


@attr.s(frozen=True)
class LcResp(object):
    prev_cmd = attr.ib(validator=attr.validators.instance_of(LcCmd))


@attr.s(frozen=True)
class LcFireResp(LcResp):
    result = attr.ib()


@attr.s
class Launcher(abc.ABC):
    context = attr.ib(validator=validators.instance_of(CgContext))
    config = attr.ib()

    @abc.abstractmethod
    async def ask(self, resp: LcResp=None) -> LcCmd:
        """
        :param resp:
        :return:
        """


class LauncherFactory(abc.ABC):
    @abc.abstractmethod
    def get_instance(self):
        pass


@attr.s
class LauncherRunner(abc.ABC):
    context = attr.ib(validator=validators.instance_of(CgContext))
    companion_factory = attr.ib(
        validator=validators.instance_of(CompanionFactory))
    companion_runner = attr.ib(
        validator=validators.instance_of(CompanionRunner))

    @abc.abstractmethod
    async def run(self, launcher):
        pass


