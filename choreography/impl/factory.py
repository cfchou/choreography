# vim:fileencoding=utf-8

from choreography.cg_context import CgContext
from choreography.cg_exception import CgException
from choreography.cg_companion import Companion, CompanionFactory
from choreography.cg_launcher import Launcher, LauncherFactory
from stevedore import DriverManager
from autologging import logged


@logged
class CompanionEntryPointFactory(CompanionFactory):
    def __init__(self, context: CgContext):
        try:
            super().__init__()
            self.context = context
            self.__log.info('load choreography.companion_plugins:{}'.
                            format(context.companion_conf['plugin']))
            self.companion_cls = DriverManager(
                namespace='choreography.companion_plugins',
                name=context.companion_conf['plugin'],
                invoke_on_load=False).driver
        except Exception as e:
            raise CgException from e

    def get_instance(self, client_id) -> Companion:
        try:
            return self.companion_cls(context=self.context,
                                      config=self.context.companion_conf.get('config', dict()),
                                      client_id=client_id)
        except Exception as e:
            raise CgException from e


@logged
class LauncherEntryPointFactory(LauncherFactory):
    '''
    A singleton factory.
    '''
    def __init__(self, context: CgContext):
        super().__init__()
        self.context = context
        self.__log.info('load and create choreography.launcher_plugins:{}'.
                        format(context.launcher_conf['plugin']))
        self.launcher_cls = DriverManager(
            namespace='choreography.launcher_plugins',
            name=context.launcher_conf['plugin'],
            invoke_on_load=False).driver
        self.launcher_singleton = None

    def get_instance(self) -> Launcher:
        if self.launcher_singleton is None:
            config = self.context.launcher_conf.get('config', dict())
            self.launcher_singleton = self.launcher_cls(context=self.context,
                                                        config=config)
        return self.launcher_singleton


