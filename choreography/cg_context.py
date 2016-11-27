# vim:fileencoding=utf-8

import attr
from attr import validators
import asyncio

class CgMetrics(object):
    pass

@attr.s(frozen=True)
class CgContext(object):
    service_id = attr.ib()
    metrics = attr.ib(validator=validators.optional(validators.instance_of(
        CgMetrics)))
    broker_conf = attr.ib()
    client_conf = attr.ib()
    launcher_conf = attr.ib()
    companion_cls = attr.ib()
    companion_conf = attr.ib()
    loop = attr.ib(validators.instance_of(asyncio.BaseEventLoop))


