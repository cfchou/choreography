# vim:fileencoding=utf-8

import functools
import copy
import pprint
from typing import List, Tuple, Dict
import yaml
import random
import uuid
import asyncio
from choreography.cg_exception import CgException
import collections
import socket
from consul.aio import Consul
from consul import Check
from autologging import logged


def deep_get(nesting, default, *keys):
    try:
        return functools.reduce(lambda d, k: d[k], keys, nesting)
    except (KeyError, TypeError) as e:
        return default


def ideep_get(nesting, *keys):
    return deep_get(nesting, None, *keys)


def update(target, src):
    """
    'target' is recursively updated by 'src'
    :param target:
    :param src:
    :return:
    """
    for k, v in src.items():
        if isinstance(v, collections.Mapping):
            tv = target.get(k, {})
            if isinstance(tv, collections.Mapping):
                update(tv, v)
                target[k] = tv
            else:
                target[k] = v
        else:
            target[k] = v


def gen_client_id(prefix='cg_cli_'):
    """
    Generates random client ID
    Note that: '/', '#', '+' will not be chosen. Which makes the result a valid
    topic name.
    :return:
    """
    gen_id = prefix
    for i in range(7, 23):
        gen_id += chr(random.randint(0, 74) + 48)
    return gen_id


def get_delay(config):
    delay = config.get('delay', 0)
    delay_max = config.get('delay_max', delay)
    if delay_max >= delay >= 0:
        return random.uniform(delay, delay_max)
    else:
        raise CgException('delay={}, delay_max={}'.
                                format(delay, delay_max))


def lorem_ipsum(length: int=0) -> bytes:
    # TODO: as it'll be called a hell a lot of time, there's room to improve.
    lorem = b"""Lorem ipsum dolor sit amet, consectetur adipiscing \
elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut \
enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut \
aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in \
voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint \
occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit \
anim id est laborum. """
    ll = len(lorem)
    if ll < length:
        q = length // ll
        m = length % ll
        return b''.join([lorem for _ in range(0, q)]) + lorem[:m]
    else:
        return lorem[:length]


def get_outbound_addr(addr: str, port: int) -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((addr, port))
    outbound = s.getsockname()[0]
    s.close()
    return outbound


@logged
def load_yaml(runner_yaml: str) -> dict:
    try:
        log = load_yaml._log
        with open(runner_yaml) as fh:
            log.info('load_yaml {}'.format(runner_yaml))
            return yaml.load(fh)
    except Exception as e:
        raise CgException from e


class SdConsul(object):
    """
    Almost a copy-paste from prometheus_async.aio.sd.ConsulAgent.
    With a tweak that allows to set up host/port/scheme.
    """
    def __init__(self,
                 host='127.0.0.1',
                 port=8500,
                 scheme='http',
                 name="app-metrics", service_id=None, tags=(),
                 token=None, deregister=True):
        self.host = host
        self.port = port
        self.scheme = scheme
        self.name = name
        self.service_id = service_id or name
        self.tags = tags
        self.token = token
        self.deregister = deregister

    @asyncio.coroutine
    def register(self, metrics_server, loop):
        """
        :return: A coroutine callable to deregister or ``None``.
        """
        consul = Consul(host=self.host, port=self.port, scheme=self.scheme,
                        token=self.token, loop=loop)

        if not (yield from consul.agent.service.register(
                name=self.name,
                service_id=self.service_id,
                address=metrics_server.socket.addr,
                port=metrics_server.socket.port,
                tags=self.tags,
                check=Check.http(
                    metrics_server.url, "10s",
                )
        )):  # pragma: nocover
            return None

        @asyncio.coroutine
        def deregister():
            try:
                if self.deregister is True:
                    yield from consul.agent.service.deregister(self.service_id)
            finally:
                consul.close()

        return deregister

