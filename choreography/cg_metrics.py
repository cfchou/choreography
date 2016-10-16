# vim:fileencoding=utf-8

from prometheus_client import Counter, Gauge, Summary, Histogram
from prometheus_async.aio import count_exceptions, time, track_inprogress

connect_attempts_total = Counter('cg_connect_attempts_total',
                                 'connect attempts in total')
connections_total = Gauge('cg_connections_total', 'connections in total')

published_bytes_total = Counter('cg_published_bytes_total',
                                'published bytes in total')
received_bytes_total = Counter('cg_received_bytes_total',
                               'received bytes in total')

subscriptions_total = Gauge('cg_subscriptions_total', 'subscriptions in total')


