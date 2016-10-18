# vim:fileencoding=utf-8

from prometheus_client import Counter, Gauge, Summary, Histogram
from prometheus_async.aio import count_exceptions, time, track_inprogress

connect_attempts_total = Counter('cg_connect_attempts_total',
                                 'connect attempts in total')
connections_total = Gauge('cg_connections_total', 'connections in total')


publish_total = Counter('cg_publish_total', 'publish requests in total')
subscribe_total = Counter('cg_subscribe_total', 'publish requests in total')

published_bytes_total = Counter('cg_published_bytes_total',
                                'published bytes in total')
received_bytes_total = Counter('cg_received_bytes_total',
                               'received bytes in total')

publish_time = Histogram('cg_publish_time', 'publish time')
subscribe_time = Histogram('cg_subscribe_time', 'subscribe time')
connect_time = Histogram('cg_connect_time', 'connect time')




