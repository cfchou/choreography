# vim:fileencoding=utf-8

from prometheus_client import Counter, Gauge, Summary, Histogram
from prometheus_async.aio import count_exceptions, time, track_inprogress

connections_total = Gauge('cg_connections_total', 'connections in total')


received_total = Counter('cg_received_total', 'received publishes in total')

published_bytes_total = Counter('cg_published_bytes_total',
                                'published bytes in total')
received_bytes_total = Counter('cg_received_bytes_total',
                               'received bytes in total')

publish_hist = Histogram('cg_publish_hist', 'publish time')
subscribe_hist = Histogram('cg_subscribe_hist', 'subscribe time')
connect_hist = Histogram('cg_connect_hist', 'connect time')




