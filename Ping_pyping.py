import pyping
import logging

'''
test ping to host to get avg_rtt

Need root execution
'''

host = '131.112.22.80'
count = 2
timeout = 5
pck_size = 1024

logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')
logger = logging.getLogger('Ping')

try:
    logger.debug('Ping to host: ' + host)
    p = pyping.ping(host, timeout, count=count, packet_size=pck_size)

    print p.avg_rtt
except Exception as e:
    logger.error(e.message)
