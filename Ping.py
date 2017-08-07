import subprocess
from sys import maxint
import re
import threading
import logging
import paramiko

# TODO: make it as a class


# create ping command
# parameters = "-n %d -l %d -w %f" % (count,pck_size,timeout*1000) if system_name().lower()=="windows" else "-c %d -s %d -W %f" % (count,pck_size,timeout)
# command = 'ping '+host+' '+parameters
import time

global logger

INDEX_AVG_RTT = 7


def ping(host, count=2, timeout=2, pck_size=1024):
    '''
    ping to host for get avg_rtt
    :return: avg_rtt of ping if not succeed, reture MAX_INT
    '''
    ping_result = None
    rtt = maxint

    try:
        # only for linux host
        ping_out = subprocess.check_output(
            ['ping', host, '-c', str(count), '-s', str(pck_size), '-W', str(timeout)]).splitlines()

        len_ping = len(ping_out)

        if len_ping > 0:
            ping_result = ping_out[len_ping - 1]

        if len(ping_result) != 0:
            # INDEX:  [ 0   ,  1   ,  2   ,  3   ,  4    ,  5 ,  6     ,  7     ,  8     ,  9     ,  10 ]
            # EX_TXT: ['rtt', 'min', 'avg', 'max', 'mdev', '=', '0.189', '0.195', '0.202', '0.015', 'ms']
            ping_result = re.split(' |/', ping_result)

            rtt = ping_result[INDEX_AVG_RTT]

    except subprocess.CalledProcessError as e:
        logger.error('CalledProcessError: ' + e.message)
    except Exception as e:
        logger.error('Unknown error: ' + e.message)

    print rtt  # 'ms'


class ping_ssh_Thread(threading.Thread):
    def __init__(self, logg, myhost):
        if logg is not None:
            logger = logg
        loggg = logging.getLogger('Ping_Thread')

        # check myhost
        # INDEX:    [ip[rtt,cpu,men]]
        if len(myhost) == 0:
            raise Exception('No host to ping')
        # Ensure that when the main program exits the thread will also exit
        self.daemon = True
        self.setDaemon(True)

    def run(self):
        time.sleep(5)
