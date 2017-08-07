import subprocess
from sys import maxint
import re

# TODO: make it as a class

# TODO: parameters
host = '131.112.21.80'
count = 2
timeout = 1
pck_size = 1024

# create ping command
# parameters = "-n %d -l %d -w %f" % (count,pck_size,timeout*1000) if system_name().lower()=="windows" else "-c %d -s %d -W %f" % (count,pck_size,timeout)
# command = 'ping '+host+' '+parameters

ping_result = None
rtt = maxint

try:
    ping = subprocess.check_output(
        ['ping', host, '-c', str(count), '-s', str(pck_size), '-W', str(timeout)]).splitlines()

    len_ping = len(ping)

    if len_ping > 0:
        ping_result = ping[len_ping - 1]

    if len(ping_result) != 0:
        # ['rtt', 'min', 'avg', 'max', 'mdev', '=', '0.189', '0.195', '0.202', '0.015', 'ms']
        ping_result = re.split(' |/', ping_result)

        rtt = ping_result[7]

except subprocess.CalledProcessError as e:
    print 'CalledProcessError: ' + e.message
except Exception as e:
    print 'Unknown error: ' + e.message

print rtt, 'ms'
