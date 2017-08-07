import subprocess
from platform import system as system_name

host = '131.112.21.80'
count = 2
timeout = 1
pck_size = 1024

# create ping command
# parameters = "-n %d -l %d -w %f" % (count,pck_size,timeout*1000) if system_name().lower()=="windows" else "-c %d -s %d -W %f" % (count,pck_size,timeout)
# command = 'ping '+host+' '+parameters


ping = subprocess.check_output(['ping', host, '-c', str(count), '-s', str(pck_size), '-W', str(timeout)]).splitlines()

ping_result = None

if len_ping > 0:
    ping_result = ping[len_ping - 1]

print ping_result
