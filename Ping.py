import threading
import logging
import paramiko

# TODO: make it as a class


# create ping command
# parameters = "-n %d -l %d -w %f" % (count,pck_size,timeout*1000) if system_name().lower()=="windows" else "-c %d -s %d -W %f" % (count,pck_size,timeout)
# command = 'ping '+host+' '+parameters
import time

global logger


class pingSshThread(threading.Thread):
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