# /urs/bin/python

import SocketServer
import logging
import threading
import time
from Host_Object import HostInfo as Host
import os
import sys

'''
Main controller to interact with sources and openvim
'''

# assume host is VM host1

global logger

# logging.basicConfig(level=logging.ERROR, format='%(name)s: %(message)s')

buffer_size = 1024
_controller_interval = 10  # second
_policy = 1  # 1: random, 2: lowest latency


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(buffer_size)
        cur_thread = threading.current_thread()
        response = '{}: {}'.format(cur_thread.name, data)
        # logger = logging.getLogger(cur_thread.name)
        logger.debug('recv() from ' + self.client_address[0])

        # TODO: implement algo. of selecting host
        lock_shared_resource.acquire()
        try:
            self.request.sendall(response)
        finally:
            lock_shared_resource.release()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


def loadHostList(host_file):
    if not os.path.isfile(host_file):
        logger.error('Host file is not existed: ' + host_file)
        return False, {}

    host_list = []
    with open(host_file) as file:
        for line in file:
            read = line.strip().split()
            try:
                # [0]    [1]     [2]
                # ip     usr     pwd
                h = Host(read[0], read[1], read[2])
            except:
                logger.error('Cannot add host')
                continue
            host_list.append(h)
    return True, host_list


# TODO: send API to openvim to look up host



# TODO: list and find IP of VM in host


_serverAddress = '131.112.21.86'  # host ip
_port = 12123  # fix port

if __name__ == "__main__":

    lock_shared_resource = threading.RLock()

    # Handle logging
    streamformat = "%(asctime)s %(name)s %(levelname)s: %(message)s"
    # logging.basicConfig(format=streamformat, level=logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG, format=streamformat, filename='controller_log.log', filemode='w')

    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt=streamformat)
    console.setFormatter(formatter)

    logger = logging.getLogger('Controller')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console)

    # Port 0 means to select an arbitrary unused port
    # HOST, PORT = "localhost", 0
    HOST = _serverAddress
    PORT = _port

    # Create host list
    logger.info('Connecting host(s)...')
    host_file = 'host_file.txt'
    isSuccess, host_list = loadHostList(host_file)
    time.sleep(1)

    logger.debug('Start sever_thread waiting requests for sources')
    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    ip, port = server.server_address
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    logger.debug('Server loop running in thread' + server_thread.name)
    logger.info('Controller is ready')

    try:
        while True:
            time.sleep(_controller_interval)
            logger.info('Update host(s)')
            lock_shared_resource.acquire()
            try:
                for h in host_list:
                    h.update()
            finally:
                lock_shared_resource.release()
    except (KeyboardInterrupt, SystemExit):
        pass

    logger.info('Disconnecting host(s)...')

    for h in host_list:
        h.close()

    logger.debug('Shut down a server')
    server.shutdown()
    server.server_close()

    logger.info('Exit')
    exit()
