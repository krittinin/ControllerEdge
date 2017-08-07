# /urs/bin/python

import SocketServer
import logging
import threading
import time

'''
Main controller to interact with sources and openvim
'''

# assume host is VM host1

global logger
# logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s', filename=time.strftime('%y%m%d_%H%M', time.localtime()) + '.log')

logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')

buffer_size = 1024


# TODO: create thread waiting for sources' request
class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(buffer_size)
        cur_thread = threading.current_thread()
        response = '{}: {}'.format(cur_thread.name, data)
        logger = logging.getLogger(cur_thread.name)
        logger.debug('recv() from ' + self.client_address[0])

        # TODO: implement algo. of selecting host
        self.request.sendall(response)


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


# TODO: send API to openvim to look up host

# TODO: ssh to host for update info (openvim)?

# TODO: measure RTT to host


# TODO: list and find IP of VM in host


_serverAddress = 'localhost'  # host ip
_port = 12076  # fix port

if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    # HOST, PORT = "localhost", 0
    HOST = _serverAddress
    PORT = _port

    logger = logging.getLogger('Controller')

    logger.debug('Start thread waiting request for sources')

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    ip, port = server.server_address
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    logger.debug('Server loop running in thread' + server_thread.name)

    try:
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        pass

    logger.debug('Controller is down')
    server.shutdown()
    server.server_close()
