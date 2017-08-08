# /urs/bin/python

import SocketServer
import logging
import threading
import time
from Host_Object import HostInfo as Host

'''
Main controller to interact with sources and openvim
'''

# assume host is VM host1

global logger

# logging.basicConfig(level=logging.ERROR, format='%(name)s: %(message)s')

buffer_size = 1024


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



# TODO: list and find IP of VM in host


_serverAddress = 'localhost'  # host ip
_port = 12076  # fix port

if __name__ == "__main__":

    streamformat = "%(asctime)s %(name)s %(levelname)s: %(message)s"
    # logging.basicConfig(format=streamformat, level=logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG, format=streamformat,
                        filename=time.strftime('%y%m%d_%H%M', time.localtime()) + '.log')

    logger = logging.getLogger('Controller')
    logger.setLevel(logging.DEBUG)

    # Port 0 means to select an arbitrary unused port
    # HOST, PORT = "localhost", 0
    HOST = _serverAddress
    PORT = _port

    # Create host
    host1 = Host('131.112.21.86', 'host', '123qweasd')
    host1.update()
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
            time.sleep(10)
            host1.update()
    except (KeyboardInterrupt, SystemExit):
        pass

    logger.debug('disconnect host(s)')

    host1.close()

    logger.debug('shut down a server')
    server.shutdown()
    server.server_close()

    logger.debug('Exit')
    exit()
