import SocketServer
import logging
import threading

buffer_size = 1024

logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')


# Seperated thread for each request


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(buffer_size)
        cur_thread = threading.current_thread()
        response = '{}: {}'.format(cur_thread.name, data)
        logger = logging.getLogger(cur_thread.name)
        logger.debug('recv() from ' + self.client_address[0])

        # TODO: Select host
        self.request.sendall(response)


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


_serverAddress = '131.112.21.86'
_port = 12123

if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    # HOST, PORT = "localhost", 0
    HOST = _serverAddress
    PORT = _port

    logger = logging.getLogger('Main server')

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request

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

    logger.debug('Server is down')
    server.shutdown()
    server.server_close()
