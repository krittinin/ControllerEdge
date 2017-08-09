# /urs/bin/python

import SocketServer
import logging
import threading
import time
import yaml

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


def read_file(file):
    read_data = None
    if not os.path.isfile(file):
        logger.error('cannot find ' + file + ' in the directory')
        return False, read_data
    try:
        f = open(file, 'r')
        read_data = f.read()
        f.close()
    except:
        logger.error('cannot read file: ' + file)
        return False, read_data

    return True, read_data


def load_config(config_file):
    # read config_file to string
    config = {}
    readable, read_data = read_file(config_file)
    if not readable: return False, config
    try:
        config = yaml.load(read_data)
    except yaml.YAMLError, exc:
        error_pos = ""
        if hasattr(exc, 'problem_mark'):
            mark = exc.problem_mark
            error_pos = " at position: (%s:%s)" % (mark.line + 1, mark.column + 1)
        logger.error(
            "Error loading configuration file \'"
            + config_file + "\'" + error_pos
            + ": content format error: Failed to parse yaml format")
        return False, config

    return True, config


def load_hostlist(host_file):
    host_list = []

    readable, read_data = read_file(host_file)
    if not readable: return False, host_list

    try:
        h_list = yaml.load(read_data)
    except yaml.YAMLError, exc:
        error_pos = ""
        if hasattr(exc, 'problem_mark'):
            mark = exc.problem_mark
            error_pos = " at position: (%s:%s)" % (mark.line + 1, mark.column + 1)
        logger.error(
            "Error loading host file \'"
            + host_file + "\'" + error_pos
            + ": content format error: Failed to parse yaml format")
        return False, host_list

    for n, h_data in h_list.items():
        try:
            h = Host(h_data['name'], h_data['host-ip'], h_data['host-user'], h_data['host-pwd'])
        except:
            logger.error('Cannot add host: ' + h_data['name'])
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
    logging.basicConfig(level=logging.DEBUG,
                        format=streamformat,
                        filename='./log/controller_' + time.strftime("%m%d-%H%M") + '.log',
                        filemode='w')

    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt=streamformat)
    console.setFormatter(formatter)

    logger = logging.getLogger('Controller')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console)

    logger.info('load configuration...')
    config_file = 'controller.cfg'
    config_is_ok, config = load_config(config_file)
    if not config_is_ok:
        logger.info('load config fail!! exit the controller')
        exit(1)
    # print config['controller_ip'], config['controller_port']

    console.setLevel(getattr(logging, config['console_log_level']))
    logger.setLevel(getattr(logging, config['file_log_level']))

    # Port 0 means to select an arbitrary unused port
    # HOST, PORT = "localhost", 0
    # HOST = _serverAddress
    # PORT = _port

    # Create host list
    logger.info('Connecting host(s)...')
    host_file = 'host_file.yaml'
    host_is_ok, host_list = load_hostlist(host_file)
    if not host_is_ok:
        logger.warning('No host is running')
    time.sleep(1)

    logger.debug('Start sever_thread waiting requests for sources')
    server = ThreadedTCPServer((config['controller_ip'], config['controller_port']), ThreadedTCPRequestHandler)
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
