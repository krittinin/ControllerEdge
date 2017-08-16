import SocketServer
import logging
import threading
import yaml
import sudoku
import os
import time
import sys

buffer_size = 1024

# logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')
global total_workload
total_workload = 0

# Seperated thread for each request
# Asynchronous_Requests, Fork process when get a new request

SEPERATOR = ','

streamformat = "%(asctime)s %(name)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.DEBUG,
                    format=streamformat, filename='./log/server_' + time.strftime("%m%d-%H%M") + '.log',
                    filemode='w')


def read_file(input_file):
    read_data = None
    if not os.path.isfile(input_file):
        logger.error('cannot find ' + input_file + ' in the directory')
        return False, read_data
    try:
        with open(input_file, 'r') as f:
            # f = open(input_file, 'r')
            read_data = f.read()
            f.close()
    except:
        logger.error('cannot read file: ' + input_file)
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


class ProcessTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(buffer_size)
        pid = str(os.getpid())
        # print 'PID' + pid + ': receive message from ' + self.client_address[0]
        logger.debug('PID' + pid + ': receive message from ' + self.client_address[0])
        data = str(data).split(SEPERATOR)
        wid = data[0]
        puzzle = data[1]
        solved_puzzle = sudoku.solve(puzzle)
        solved_puzzle = wid + SEPERATOR + solved_puzzle
        self.request.sendall(solved_puzzle)
        global total_workload
        total_workload += 1
        logger.debug('PID' + pid + ': ' + wid + ' job is done. send the solution')


class ProcessTCPServer(SocketServer.ForkingMixIn, SocketServer.TCPServer):
    pass


if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port

    logging.info('Loading configuration')

    config_file = 'server.cfg'
    config_is_ok, config = load_config(config_file)
    if not config_is_ok:
        logging.error('load config fail!! exit the program')
        exit(1)
    logger = logging.getLogger(config['host_name'])
    logger.setLevel(getattr(logging, config['file_log_level']))

    console = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(fmt=streamformat)
    console.setFormatter(formatter)
    console.setLevel(getattr(logging, config['console_log_level']))

    logger.addHandler(console)

    server_ip = config['server_ip']
    server_port = config['server_port']

    logger.info('Start service..')

    server = ProcessTCPServer((server_ip, server_port), ProcessTCPRequestHandler)
    server_ip, server_port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)

    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()

    logger.info('Server is ready')

    try:
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        pass

    logger.debug('Server is shutting down')
    server.shutdown()
    server.server_close()
    print 'Total workloads =', total_workload
    logger.info('Exit server')
    exit()
