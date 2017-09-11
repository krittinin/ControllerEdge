import SocketServer
import logging
import threading
import yaml
import os
import time
import sys
import sudoku

buffer_size = 2048

global total_workload
total_workload = 0

SEPERATOR = ','

streamformat = "%(asctime)s %(name)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.DEBUG,
                    format=streamformat, filename='./log/server_' + time.strftime("%m%d-%H%M") + '.log',
                    filemode='w')

SERVICE_NAME = 'Sudoku_Service'

logger = logging.getLogger('root')

END_FLAG = 'END'


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
        arrive_time = time.time() if sys.platform is not 'win32' else time.clock()
        pid = str(os.getpid())
        puzzle_data = ''
        try:

            while True:
                data = self.request.recv(buffer_size)
                puzzle_data = puzzle_data + str(data)
                if not data or str(data).endswith(SEPERATOR + END_FLAG):
                    break

            # recv [service,id,puzzle,'END']
            puzzle_data = puzzle_data.split(SEPERATOR)
            assert len(puzzle_data) == 4, 'Receive err message'
            service = puzzle_data[0]
            wid = puzzle_data[1]
            puzzle_data = puzzle_data[2]

            source = self.client_address[0]
            logger.debug('PID' + pid + ': receive message from ' + source)

            solved_puzzle = sudoku.solve(puzzle_data)

            finish_time = time.time() if sys.platform is not 'win32' else time.clock()
            comp = finish_time - arrive_time

            # reply_msg = [work_id, result, computing_time]
            resp = SEPERATOR.join((wid, solved_puzzle, str(comp)))

            self.request.sendall(resp)
            logger.debug('PID{}: {} job is done.'.format(pid, wid))

        except Exception as e:
            logger.error('PID{}: {}'.format(pid, e.message))


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

    user = config['user']
    server_ip = config['server_ip']
    server_port = config['server_port']

    logger.info('Start service..%s' % SERVICE_NAME)

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
    # print 'Total workloads =', total_workload
    logger.info('Exit server')
    exit()
