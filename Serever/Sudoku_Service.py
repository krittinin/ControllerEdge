import SocketServer
import logging
import threading
import yaml
import sudoku
import os
import time
import sys
from host_httpserver import HostHTTPThread

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

RECORD_LATENCY_FILE = 'record.txt'
SERVICE_NAME = 'Sudoku_Service'
http_flag = False


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


def record_file_format(comp, p_num):
    return 'comp: {}\np-num: {}\nsources:\n'.format(comp, p_num)


def record_file_source_format(s, ip, rtt, s_num):
    return ' {}:\n  ip: {}\n  rtt: {}\n  s-num: {}\n'.format(s, ip, rtt, s_num)


def record_workload_in_file(source, comm, comp, file_name):
    with open(file_name, 'r') as f:
        read_data = f.read()

    new_source = source not in read_data

    data = yaml.load(read_data)

    # data  = {comp: 0.0, p-num: 0, sources: {s1:{ip: '120', rtt: 0.0, s-num: 0}, s2: {} } }
    p_num = data['p-num'] + 1
    avg_comp = (data['comp'] * data['p-num'] + comp) / p_num
    w = record_file_format(avg_comp, p_num)
    if new_source:
        if data['sources'] is not None:
            for s, s_data in data['sources'].items():
                w += record_file_source_format(s, s_data['ip'], s_data['rtt'], s_data['s-num'])
        w += record_file_source_format('s{}'.format(
            len(data['sources']) if data['sources'] is not None else 0), source, comm, 1)
    else:
        for s, s_data in data['sources'].items():
            if s_data['ip'] == source:
                s_num = s_data['s-num']
                avg_comm = (s_data['rtt'] * s_num + comm) / (s_num + 1)
                s_num += 1
                w += record_file_source_format(s, s_data['ip'], avg_comm, s_num)
            else:
                w += record_file_source_format(s, s_data['ip'], s_data['rtt'], s_data['s-num'])

    with open(file_name, 'w') as f:
        f.write(w)


class ProcessTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        arrive_time = time.time()
        data = self.request.recv(buffer_size)
        pid = str(os.getpid())
        # print 'PID' + pid + ': receive message from ' + self.client_address[0]
        source = self.client_address[0]
        logger.debug('PID' + pid + ': receive message from ' + source)

        # msg = wid, puzzle, send-time
        try:
            data = str(data).split(SEPERATOR)
            assert len(data) == 3, 'Receive err message'
            wid = data[0]
            puzzle = data[1]
            send_time = float(data[2])
            solved_puzzle = sudoku.solve(puzzle)
            finish_time = time.time()

            # reply_msg = [arrive_time, finish_time, work_id, solution]
            resp = str(arrive_time) + SEPERATOR + str(finish_time) + SEPERATOR + wid + SEPERATOR + solved_puzzle

            self.request.sendall(resp)
            logger.debug('PID{}: {} job is done. send the solution'.format(pid, wid))

            # global total_workload
            # total_workload += 1
            if http_flag:
                record_workload_in_file(source, arrive_time - send_time, finish_time - arrive_time, RECORD_LATENCY_FILE)

        except yaml.YAMLError:
            logger.error('PID{} cannot update {}'.format(pid, RECORD_LATENCY_FILE))
        except IOError:
            logger.error('{} not found'.format(RECORD_LATENCY_FILE))
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
    http_port = config['http_port']

    # read parameter
    assert len(sys.argv) <= 2, 'Please insert [-http] if you want to run http server'
    http_flag = False
    http_thread = None
    if len(sys.argv) > 1 and sys.argv[1] == '-http':
        # Start HTTP server
        http_flag = True
        with open(RECORD_LATENCY_FILE, 'w') as f:
            f.write(record_file_format(0.0, 0))
        http_thread = HostHTTPThread(user, server_ip, http_port, SERVICE_NAME)
        http_thread.daemon = True
        http_thread.start()

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
    if http_flag: http_thread.close()
    print 'Total workloads =', total_workload
    logger.info('Exit server')
    exit()
