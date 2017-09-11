import base64
import linecache
import random
import socket
import threading
import time
import collections
import yaml
import sys
import logging
import os
import re
from glob import glob1
from Sudoku import sudoku

max_latency = 1.5  # second

total_request = 0
total_reject = 0
total_error = 0
buffer_size = 2048
socket_timeout = 15

streamformat = "%(asctime)s %(name)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.DEBUG,
                    format=streamformat, filename='./log/source_' + time.strftime("%m%d-%H%M") + '.log',
                    filemode='w')

logger = logging.getLogger("Source")

# Constant variables
ERR_MSG = 'Error'
ACPT_MSG = 'Accept'
REJ_MSG = 'Reject'
TEST_MODE = 'test'

SEPERATOR = ','

lock = threading.Condition()
flag_condition = True
END_FLAG = 'END'

CLT_INT_FLAG = 'CLT_INI_SETUP'

SERVICE_NAME = 'Sudoku_Service'
# puzzle mode
FILE_MODE = 'FILE'
GENERATOR_MODE = 'GENERATOR'
FILE_EOF = 200
EXP_SEED = 2017
IMG_SEED = 2560

exp_random = random.Random(EXP_SEED)
puzzle_random = random.Random(IMG_SEED)


def check_exist_file(file):
    if not os.path.isfile(file):
        logger.error('cannot find {} in the directory'.format(file))
        return False
    return True


def read_file(input_file):
    read_data = None
    if not check_exist_file(input_file):
        return False, read_data
    try:
        with open(input_file, 'r') as f:
            # f = open(input_file, 'r')
            read_data = f.read()
            f.close()
    except:
        logger.error('cannot read file: {}'.format(input_file))
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
            "Error loading configuration file \'{}\' {}:content format error: Failed to parse yaml format".format(
                config_file, error_pos))
        return False, config

    return True, config


def send_intial_setup_to_ctrl(controller_ip, controller_port):
    int_msg = CLT_INT_FLAG
    host, port = controller_ip, controller_port
    policy, ctrl_interval = ERR_MSG, -1
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.settimeout(1)
        try:
            sock.sendall(int_msg)
            response = sock.recv(buffer_size)
        finally:
            sock.close()
        response = str(response).split(SEPERATOR)
        if len(response) == 2:
            policy, ctrl_interval = response[0], response[1]
    except socket.timeout:
        logger.error("Socket timeout " + host + " " + str(port))
    except socket.error as e:
        logger.error("Socket error " + host + ": " + e.message)

    return policy, ctrl_interval


def send_message_to_ctrl(max_latency, work_id, puzzle, controller_ip, controller_port):
    '''
    Send request message to controller and wait for reply
    :param max_latency:
    :param work_id:
    :param puzzle:
    :param controller_ip:
    :param controller_port:
    :return:[Accept, work_id, edge_name, edge_ip, edge_port]
    '''
    # ask_msg = id,max_latency,size-of_puzzle
    ask_msg = SEPERATOR.join((str(work_id), str(max_latency), str(sys.getsizeof(puzzle))))

    host, port = controller_ip, controller_port
    response = ERR_MSG
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.settimeout(1)
        try:
            sock.sendall(ask_msg)
            response = sock.recv(buffer_size)
        finally:
            sock.close()
    except socket.timeout:
        logger.error("Socket timeout " + host + " " + str(port))
    except socket.error as e:
        logger.error("Socket error " + host + ": " + e.message)

    ctrl_rep = re.split(SEPERATOR, response)

    # ctrl_rep = [Accept, work_id, edge_name, edge_ip, edge_port, comm, comp]
    if len(ctrl_rep) != 7:
        ctrl_rep = [ERR_MSG, work_id, '', '', '', '', '0', '0']
        logger.error("Controller response error")
    return ctrl_rep[0], ctrl_rep[1], ctrl_rep[2], ctrl_rep[3], ctrl_rep[4], ctrl_rep[5], ctrl_rep[6]


def get_guzzle(line):
    if puzzle_mode == FILE_MODE:
        # l = puzzle_random.randint(1, FILE_EOF)
        puzzle = linecache.getline(puzzle_file, line).strip()
        puzzle = puzzle.replace(',', '\n')
    else:
        puzzle = sudoku.make_puzzle(puzzle_size)
    return puzzle


def send_workload_to_edge(server_ip, server_port, wid, puzzle):
    status, wk_id, solved_puzzle, computing_time = ERR_MSG, -1, 0, 0
    resp = ERR_MSG
    host, port = server_ip, server_port
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.settimeout(max_latency * 2 if max_latency > 1 else 2)

        # send [service,id,puzzle,'END']
        message = SEPERATOR.join((service, str(wid), puzzle, END_FLAG))
        try:
            sock.sendall(message)
            resp = sock.recv(buffer_size)
        finally:
            sock.close()

    except socket.timeout:
        logger.error("Socket timeout " + host + " " + str(port))
    except socket.error as e:
        logger.error("Socket error " + host + ": " + e.message)

    if resp != ERR_MSG:
        # reply_msg = [work_id, result, computing_time]
        resp = resp.split(SEPERATOR)
        try:
            assert len(resp) == 3, ''
            status = 'Done'
            wk_id = int(resp[0])
            solved_puzzle = resp[1]
            computing_time = float(resp[2])
        except ValueError:
            logger.error('Cannot get values from reply msg: ' + str(resp))
            status, wk_id, solved_puzzle, computing_time = ERR_MSG, -1, 0, 0
        except AssertionError:
            logger.error('Reply message error: ' + str(resp))

    return status, wk_id, solved_puzzle, computing_time


class SourceThread(threading.Thread):
    def __init__(self, id, cip, cport, d_sip, d_sport, maxlatency, mmode):
        threading.Thread.__init__(self)
        self.thread_id = id
        self.ctrl_ip = cip
        self.ctrl_port = cport
        self.default_server_name = 'default'
        self.default_server_ip = d_sip
        self.default_server_port = d_sport
        self.max_latency = maxlatency
        self.test_mode = mmode == TEST_MODE  # (m == 'test')

    def run(self):
        puzzle_line = puzzle_random.randint(1, FILE_EOF)
        puzzle = get_guzzle(line=puzzle_line)
        if not self.test_mode:
            accept, wk_id, server_name, server_ip, server_port, est_commu, est_comp = send_message_to_ctrl(
                max_latency=self.max_latency,
                work_id=self.thread_id,
                puzzle=puzzle,
                controller_ip=self.ctrl_ip,
                controller_port=self.ctrl_port)
            if accept == ACPT_MSG:
                server_port = int(server_port)
                wk_id = int(wk_id)
            est_commu = float(est_commu)
            est_comp = float(est_comp)

        else:
            accept, wk_id, server_name, server_ip, server_port, est_commu, est_comp = ACPT_MSG, self.thread_id, self.default_server_name, self.default_server_ip, self.default_server_port, -1, -1

        is_reject, is_error = False, False
        logger.debug('%s, wk_%d(%s) by %s (%s): %.6f+%.6f ' % (
            accept, int(wk_id), puzzle_line, server_name, server_ip, est_commu, est_comp))
        if accept == ERR_MSG:
            is_error = True
        elif accept == REJ_MSG:
            is_reject = True
        elif accept == ACPT_MSG:
            send_time = time.time() if sys.platform is not 'win32' else time.clock()
            status, wid, recognition_result, compu_latency = send_workload_to_edge(server_ip, server_port,
                                                                                   wk_id, puzzle)
            receive_time = time.time() if sys.platform is not 'win32' else time.clock()
            total_latency = receive_time - send_time
            # compu_latency = finish_time - arrive_time
            commu_latency = total_latency - compu_latency

            if total_latency > self.max_latency or status == ERR_MSG:
                is_error = True

            logger.debug('Receive wk_%d, %.6f seconds, comm%.6f+comp%.6f, %s, (puzzle no.%s)' % (
                self.thread_id, total_latency, commu_latency, compu_latency, status, puzzle_line))

        global total_request, total_reject, total_error, flag_condition
        lock.acquire()
        if flag_condition:
            flag_condition = False
            total_request += 1
            # print total_request
            if is_reject:
                total_reject += 1
            if is_error:
                total_error += 1
            flag_condition = True
            lock.notifyAll()
        else:
            lock.wait()
        lock.release()


if __name__ == "__main__":
    # --------------Load configuration---------------
    config_file = 'source.cfg'
    config_is_ok, config = load_config(config_file)
    if not config_is_ok:
        logger.error('load config fail!! exit the program')
        exit(1)

    logger.setLevel(getattr(logging, config['log_level']))
    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(getattr(logging, config['console_log_level']))
    formatter = logging.Formatter(fmt=streamformat)
    console.setFormatter(formatter)

    logger.addHandler(console)

    controller_ip = str(config['controller_ip'])
    controller_port = int(config['controller_port'])

    mode = config['mode']

    puzzle_mode = config['puzzle_mode']
    puzzle_file = config['puzzle_file']
    puzzle_size = int(config['puzzle_size'])

    if not check_exist_file(puzzle_file):
        puzzle_mode = GENERATOR_MODE
        logger.warning('{} not exist, change to generator mode'.format(puzzle_file))
    else:
        # -----get file eof--------
        with open(puzzle_file) as f:
            FILE_EOF = sum(1 for _ in f)

    if mode == 'test': logger.warning('Test mode')
    if puzzle_mode == GENERATOR_MODE: logger.warning('Use puzzle generator, may delay the program')

    server_ip = config['default_server_ip']
    server_port = config['default_server_port']
    # --------------Finish configuration---------------

    # ---------------Initial parameter-----------------
    # max_latency, num_of_workloads, work_rate
    len_arg = len(sys.argv)
    num_of_workloads, wk_rate, service = 0, 0, SERVICE_NAME
    policy, interval = None, None
    try:
        assert len_arg == 3 + 1, 'Please insert correct parameters: ' \
                                 'max_latency, num_of_workloads, work_rate'
        max_latency = float(sys.argv[1])
        num_of_workloads = int(sys.argv[2])
        wk_rate = float(sys.argv[3])  # per second
        assert max_latency > 0 and num_of_workloads >= 0 and wk_rate > 0, 'Please insert correct parameters: ' \
                                                                          'max_latency, num_of_workloads, work_rate'

        # connect to controller
        if mode == 'test':
            policy, interval = 'Not connect', -1
        else:
            policy, interval = send_intial_setup_to_ctrl(controller_ip, controller_port)
        assert policy is not ERR_MSG, 'Error to connect to the controller'

    except Exception as e:
        logger.error('Parameter error: ' + e.message)
        exit(1)

    # ---------------Finish initializing parameter-----------------
    logger.info('MAX_LEN={0[1]}, NUM_OF_WK={0[2]}, lambda={0[3]}, SERVICE={1}'.format(sys.argv, service))
    logger.info('CTRL: POLICY={}, INTERVAL={}'.format(policy, interval))

    threads = collections.deque(maxlen=(num_of_workloads / 2))

    try:
        for i in range(num_of_workloads):
            client = SourceThread(i, controller_ip, controller_port, server_ip, server_port, max_latency,
                                  mode)
            client.daemon = True
            client.start()
            threads.append(client)
            next_workload = exp_random.expovariate(wk_rate)  # random.expovariate(wk_rate)  # as poisson process
            logger.debug("Next wk_%d: %.6f seconds" % ((i + 1), next_workload))

            # if len(threads) >= max_thread_list:
            #    logger.warning('Threads reach the max size.')
            time.sleep(next_workload)
    except KeyboardInterrupt, SystemExit:
        logger.info('Exit the program')

    logger.debug('Wait for all threads')
    for c in threads:
        c.join()

    time.sleep(1)

    # print "total requests:", total_request
    # print "total rejections:", total_reject
    # print "total errors:", total_error
    logger.info('MAX_LEN={0[1]}, NUM_OF_WK={0[2]}, lambda={0[3]}, SERVICE={1}'.format(sys.argv, service))
    logger.info('CTRL: POLICY={}, INTERVAL={}'.format(policy, interval))
    result_text = 'total={}, reject={}, error={}'.format(total_request, total_reject, total_error)
    logger.info(result_text)
    logger.info('Source exit')
    exit()
