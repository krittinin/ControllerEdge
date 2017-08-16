import random
import socket
import threading
import time

import collections
import yaml
import sys
import sudoku
import logging
import os
import re

max_latency = 1.5  # second

total_request = 0
total_reject = 0
total_error = 0
buffer_size = 1024
socket_timeout = 15

streamformat = "%(asctime)s %(name)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.DEBUG,
                    format=streamformat)

logger = logging.getLogger("Source")

# Constant variables
ERR_MSG = 'Error'
ACPT_MSG = 'Accept'
REJ_MSG = 'Reject'
TEST_MODE = 'test'

SEPERATOR = ','

lock = threading.Condition()
flag_condition = True


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


def send_message(host, port, message):
    response = ERR_MSG
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.settimeout(60)
        try:
            sock.sendall(message)
            response = sock.recv(buffer_size)
        finally:
            sock.close()
    except socket.timeout:
        logger.error("Socket timeout " + host, port)
    except socket.error as e:
        logger.error("Socket error " + host + ": " + e.message)

    return str(response)


def get_server_from_ctrl(max_latency, work_id, puzzle, controller_ip, controller_port):
    # ask_msg = id,max_latency,size-of_puzzle
    ask_msg = str(work_id) + SEPERATOR + str(max_latency) + SEPERATOR + str(sys.getsizeof(puzzle))
    ctrl_rep = send_message(controller_ip, controller_port, ask_msg)
    ctrl_rep = re.split(SEPERATOR, ctrl_rep)
    # ctrl_rep = [Accept, work_id, edge_ip, edge_port]
    if len(ctrl_rep) != 4:
        ctrl_rep = [ERR_MSG, work_id, '', '', '']
        logger.error("Controller response error")
    return ctrl_rep[0], ctrl_rep[1], ctrl_rep[2], ctrl_rep[3]


class sourceThread(threading.Thread):
    def __init__(self, id, cip, cport, d_sip, d_sport, pzzsize, maxlatency, mmode):
        threading.Thread.__init__(self)
        self.thread_id = id
        self.ctrl_ip = cip
        self.ctrl_port = cport
        self.default_server = d_sip
        self.default_server_port = d_sport
        self.max_latency = maxlatency
        self.puzzle_size = pzzsize
        self.test_mode = mmode == TEST_MODE  # (m == 'test')

    def run(self):
        puzzle = sudoku.make_puzzle(self.puzzle_size)  # get a sudoku puzzzle
        puzzle = str(self.thread_id) + SEPERATOR + puzzle.strip()

        if not self.test_mode:
            accept, wk_id, server_ip, server_port = get_server_from_ctrl(max_latency=self.max_latency,
                                                                         work_id=self.thread_id,
                                                                         puzzle=puzzle,
                                                                         controller_ip=self.ctrl_ip,
                                                                         controller_port=self.ctrl_port)
            server_port = int(server_port)
            wk_id = int(wk_id)
        else:
            accept, wk_id, server_ip, server_port = ACPT_MSG, self.thread_id, self.default_server, self.default_server_port

        is_reject, is_error = False, False

        logger.debug(accept + ",wk" + str(wk_id) + "," + str(server_ip) + "," + str(server_port))

        if accept == REJ_MSG:
            is_reject = True
        elif accept == ACPT_MSG:
            t1 = time.time()
            solved_puzzle = send_message(server_ip, server_port, str(puzzle))
            t2 = time.time()
            solved_puzzle = solved_puzzle.split(SEPERATOR)
            # print solved_puzzle[0] + '=\n' + solved_puzzle[1]
            diff_t = t2 - t1
            logger.debug('Done workload_' + str(self.thread_id) + SEPERATOR + str(diff_t))
            if diff_t > self.max_latency or solved_puzzle == ERR_MSG:
                is_error = True

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

    controller_ip = str(config['controller_ip'])
    controller_port = int(config['controller_port'])
    # --------------Finish configuration---------------

    # ---------------Initial parameter-----------------
    # max_latency, num_of_workloads, work_rate
    len_arg = len(sys.argv)
    try:
        assert len(sys.argv) == 3 + 1, 'Please insert correct parameters'
        max_latency = float(sys.argv[1])
        num_of_workloads = int(sys.argv[2])
        wk_rate = float(sys.argv[3])  # per second
        assert max_latency > 0 and num_of_workloads >= 0 and wk_rate > 0, 'Please insert correct parameters'
    except Exception as e:
        logger.error('Parameter error: ' + e.message)
        exit(1)
    # ---------------Finish initialinging parameter-----------------

    puzzle_size = int(config['puzzle_size'])
    mode = config['mode']
    is_test_mode = False
    if mode == TEST_MODE:
        is_test_mode = True

    server_ip = config['default_server_ip']
    server_port = config['default_server_port']

    # server_ip, server_port = '131.112.21.86', 12541

    threads = collections.deque(maxlen=100)

    try:
        for i in range(num_of_workloads):
            client = sourceThread(i, controller_ip, controller_port, server_ip, server_port, puzzle_size, max_latency,
                                  mode)
            client.daemon = True
            client.start()
            threads.append(client)
            next_workload = random.expovariate(wk_rate)  # as poisson process
            logger.debug('workload_' + str(i + 1) + ': ' + str(next_workload))
            time.sleep(next_workload)

    except KeyboardInterrupt, SystemExit:
        logger.info('Exit the program')

    logger.debug('Wait for all threads')
    for c in threads:
        c.join()

    print "total request:", total_request
    print "total rejection:", total_reject
    print "total error", total_error
    print 'Exit source'
    exit()
