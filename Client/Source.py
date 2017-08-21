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
import linecache

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

FILE_EOF = 200

lock = threading.Condition()
flag_condition = True


def read_file(input_file):
    read_data = None
    if not os.path.isfile(input_file):
        logger.error('cannot find {} in the directory'.format(input_file))
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


def send_message(host, port, message):
    response = ERR_MSG
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.settimeout(max_latency * 2)
        try:
            sock.sendall(message)
            response = sock.recv(buffer_size)
        finally:
            sock.close()
    except socket.timeout:
        logger.error("Socket timeout " + host + " " + str(port))
    except socket.error as e:
        logger.error("Socket error " + host + ": " + e.message)

    return str(response)


def get_server_from_ctrl(max_latency, work_id, puzzle, controller_ip, controller_port):
    # ask_msg = id,max_latency,size-of_puzzle
    ask_msg = str(work_id) + SEPERATOR + str(max_latency) + SEPERATOR + str(sys.getsizeof(puzzle))
    ctrl_rep = send_message(controller_ip, controller_port, ask_msg)
    ctrl_rep = re.split(SEPERATOR, ctrl_rep)
    # ctrl_rep = [Accept, work_id, edge_name, edge_ip, edge_port]
    if len(ctrl_rep) != 5:
        ctrl_rep = [ERR_MSG, work_id, '', '', '', '']
        logger.error("Controller response error")
    return ctrl_rep[0], ctrl_rep[1], ctrl_rep[2], ctrl_rep[3], ctrl_rep[4]


def get_guzzle():
    if puzzle_mode == 'file':
        l = random.randint(1, FILE_EOF)
        line = linecache.getline(puzzle_file, l).strip()
        line = line.replace(',', '\n')
    else:
        line = sudoku.make_puzzle(puzzle_size)
    return line


class sourceThread(threading.Thread):
    def __init__(self, id, cip, cport, d_sip, d_sport, pzzsize, maxlatency, mmode):
        threading.Thread.__init__(self)
        self.thread_id = id
        self.ctrl_ip = cip
        self.ctrl_port = cport
        self.default_server_name = 'default'
        self.default_server_ip = d_sip
        self.default_server_port = d_sport
        self.max_latency = maxlatency
        self.puzzle_size = pzzsize
        self.test_mode = mmode == TEST_MODE  # (m == 'test')

    def run(self):
        # puzzle = sudoku.make_puzzle(self.puzzle_size)  # get a sudoku puzzzle
        puzzle = get_guzzle()
        puzzle = str(self.thread_id) + SEPERATOR + puzzle.strip()

        if not self.test_mode:
            accept, wk_id, server_name, server_ip, server_port = get_server_from_ctrl(max_latency=self.max_latency,
                                                                                      work_id=self.thread_id,
                                                                                      puzzle=puzzle,
                                                                                      controller_ip=self.ctrl_ip,
                                                                                      controller_port=self.ctrl_port)
            if accept == ACPT_MSG:
                server_port = int(server_port)
                wk_id = int(wk_id)

        else:
            accept, wk_id, server_name, server_ip, server_port = ACPT_MSG, self.thread_id, self.default_server_name, self.default_server_ip, self.default_server_port

        is_reject, is_error = False, False

        logger.debug('%s, wk_%d by %s (%s)' % (accept, wk_id, server_name, server_ip))
        # accept + ", wk_" + str(wk_id) + " by " + str(server_name) + ' (' + str(server_ip) + ")")

        if accept == REJ_MSG:
            is_reject = True
        elif accept == ACPT_MSG:
            t1 = time.time()
            solved_puzzle = send_message(server_ip, server_port, str(puzzle))
            t2 = time.time()
            # solved_puzzle = solved_puzzle.split(SEPERATOR)
            # print solved_puzzle[0] + '=\n' + solved_puzzle[1]
            diff_t = t2 - t1
            err = 'OK'
            if diff_t > self.max_latency or solved_puzzle == ERR_MSG:
                is_error = True
                err = ERR_MSG

            # logger.debug('Done wk_' + str(self.thread_id) + ' ' + SEPERATOR + str(diff_t) + ' seconds, ' + err)
            logger.debug('Done wk_%d, %.5f seconds, %s' % (self.thread_id, diff_t, err))

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
    logger.info('MAX_LEN={0[1]}, NUM_OF_WK={0[2]}, lambda={0[3]}'.format(sys.argv))
    # $logger.info('MAX_LEN=' + sys.argv[1] + ', NUM_OF_WK=' + sys.argv[2] + ', lambda=' + sys.argv[3])

    puzzle_size = int(config['puzzle_size'])
    mode = config['mode']
    puzzle_mode = config['puzzle_mode']
    puzzle_file = config['puzzle_file']
    if mode == 'test': logger.warning('Test mode')
    if puzzle_mode == 'generator': logger.warning('Use puzzle generator, may delay the program')

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
            # logger.debug('Next wk_' + str(i + 1) + ' ' + str(next_workload) + ' seconds')
            logger.debug("Next wk_%d: %.5f seconds" % ((i + 1), next_workload))
            if len(threads) == 100:
                logger.debug('Threads reach the max size.')
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