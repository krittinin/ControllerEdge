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
# max_thread_list = 100

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

# CTRL_MSG flag
UPDATE_FLAG = '1'
ASK_FLAG = '0'

# puzzle mode
FILE_MODE = 'FILE'
GENERATOR_MODE = 'GENERATOR'

lock = threading.Condition()
flag_condition = True

is_source_update = False


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


def send_message(host, port, message, wait_resp=True):
    response = ERR_MSG
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.settimeout(max_latency * 2)
        try:
            sock.sendall(message)
            if wait_resp:
                response = sock.recv(buffer_size)
            else:
                response = None
        finally:
            sock.close()
    except socket.timeout:
        logger.error("Socket timeout " + host + " " + str(port))
    except socket.error as e:
        logger.error("Socket error " + host + ": " + e.message)

    return str(response)


def get_server_from_ctrl(max_latency, work_id, puzzle, controller_ip, controller_port):
    # ask_msg = ASK_FLAG, id,max_latency,size-of_puzzle
    ask_msg = ASK_FLAG + SEPERATOR + str(work_id) + SEPERATOR + str(max_latency) + SEPERATOR + str(
        sys.getsizeof(puzzle))

    ctrl_rep = send_message(controller_ip, controller_port, ask_msg)
    ctrl_rep = re.split(SEPERATOR, ctrl_rep)

    # ctrl_rep = [Accept, work_id, edge_name, edge_ip, edge_port]
    if len(ctrl_rep) != 5:
        ctrl_rep = [ERR_MSG, work_id, '', '', '', '']
        logger.error("Controller response error")
    return ctrl_rep[0], ctrl_rep[1], ctrl_rep[2], ctrl_rep[3], ctrl_rep[4]


def update_time_to_server(commu, comp, host_name, host_ip):
    # msg = UPDATE_FLAG, host_name, host_ip, commu, comp
    update_msg = UPDATE_FLAG + SEPERATOR + host_name + SEPERATOR + host_ip + SEPERATOR + str(commu) + SEPERATOR + str(
        comp)
    send_message(host=controller_ip, port=controller_port, message=update_msg, wait_resp=False)
    pass


def get_guzzle():
    if puzzle_mode == FILE_MODE:
        l = random.randint(1, FILE_EOF)
        line = linecache.getline(puzzle_file, l).strip()
        line = line.replace(',', '\n')
    else:
        line = sudoku.make_puzzle(puzzle_size)
    return line


def send_pluzzle_to_server(server_ip, server_port, puzzle, send_time):
    status, wk_id, solved_puzzle, arrive_time, finish_time = ERR_MSG, -1, '', 0, 0

    # send_msg = id, puzzle, send-time
    resp = send_message(server_ip, server_port, puzzle + SEPERATOR + '{}'.format(send_time))

    if resp != ERR_MSG:
        # reply_msg = [arrive_time, finish_time, work_id, solution]
        resp = resp.split(SEPERATOR)
        if len(resp) == 4:
            try:
                status = 'OK'
                arrive_time = float(resp[0])
                finish_time = float(resp[1])
                wk_id = int(resp[2])
                solved_puzzle = resp[3]
            except ValueError:
                logger.error('Cannot get values from reply msg: ' + str(resp))
                status, wk_id, solved_puzzle, arrive_time, finish_time = ERR_MSG, -1, '', 0, 0
        else:
            logger.error('Reply message error: ' + str(resp))

    return status, wk_id, solved_puzzle, arrive_time, finish_time


class SourceThread(threading.Thread):
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

        logger.debug('%s, wk_%d by %s (%s)' % (accept, int(wk_id), server_name, server_ip))
        if accept == ERR_MSG:
            is_error = True
        elif accept == REJ_MSG:
            is_reject = True
        elif accept == ACPT_MSG:
            send_time = time.time()
            # solved_puzzle = send_message(server_ip, server_port, str(puzzle))
            err, wid, solved_puzzle, arrive_time, finish_time = send_pluzzle_to_server(server_ip, server_port, puzzle,
                                                                                       send_time)
            receive_time = time.time()
            # solved_puzzle = solved_puzzle.split(SEPERATOR)
            total_latency = receive_time - send_time
            if err != ERR_MSG:
                # total_latency = receive_time - send_time
                compu_latency = finish_time - arrive_time
                commu_latency = total_latency - compu_latency
                if is_source_update:
                    update_time_to_server(commu_latency, compu_latency, server_name, server_ip)
                    # print 'total = %5fs, commu = %5fs, compu = %5fs' % (total_latency, commu_latency, compu_latency)
            if total_latency > self.max_latency or err == ERR_MSG:
                is_error = True

            logger.debug('Done wk_%d, %.5f seconds, %.5f+%.5f, %s' % (
                self.thread_id, total_latency, commu_latency, compu_latency, err))

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
        assert max_latency > 0 and num_of_workloads >= 0 and wk_rate > 0, 'Please insert correct parameters: ' \
                                                                          'max_latency, num_of_workloads, work_rate'

    except Exception as e:
        logger.error('Parameter error: ' + e.message)
        exit(1)
    # ---------------Finish initializing parameter-----------------
    logger.info('MAX_LEN={0[1]}, NUM_OF_WK={0[2]}, lambda={0[3]}'.format(sys.argv))

    # TODO: initially connect to all hosts

    puzzle_size = int(config['puzzle_size'])
    mode = config['mode']
    is_source_update = True if config['source_update'] == 'YES' else False
    puzzle_mode = config['puzzle_mode']
    puzzle_file = config['puzzle_file']
    if not check_exist_file(puzzle_file):
        puzzle_mode = GENERATOR_MODE
        logger.warning('{} not exist, change to generator mode'.format(puzzle_file))
    else:
        # -----get file eof--------
        with open(puzzle_file) as f:
            FILE_EOF = sum(1 for _ in f)

    if mode == 'test': logger.warning('Test mode')
    if puzzle_mode == GENERATOR_MODE: logger.warning('Use puzzle generator, may delay the program')

    # is_test_mode = (mode == TEST_MODE)

    server_ip = config['default_server_ip']
    server_port = config['default_server_port']

    threads = collections.deque(maxlen=(num_of_workloads / 2))

    try:
        for i in range(num_of_workloads):
            client = SourceThread(i, controller_ip, controller_port, server_ip, server_port, puzzle_size, max_latency,
                                  mode)
            client.daemon = True
            client.start()
            threads.append(client)
            next_workload = random.expovariate(wk_rate)  # as poisson process
            logger.debug("Next wk_%d: %.5f seconds" % ((i + 1), next_workload))

            # if len(threads) >= max_thread_list:
            #    logger.warning('Threads reach the max size.')
            time.sleep(next_workload)

    except KeyboardInterrupt, SystemExit:
        logger.info('Exit the program')

    logger.debug('Wait for all threads')
    for c in threads:
        c.join()

    print "total requests:", total_request
    print "total rejections:", total_reject
    print "total errors:", total_error
    print 'Source exit'
    exit()
