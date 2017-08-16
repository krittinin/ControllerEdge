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
import Queue

max_latency = 2  # second

total_request = 0
total_reject = 0
total_error = 0
buffer_size = 1024

streamformat = "%(asctime)s %(name)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.DEBUG,
                    format=streamformat)

logger = logging.getLogger("Source")

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
    response = "Error"
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        try:
            sock.sendall(message)
            response = sock.recv(buffer_size)
        finally:
            sock.close()
    except:
        logger.error("Socket error " + host, port)

    return str(response)


def get_server_from_ctrl(max_latency, puzzle, controller_ip, controller_port):
    ask_msg = str(max_latency) + ',' + str(sys.getsizeof(puzzle))
    ctrl_rep = send_message(controller_ip, controller_port, ask_msg)
    ctrl_rep = re.split(',', ctrl_rep)
    if len(ctrl_rep) != 3:
        ctrl_rep = {"Error", None, None}
        logger.error("Controller response error")
    return ctrl_rep[0], ctrl_rep[1], ctrl_rep[2]


class sourceThread(threading.Thread):
    def __init__(self, cip, cport, d_sip, d_sport, pzzsize, maxL, mode):
        threading.Thread.__init__(self)
        self.ctrl_ip = cip
        self.ctrl_port = cport
        self.default_server = d_sip
        self.default_server_port = d_sport
        self.max_latency = maxL
        self.puzzle_size = pzzsize
        self.test_mode = mode == 'test'  # (m == 'test')

    def run(self):
        puzzle = sudoku.make_puzzle(self.puzzle_size)  # get a sudoku puzzzle
        if not self.test_mode:
            accept, server_ip, server_port = get_server_from_ctrl(max_latency=self.max_latency, puzzle=puzzle,
                                                                  controller_ip=self.ctrl_ip,
                                                                  controller_port=self.ctrl_port)
        else:
            accept, server_ip, server_port = "Accept", self.default_server, self.default_server_port
            is_reject, is_error = False, False

        if accept == "Reject":
            is_reject = True
        elif accept == "Accept":
            t1 = time.time()
            solved_puzzle = send_message(server_ip, server_port, str(puzzle))
            t2 = time.time()
            print threading.current_thread(), t2 - t1
            if t2 - t1 > self.max_latency:
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
    config_file = 'source.cfg'
    config_is_ok, config = load_config(config_file)
    if not config_is_ok:
        logger.error('load config fail!! exit the program')
        exit(1)

    logger.setLevel(getattr(logging, config['log_level']))

    controller_ip = str(config['controller_ip'])
    controller_port = int(config['controller_port'])

    puzzle_size = int(config['puzzle_size'])
    mode = config['mode']
    is_test_mode = False
    if mode == 'test':
        is_test_mode = True

    server_ip = config['default_server_ip']
    server_port = config['default_server_port']

    wk_rate = 0.09  # per second
    # server_ip, server_port = '131.112.21.86', 12541

    threads = collections.deque(maxlen=100)

    try:
        for i in range(2):
            client = sourceThread(controller_ip, controller_port, server_ip, server_port, puzzle_size, max_latency,
                                  mode)
            client.daemon = True
            client.start()
            threads.append(client)
            next_workload = random.expovariate(wk_rate)  # as poisson process
            logger.debug('next workload: ' + str(next_workload))
            time.sleep(next_workload)

    except KeyboardInterrupt, SystemExit:
        logger.info('Exit the program')

    logger.debug('Wait for all threads')
    for c in threads:
        c.join()

    print total_request, total_reject, total_error
    exit()