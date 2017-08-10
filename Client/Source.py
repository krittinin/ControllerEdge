import socket
import time
import yaml
import sys
import sudoku
import logging
import os
import re

max_latency = 2  # second
total_request = 0
total_reject = 0
total_error = 0
buffer_size = 1024

streamformat = "%(asctime)s %(name)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.DEBUG,
                    format=streamformat)

logger = logging.getLogger("Source")


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


def send_messge(host, port, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall(message)
    response = sock.recv(buffer_size)
    sock.close()
    return str(response)


if __name__ == "__main__":
    config_file = 'source.cfg'
    config_is_ok, config = load_config(config_file)
    if not config_is_ok:
        logger.error('load config fail!! exit the program')
        exit(1)

    logger.setLevel(getattr(logging, config['log_level']))

    controller_ip = config['controller_ip']
    controller_port = config['controller_port']

    puzzle_size = 12
    server_ip, server_port = '131.112.21.86', 12541
    try:
        while True:
            puzzle = sudoku.make_puzzle(puzzle_size)

            print 'Puzzle='
            print puzzle.split()

            # ask controller
            ask_msg = str(max_latency) + ',' + str(sys.getsizeof(puzzle))
            ctrl_rep = send_messge(controller_ip, controller_port, ask_msg)
            total_request += 1
            ctrl_rep = re.split(',', ctrl_rep)
            print  ctrl_rep
            if "Accept" in ctrl_rep:
                server_ip = ctrl_rep[1]
                server_port = int(ctrl_rep[2])
            else:
                total_reject += 1
                continue
            t1 = time.time()
            solved_puzzle = send_messge(server_ip, server_port, puzzle)
            print 'Solution='
            print solved_puzzle.split()
            del_time = time.time() - t1
            print 'e2e latency =', del_time
            if del_time > max_latency:
                total_error += 1
            time.sleep(5)
    except KeyboardInterrupt, SystemExit:
        logger.info('Exit the program')

    print total_request, total_reject, total_error
    exit()
