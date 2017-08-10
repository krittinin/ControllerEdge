# /urs/bin/python

import SocketServer
import logging
import threading
import time
import yaml
import copy
from Host_Object import Host
import os
import sys
import random

'''
Main controller to interact with sources and openvim
'''

global logger
global active_host_list
global controller_is_ready
controller_is_ready = False

global total_request, total_reject
total_request, total_reject = 0, 0

# global condition
# condition = threading.Condition()

# logging.basicConfig(level=logging.ERROR, format='%(name)s: %(message)s')

buffer_size = 1024
controller_interval = 20  # second
policy = 1  # 1: random, 2: lowest latency

max_proc = 1000


def calculate_commp_latency(host):
    ttl = 1
    factor = 1
    # TODO: find a way to cal it....
    # ttl = host['cpu'] / host['proc'] * factor
    return ttl


def check_constrain(active_host, max_latency):
    # assume permissive syst.

    candidate_hosts = []
    # get total_latncy for each host
    for host in active_host:
        commu, comp = host['rtt'], calculate_commp_latency(host)
        if commu == 0 or comp == 0: continue

        host['total_latency'] = commu + comp
        # check 1: new wk not grater max.latency
        if 0 < host['total_latency'] <= max_latency:
            candidate_hosts.append(host)
            # ckeck others are ok
            # TODO: 2: not make other .... if stick system

    return candidate_hosts


def select_host(policy, active_host, max_latency):
    is_accepted, host_ip, host_port = False, None, None

    candidate_hosts = check_constrain(active_host, max_latency)

    if policy == 1:
        is_accepted, host_ip, host_port = select_random(candidate_hosts)
    elif policy == 2:
        is_accepted, host_ip, host_port = select_low_latency(candidate_hosts)
    return is_accepted, host_ip, host_port


def select_random(candidate_host):
    is_accepted, host_ip, host_port = False, None, None
    if len(candidate_host) > 0:
        h = random.choice(candidate_host)
        is_accepted, host_ip, host_port = True, h['ip'], h['port']
    return is_accepted, host_ip, host_port


def select_low_latency(candidate_host):
    is_accepted, host_ip, host_port = False, None, None
    if len(candidate_host) == 0:
        return is_accepted, host_ip, host_port
    key = 'total_latency'
    min_latency = min(l[key] for l in candidate_host)
    can = []
    for c in candidate_host:
        if c[key] == min_latency:
            can.append(c)
    if len(can) <= 0:
        raise Exception('Something wrong while selecting')
    else:
        h = random.choice(can)
        is_accepted = True
        host_ip, host_port = h['ip'], h['port']
    return is_accepted, host_ip, host_port


class ControllerThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            global total_request, total_reject
            total_request += 1
            data = self.request.recv(buffer_size)
            # data: max.latexy (sec)+','+workload in byte e.g 100,144
            # TODO: Define msg..
            data = str(data).strip(' ')
            data = data.split(',')

            max_latency = int(data[0])

            # cur_thread = threading.current_thread()
            # response = '{}: {}'.format(cur_thread.name, data)
            # logger = logging.getLogger(cur_thread.name)
            logger.debug('recv() from ' + self.client_address[0])

            if not controller_is_ready:
                raise Exception('Controller is not ready')

            candidate_host = []
            for h in host_threads:
                if not h.isConnected: continue
                hh = {'name': h.name, 'ip': h.host,
                      'port': h.port, 'cpu': h.cpu, 'rtt': h.get_source_rtt(self.client_address[0]),
                      'proc': h.num_of_proc, 'total_latency': None}
                candidate_host.append(hh)

            is_accepted, selected_ip, selected_port = select_host(policy, candidate_host, max_latency)
            response = str("Reject,None,None")
            if is_accepted:
                response = str("Accept," + selected_ip + "," + str(selected_port))
            self.request.sendall(response)
            # print response
            if not is_accepted: total_reject += 1

        except Exception as e:
            logger.error('Request error: ' + e.message)
            total_reject += 1


class ControllerThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    '''
    handle each source request per thread
    '''
    pass


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


def load_hosts(host_file):
    hosts = []
    readable, read_data = read_file(host_file)
    if not readable: return False, hosts

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
        return False, hosts

    for n, h_data in h_list.items():
        try:
            h = Host(h_data['name'], h_data['host-ip'], h_data['sever-port'], h_data['host-user'], h_data['host-pwd'],
                     controller_interval)
            h.start()  # start host threat
        except Exception as e:
            logger.error('Cannot add host: ' + h_data['name'] + ': ' + e.message)
            continue
        hosts.append(h)

    return (hosts.__len__() > 0), hosts


def update_host():
    hlist = []
    for h in host_threads:
        # h.update()
        if not h.isConnected: continue
        hh = {'name': h.name, 'ip': h.host,
              'port': h.port, 'cpu': h.cpu, 'rtt': h.rtt, 'proc': h.num_of_proc}
        hlist.append(hh)
    return hlist


# TODO: send API to openvim to look up host



# TODO: list and find IP of VM in host


if __name__ == "__main__":

    global lock_host_list
    lock_host_list = threading.RLock()

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
        logger.err('load config fail!! exit the controller')
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
    host_is_ok, host_threads = load_hosts(host_file)
    if not host_is_ok:
        logger.warning('No host is running')

    time.sleep(1)

    logger.debug('Start sever_thread waiting requests for sources')
    server = ControllerThreadedTCPServer((config['controller_ip'], config['controller_port']),
                                         ControllerThreadedTCPRequestHandler)
    ip, port = server.server_address
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    logger.debug('Server loop running in thread' + server_thread.name)

    temp_host_list = update_host()
    lock_host_list.acquire()
    try:
        active_host_list = copy.copy(temp_host_list)
    finally:
        lock_host_list.release()

    controller_is_ready = True
    logger.info('Controller is ready')
    sys.stdout.flush()

    try:
        while True:
            pass
            '''time.sleep(controller_interval)
            logger.info('Update host(s)')
            temp_host_list = update_host()
            lock_host_list.acquire()
            try:
                active_host_list = copy.copy(temp_host_list)
            finally:
                lock_host_list.release()'''
    except (KeyboardInterrupt, SystemExit):
        pass

    logger.info('Disconnecting host(s)...')
    controller_is_ready = False

    for h in host_threads:
        h.terminate()
        h.join()

    logger.debug('Shut down a server')
    server.shutdown()
    server.server_close()
    print "total reject=", total_reject, ", total=", total_request
    logger.info('Exit')
    exit()
