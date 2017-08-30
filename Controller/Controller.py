# /urs/bin/python

import SocketServer
import logging
import threading
import time
import yaml
import copy
import host_thread as host_th
import os
import sys
import random

'''
Main controller to interact with sources and openvim
'''

# global logger
# global active_host_list
# global controller_is_ready
controller_is_ready = False

# global total_request, total_reject
total_request, total_reject = 0, 0

# Constant variables
ERR_MSG = 'Error'
ACPT_MSG = 'Accept'
REJ_MSG = 'Reject'

# CTRL_MSG flag
UPDATE_FLAG = '1'
ASK_FLAG = '0'

SEPERATOR = ','

RANDOM_POLICY = 0
LOW_LATENCY_POLICY = 1

buffer_size = 1024
controller_interval = 20  # second
policy = RANDOM_POLICY


def calculate_commp_latency(host):
    '''host ={'hname':, 'ip': ,'port':, 'cpu':, 'rtt':,
            'avg_proc_time': , 'proc':, 'total_latency':, 'max_core':}'''

    comp = host['avg_proc_time']
    # num_core = max. single threads server can run simultaneously
    # proc = number of current process
    # num_core > proc ==> server provides full efficiency to wk.

    cpu = host['max_core'] / float((host['proc'])) if host['max_core'] <= host['proc'] else 1
    comp = comp / cpu
    # print host['hname'], cpu, comp
    return comp


def check_constrain(active_host, max_latency):
    # assume permissive syst.

    candidate_hosts = []
    # get total_latncy for each host
    for host in active_host:
        commu, comp = host['rtt'], calculate_commp_latency(host)
        print host['hname'], commu, comp
        if commu == 0 or comp == 0: continue
        print host['hname'], commu, comp
        host['total_latency'] = commu + comp

        # check 1: new wk not grater max.latency
        if 0 < host['total_latency'] <= max_latency:
            candidate_hosts.append(host)
            # ckeck others are ok
            # TODO: 2: not make other .... if stick system

    return candidate_hosts


def select_host(policy, active_hosts, max_latency):
    is_accepted, host = False, None

    candidate_hosts = check_constrain(active_hosts, max_latency)

    if policy == RANDOM_POLICY:
        is_accepted, host = select_random(candidate_hosts)
    elif policy == LOW_LATENCY_POLICY:
        is_accepted, host = select_low_latency(candidate_hosts)
    # print host
    if is_accepted:
        for h in host_threads:
            if h.host_name == host['hname']:
                # print h.num_of_proc
                h.accept_workload()
                # print h.host_name, h.num_of_proc
                break

    return is_accepted, host


def select_random(candidate_host):
    is_accepted, host = False, None
    if len(candidate_host) > 0:
        h = random.choice(candidate_host)
        is_accepted, host = True, h
    return is_accepted, host


def select_low_latency(candidate_host):
    is_accepted, host = False, None
    if len(candidate_host) == 0:
        return is_accepted, host
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
        host = h
    return is_accepted, host


def update_latency_host(source, host_name, host_ip, commu, comp):
    for h in host_threads:
        if h.host_name == host_name and h.host == host_ip:
            h.update_latency(source, commu, comp)
            break


class ControllerThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            global total_request, total_reject

            data = self.request.recv(buffer_size)

            # data = ASK_FLAG, id,max_latency,size-of_puzzle
            # data = UPDATE_FLAG, host_name, host_ip, commu, comp

            data = str(data).strip()
            data = data.split(SEPERATOR)
            logger.debug('recv() {} from {}'.format(data, self.client_address[0]))

            if data[0] == UPDATE_FLAG:
                assert len(data) == 5, 'Data not fit in format'
                d3, d4 = float(data[3]), float(data[4])
                update_latency_host(self.client_address[0], data[1], data[2], d3, d4)
            elif data[0] == ASK_FLAG:
                total_request += 1
                assert len(data) == 4, 'Data not fit in format'

                if not controller_is_ready:
                    raise Exception('Controller is not ready')

                work_id = int(data[1])
                max_latency = float(data[2])

                active_hosts = []
                for h in host_threads:
                    if not h.is_connected: continue
                    hh = {'hname': h.host_name, 'ip': h.host_ip,
                          'port': h.port, 'cpu': h.cpu, 'rtt': h.get_source_rtt(self.client_address[0]),
                          'avg_proc_time': h.avg_process_time, 'proc': h.num_of_proc, 'total_latency': None,
                          'max_core': h.num_of_core}
                    active_hosts.append(hh)

                is_accepted, selected_host = select_host(policy, active_hosts, max_latency)
                response = REJ_MSG + SEPERATOR + str(
                    work_id) + SEPERATOR + "No_host" + SEPERATOR + "None" + SEPERATOR + "0"
                if is_accepted:
                    response = ACPT_MSG + SEPERATOR + str(work_id) + SEPERATOR + selected_host['hname'] + SEPERATOR + \
                               selected_host['ip'] + SEPERATOR + str(selected_host['port'])
                logger.debug(response)
                self.request.sendall(response)

                if not is_accepted: total_reject += 1

        except ValueError:
            logger.error('Request error: cannot convert data ')
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
            "Error loading configuration file \'{}\' {}:content format error: Failed to parse yaml format".format(
                config_file, error_pos))
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
            "Error loading host file \'{}\'{}: content format error: Failed to parse yaml format".format(host_file,
                                                                                                         error_pos))
        return False, hosts

    for n, h_data in h_list.items():
        try:
            h = host_th.Host_Thread(h_data['name'], h_data['host-ip'], h_data['sever-port'], h_data['http-port'],
                                    h_data['host-user'], h_data['host-pwd'],
                                    controller_interval, h_data['cpu-core'], h_data['avg-ps'], config['host_update'])
            if h.update_mode == host_th.SSH_UDP_MODE:
                h.set_udp_pinger(h_data['udp-client-path'], h_data['udp-client-port'])
            h.start()  # start host threat
        except Exception as e:
            logger.error('Cannot add host: {}: {}'.format(h_data['name'], e.message))
            continue
        hosts.append(h)

    return (len(hosts) > 0), hosts


'''
def update_host():
    hlist = []
    for h in host_threads:
        # h.update()
        if not h.is_connected: continue
        hh = {'name': h.name, 'ip': h.host, 'port': h.port, 'cpu': h.cpu, 'rtt': h.rtt, 'proc': h.num_of_proc,
              'process_time': h.avg_process_time}
        hlist.append(hh)
    return hlist
'''

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

    console.setLevel(getattr(logging, config['console_log_level']))
    logger.setLevel(getattr(logging, config['file_log_level']))

    # ---------------Initial parameter-----------------
    # policy: RANDOM=0, LOW.LATENCY=1
    # controller_interval: update period [second]
    try:
        assert len(sys.argv) == 2 + 1, 'Please insert correct parameters; policy[0,1] controller_interval'
        policy = int(sys.argv[1])
        assert policy == RANDOM_POLICY or policy == LOW_LATENCY_POLICY, 'Policy must be ' + str(
            RANDOM_POLICY) + 'or' + str(LOW_LATENCY_POLICY)
        controller_interval = float(sys.argv[2])  # [sec]
        assert controller_interval > 0, 'Please insert correct parameters'
    except Exception as e:
        logger.error('Parameter error: ' + e.message)
        exit(1)
    # ---------------Finish initialising parameter-----------------

    logger.info('{} policy, INTERVAL={}'.format(
        'RANDOM' if policy == RANDOM_POLICY else 'LOW.LATENCY' if policy == LOW_LATENCY_POLICY else 'ELSE',
        sys.argv[2]))

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
    logger.debug('Server loop running in thread{}'.format(server_thread.name))
    '''
    temp_host_list = update_host()
    lock_host_list.acquire()
    try:
        active_host_list = copy.copy(temp_host_list)
    finally:
        lock_host_list.release()
    '''
    controller_is_ready = True
    logger.info('Controller is ready')
    sys.stdout.flush()

    try:
        while True:
            pass
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
