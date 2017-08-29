import paramiko
import subprocess
import re
import time
import logging
import threading
import httplib
import yaml

SERVICE_NAME = 'Sudoku_Service'

# Update mode:
SSH_MODE = 'SSH'
SOURCE_MODE = 'SOURCE'
HTTP_MODE = 'HTTP'

# TODO: update by running http server in host

DEFAULT_RTT = 0.00001

SOURCE_TTL = 60


class Host_Thread(threading.Thread):
    def __init__(self, host_name, host_ip, server_port, http_port, user, password, interval, num_of_cpu, avg_pc,
                 update_mode=SSH_MODE):
        threading.Thread.__init__(self)
        self.host_name = host_name
        self.host_ip = host_ip
        self.port = server_port
        self.http_port = http_port

        self.cpu = None
        self.mem = None
        self.num_of_proc = 0
        self.num_of_core = num_of_cpu

        self.update_interval = interval
        self.last_update = None
        self.user = user
        self.password = password
        self.connection = None
        self.is_connected = False
        self.isRun = False

        self.event = threading.Event()
        # self.source_ttl = 60  # second

        self.avg_process_time = avg_pc

        assert update_mode in {SSH_MODE, SOURCE_MODE, HTTP_MODE}, 'Invalid update mode'
        self.update_mode = update_mode

        self.temp_comp = avg_pc
        self.temp_comp_counter = 1

        self.source_list = []  # source = {'ip':,'last_connect':,'rtt':,'count':,'sum_rtt':}

        global logger
        logger = logging.getLogger('Host ')

        if self.update_mode == HTTP_MODE:
            self.http_connect()
        else:
            self.ssh_connect()

            # self.rtt = self.ping_controller()

            # if self.isConnected:
            #    self.update()

    def http_connect(self):
        try:
            self.connection = httplib.HTTPConnection(self.host_ip, port=self.http_port)
            self.is_connected = True
        except httplib.HTTPException as e:
            logger.error(self.host_name + ": http_connect Exception:", e.message)
            self.is_connected = False
        except:
            logger.error(self.host_name + ": connection fail")
            self.is_connected = False

    def ssh_connect(self):
        try:
            # Connect SSH
            self.connection = paramiko.SSHClient()
            self.connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.connection.load_system_host_keys()
            self.connection.connect(self.host_ip, username=self.user, password=self.password, timeout=10)  # , None)
            self.is_connected = True
        except paramiko.ssh_exception.SSHException as e:
            logger.error(self.host_name + ": ssh_connect ssh Exception:", e.message)
            self.is_connected = False
        except:
            logger.error(self.host_name + ": connection fail")
            self.is_connected = False

    def ping_controller(self, count=3, timeout=2, pck_size=1024):
        ping_result = None
        avg_rtt = 0

        try:
            # if system().lower() == 'windows':
            #    # for windows
            #    ping_out = subprocess.check_output(
            #        ['ping', self.host, '-n', str(count), '-l', str(pck_size), '-w', str(timeout * 1000)]).splitlines()
            # else:
            # for else assume linux
            ping_out = subprocess.check_output(
                ['ping', self.host_ip, '-c', str(count), '-s', str(pck_size), '-W', str(timeout)]).splitlines()

            len_ping = len(ping_out)

            if len_ping > 0:
                ping_result = ping_out[len_ping - 1]

            if len(ping_result) != 0:
                # linux
                # INDEX:  [ 0   ,  1   ,  2   ,  3   ,  4    ,  5 ,  6     ,  7     ,  8     ,  9     ,  10 ]
                # EX_TXT: ['rtt', 'min', 'avg', 'max', 'mdev', '=', '0.189', '0.195', '0.202', '0.015', 'ms']

                ping_result = re.split(' |/', ping_result)
                # print ping_result
                _INDEX_AVG_RTT = 7
                avg_rtt = float(ping_result[_INDEX_AVG_RTT])

        except subprocess.CalledProcessError as e:
            logger.error(self.host_name + ': CalledProcessError: ' + e.message)
        except ValueError:
            logger.error(self.host_name + ': ValueError: ' + 'check ping result format')
        except Exception as e:
            logger.error(self.host_name + ': Unknown error: ' + e.message)
        finally:
            return avg_rtt

    def ping_source(self, source, count=2, timeout=2, pck_size=1024):
        '''SSH MODE'''
        assert self.update_mode == SSH_MODE, 'Wrong mode'
        avg_rtt = 0

        if not self.is_connected:
            self.ssh_connect()

        if not self.is_connected:
            return avg_rtt
        try:
            command = "ping " + source + " -c" + str(count) + " -s" + str(pck_size) + " -W" + str(timeout)
            _, stdout, stderr = self.connection.exec_command(command)
            #
            # PING 131.112.21.80 (131.112.21.80) 1024(1052) bytes of data.
            # 1032 bytes from 131.112.21.80: icmp_seq=1 ttl=64 time=0.280 ms
            # 1032 bytes from 131.112.21.80: icmp_seq=2 ttl=64 time=0.227 ms
            #
            # --- 131.112.21.80 ping statistics ---
            # 2 packets transmitted, 2 received, 0% packet loss, time 999ms
            # rtt min/avg/max/mdev = 0.227/0.253/0.280/0.030 ms

            ping_result = None
            for line in stdout.readlines():
                ping_result = line.strip()
            ping_result = re.split(' |/', ping_result)
            # print ping_result
            _INDEX_AVG_RTT = 7
            avg_rtt = float(ping_result[_INDEX_AVG_RTT])
        except paramiko.ssh_exception.SSHException as e:
            logger.error(self.host_name + ': ssh_connect ssh Exception:', e.message)
            self.ssh_connect()
        except ValueError:
            logger.error(self.host_name + ': ValueError: ' + 'check ping result format')
        except Exception as e:
            logger.error(self.host_name + ': Unknown error: ' + e.message)
        finally:
            return avg_rtt

    def update_rtt_source_list(self, sources=None):
        # source = {'ip':,'last_connect':,'rtt':,'count':,'sum_rtt':}
        for sr in self.source_list:
            if time.time() - sr['last_connect'] > SOURCE_TTL:
                self.source_list.remove(sr)
            else:
                if self.update_mode == SSH_MODE:
                    sr['rtt'] = self.ping_source(source=sr['ip'])
                elif self.update_mode == SOURCE_MODE:
                    sr['rtt'] = sr['sum_rtt'] / sr['count']
                    sr['sum_rtt'] = sr['rtt']
                    sr['count'] = 1
                elif self.update_mode == HTTP_MODE:
                    if sources is not None:
                        # list = [s1:{ip: , rtt: }, s2:{ip:, rtt:}, ...]
                        for s, s_data in sources.items():
                            if sr['ip'] == s_data['ip']:
                                sr['rtt'] = s_data['rtt']
                    else:
                        logger.warning('No sources list from host')

    def get_source_rtt(self, source):
        '''
        Measure rtt from host to source
        if source exists in maintaining list, get from the list
        else add a new source and measure rtt newly
        :param source: ip
        :return: round-trip-time from host to source
        '''
        # source = {'ip':,'last_connect':,'rtt':,'count':,'sum_rtt':}
        new_source = True
        rtt = 0
        for sr in self.source_list:
            if sr['ip'] == source:
                new_source = False
                rtt = sr['rtt']
                sr['last_connect'] = time.time()

        if new_source:
            rtt = self.ping_source(source) if self.update_mode == SSH_MODE else DEFAULT_RTT
            self.source_list.append({'ip': source, 'last_connect': time.time(), 'rtt': rtt, 'count': 1, 'sum_rtt': rtt})

        return rtt

    def vmstat(self):
        '''
        Must run in SSH MODE
        Get free memory and idle cpu using vmstat command through ssh connection
        '''
        assert self.update_mode == SSH_MODE, 'Wrong mode'
        # find free mem and idle cpu
        free_mem, cpu_idle = None, None
        if not self.is_connected:
            self.ssh_connect()

        if self.is_connected:
            try:
                command = "vmstat 2 2"
                _, stdout, stderr = self.connection.exec_command(command)

                #
                #       procs -----------memory---------- ---swap-- -----io---- -system-- ------cpu-----
                #       r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
                #       3  0 968880 206736 279960 1610188    1    1     7    20   25   17  3  1 95  1  0
                # index  0   1   2       3       4   5       6    7     8     9   10   11 12 13 14 15 16
                # use: free(3), id(14)

                string = None
                for line in stdout.readlines():
                    string = line.strip()
                vmstat_result = re.split('\s+', string)

                free_mem = int(vmstat_result[3])
                cpu_idle = int(vmstat_result[14])

            except paramiko.ssh_exception.SSHException as e:
                logger.error(self.host_name + ': ssh_connect ssh Exception:', e.message)
                self.ssh_connect()
            except ValueError:
                logger.error(self.host_name + ': Value error: vmstat not in defined format')

        return free_mem, cpu_idle

    def pc_count(self):
        '''
        Must run in SSH MODE'
        cont number of process using ps and grep command through ssh connection
        '''
        assert self.update_mode == SSH_MODE, 'Wrong mode'
        # ps aux | wc -l
        process_count = None
        if not self.is_connected:
            self.ssh_connect()

        if self.is_connected:
            try:
                command = "ps aux | grep " + SERVICE_NAME + " | grep " + self.user + " | wc -l"  # show num of all processes
                # command = 'pgrep ' + SERVICE_NAME + ' | wc -l'
                _, stdout, stderr = self.connection.exec_command(command)

                # 12 --> #of proc

                string = None
                for line in stdout.readlines():
                    string = line.strip()
                pc_result = re.split('\s+', string)

                process_count = int(pc_result[0])

            except paramiko.ssh_exception.SSHException as e:
                logger.error(self.host_name + ': ssh_connect ssh Exception:', e.message)
                self.ssh_connect()
            except ValueError:
                logger.error(self.host_name + ': Value error: \'pgrep\' not in defined format')

            return process_count

    def update_by_http(self):
        '''
        Must runin HTTP_MODE
        Get http response then translate from yaml format to host status
        '''
        assert self.update_mode == HTTP_MODE, 'Wrong mode'
        if not self.is_connected:
            self.http_connect()
        try:
            if self.is_connected:
                self.connection = httplib.HTTPConnection(self.host_ip, port=self.http_port)

                self.connection.request('GET', '/')
                resp = self.connection.getresponse()
                # resp ==> YAML format
                # proc: 0\n mem: 0\n cpu: 0.0\n comp: 0.0\n p-num: 0
                # sources:\n s1:\n  ip: 127.0.0.1\n  rtt: 0\n  s-sum: 0
                #         \n s2:\n  ip: 127.0.0.1\n  rtt: 0\n  s-sum: 0
                assert resp.status == httplib.OK, 'Response error'
                data = yaml.load(resp.read())
                self.mem, self.cpu = data['mem'], data['cpu']
                self.num_of_proc = data['proc']
                self.update_rtt_source_list(sources=data['sources'])
                # for s, s_data in data['sources'].items():
                #    print s_data['ip'], s_data['rtt']
                #    print '\n'

        except yaml.YAMLError:
            logger.error('HTTP respond not in YAML format')
        except Exception as e:
            logger.error('Cannot update host: ' + e.message)

    def update(self):
        '''
        update host status with controller interval
        memory, idle cpu, number of process, rtt of each source
        '''
        logger.debug(self.host_name + ': updated')
        self.last_update = time.localtime()

        if self.update_mode == SSH_MODE:
            if not self.is_connected:
                self.ssh_connect()
            if self.is_connected:
                self.mem, self.cpu = self.vmstat()
                self.num_of_proc = self.pc_count()
                self.update_rtt_source_list()

        if self.update_mode == SOURCE_MODE or self.is_connected:
            self.update_rtt_source_list()
            # self.update_avg_process_time() #Dont update it

        if self.update_mode == HTTP_MODE:
            if not self.is_connected:
                self.http_connect()
            if self.is_connected:
                self.update_by_http()

    def update_latency(self, source, commu, comp):
        '''
        Once controller receives update msg from source order this command
        :param source: ip
        :param commu: communication latency
        :param comp:  computation latency
        :return: None
        '''
        # ---Communnication-------
        # source = {'ip':,'last_connect':,'rtt':,'count':,'sum_rtt':}

        for sr in self.source_list:
            if sr['ip'] == source:
                sr['count'] += 1
                sr['sum_rtt'] += commu
                break
        # ---Computation---------
        self.temp_comp += comp
        self.temp_comp_counter += 1

    def run(self):
        self.isRun = True
        while self.isRun:
            self.update()
            # time.sleep(self.update_interval)
            self.event.wait(self.update_interval)
        self.close()

    def update_avg_process_time(self):
        self.avg_process_time = self.temp_comp / self.temp_comp_counter
        self.temp_comp_counter = 1
        self.temp_comp = self.avg_process_time

    def accept_workload(self):
        self.num_of_proc += 1

    def terminate(self):
        self.isRun = False
        self.event.set()

    def close(self):
        self.is_connected = False
        self.connection.close()
        logger.debug(self.host_ip + ': closed')
