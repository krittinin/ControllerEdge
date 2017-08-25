import paramiko
import subprocess
from sys import maxint
import re
from platform import system
import time
import logging
import threading

SERVICE_NAME = 'Sudoku_Service'

# Update mode:
SSH_MODE = 'SSH'
SOURCE_MODE = 'SOURCE'
HTTP_MODE = 'HTTP'
# TODO: update by running http server in host

DEFAULT_RTT = 0.00001

SOURCE_TTL = 60


class Host(threading.Thread):
    def __init__(self, host_name, host_ip, server_port, user, password, interval, num_of_cpu, avg_pc,
                 update_mode=SSH_MODE):
        threading.Thread.__init__(self)
        self.host_name = host_name
        self.host = host_ip
        self.port = server_port

        self.cpu = None
        self.mem = None
        self.num_of_proc = 0
        self.num_of_core = num_of_cpu

        self.update_interval = interval
        self.last_update = None
        self.user = user
        self.password = password
        self.ssh_con = None
        self.is_connected = False
        self.isRun = False

        self.event = threading.Event()
        # self.source_ttl = 60  # second

        self.avg_process_time = avg_pc

        assert update_mode == SSH_MODE or update_mode == SOURCE_MODE, 'Invalid update mode'
        self.update_mode = update_mode

        self.temp_comp = avg_pc
        self.temp_comp_counter = 1

        self.source_list = []  # source = {'ip':,'last_connect':,'rtt':,'count':,'sum_rtt':}

        global logger
        logger = logging.getLogger('Host ')
        self.ssh_connect()

        # self.rtt = self.ping_controller()

        # if self.isConnected:
        #    self.update()

    def ssh_connect(self):
        try:
            # Connect SSH
            self.ssh_con = paramiko.SSHClient()
            self.ssh_con.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_con.load_system_host_keys()
            self.ssh_con.connect(self.host, username=self.user, password=self.password, timeout=10)  # , None)
            self.is_connected = True
        except paramiko.ssh_exception.SSHException as e:
            logger.error(self.host_name + ": ssh_connect ssh Exception:", e.message)
            self.is_connected = False
        except:
            logger.error(self.host_name + ": connection fail")

    def close(self):
        self.is_connected = False
        self.ssh_con.close()
        logger.debug(self.host + ': closed')

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
                ['ping', self.host, '-c', str(count), '-s', str(pck_size), '-W', str(timeout)]).splitlines()

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
        avg_rtt = 0

        if not self.is_connected:
            self.ssh_connect()

        if not self.is_connected:
            return avg_rtt
        try:
            command = "ping " + source + " -c" + str(count) + " -s" + str(pck_size) + " -W" + str(timeout)
            _, stdout, stderr = self.ssh_con.exec_command(command)
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

    def update_rtt_source_list(self):
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

    def get_source_rtt(self, source):
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
        # find free mem and idle cpu
        free_mem, cpu_idle = None, None
        if not self.is_connected:
            self.ssh_connect()

        if self.is_connected:
            try:
                command = "vmstat 2 2"
                _, stdout, stderr = self.ssh_con.exec_command(command)

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
        # ps aux | wc -l
        process_count = None
        if not self.is_connected:
            self.ssh_connect()

        if self.is_connected:
            try:
                command = "ps aux | grep " + SERVICE_NAME + " | grep " + self.user + " | wc -l"  # show num of all processes
                # command = 'pgrep ' + SERVICE_NAME + ' | wc -l'
                _, stdout, stderr = self.ssh_con.exec_command(command)

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

    def update(self):
        logger.debug(self.host_name + ': updated')
        self.last_update = time.localtime()

        if not self.is_connected:
            self.ssh_connect()
        if self.is_connected:
            self.mem, self.cpu = self.vmstat()
            self.num_of_proc = self.pc_count()

        if self.update_mode == SOURCE_MODE or self.is_connected:
            self.update_rtt_source_list()

        self.update_avg_process_time()

        # print  '{}, num_proc = {}, comp = {}, commu = {}'.format(self.host_name, self.num_of_proc, self.avg_process_time, self.get_source_rtt('131.112.21.86'))

    def accept_workload(self):
        self.num_of_proc += 1

    def update_latency(self, source, commu, comp):
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

    def terminate(self):
        self.isRun = False
        self.event.set()

    def update_avg_process_time(self):
        self.avg_process_time = self.temp_comp / self.temp_comp_counter
        self.temp_comp_counter = 1
        self.temp_comp = self.avg_process_time
