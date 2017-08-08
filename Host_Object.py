import paramiko
import subprocess
from sys import maxint
import re
from platform import system
import time
import logging


class HostInfo():
    def __init__(self, host_ip, user, password):
        self.host = host_ip
        self.rtt = 0
        self.cpu = None
        self.mem = None
        self.last_update = None
        self.user = user
        self.password = password
        self.ssh_con = None
        self.status = False
        self.num_of_proc = None

        global logger
        logger = logging.getLogger('Host')

        self.update()

    def ssh_connect(self):
        try:
            # Connect SSH
            self.ssh_con = paramiko.SSHClient()
            self.ssh_con.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_con.load_system_host_keys()
            self.ssh_con.connect(self.host, username=self.user, password=self.password, timeout=10)  # , None)
            self.status = True
        except paramiko.ssh_exception.SSHException as e:
            logger.error(self.host + ": ssh_connect ssh Exception:", e.message)
            self.status = False

    def close(self):
        self.status = False
        self.ssh_con.close()
        logger.debug(self.host + ': closed')

    def ping(self, count=2, timeout=2, pck_size=1024):
        '''
        ping to host for get avg_rtt
        :return: avg_rtt of ping if not succeed, return MAX_INT
        '''

        ping_result = None
        avg_rtt = maxint

        try:
            if system().lower() == 'windows':
                # for windows
                ping_out = subprocess.check_output(
                    ['ping', self.host, '-n', str(count), '-l', str(pck_size), '-w', str(timeout * 1000)]).splitlines()
            else:
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
                # TODO: windows ping

        except subprocess.CalledProcessError as e:
            logger.error(self.host + ': CalledProcessError: ' + e.message)
        except ValueError:
            logger.error(self.host + ': ValueError: ' + 'check ping result format')
        except Exception as e:
            logger.error(self.host + ': Unknown error: ' + e.message)
        finally:
            return avg_rtt

    def vmstat(self):
        # find free mem and idle cpu
        free_mem, cpu_idle = None, None
        if not self.status:
            self.ssh_connect()

        if self.status:
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
                logger.error(self.host + ': ssh_connect ssh Exception:', e.message)
                self.ssh_connect()
            except ValueError:
                logger.error(self.host + ': Value error: vmstat not in defined format')

            return free_mem, cpu_idle

    def pc_count(self):
        # ps aux | wc -l
        process_count = None
        if not self.status:
            self.ssh_connect()

        if self.status:
            try:
                command = "ps aux | wc -l"
                _, stdout, stderr = self.ssh_con.exec_command(command)

                # 356 --> #of proc

                string = None
                for line in stdout.readlines():
                    string = line.strip()
                pc_result = re.split('\s+', string)

                process_count = int(pc_result[0])

            except paramiko.ssh_exception.SSHException as e:
                logger.error(self.host + ': ssh_connect ssh Exception:', e.message)
                self.ssh_connect()
            except ValueError:
                logger.error(self.host + ': Value error: \'pc aux\' not in defined format')

            return process_count

    def update(self):
        logger.debug(self.host + ': updated')
        self.last_update = time.localtime()
        self.mem, self.cpu = self.vmstat()
        self.rtt = self.ping(count=4)
        self.num_of_proc = self.pc_count()

        # print self.host, self.mem, self.cpu, self.rtt
