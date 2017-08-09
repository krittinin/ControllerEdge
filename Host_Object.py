import paramiko
import subprocess
from sys import maxint
import re
from platform import system
import time
import logging
import threading


# TODO: one thread per host

class Host(threading.Thread):
    def __init__(self, host_name, host_ip, server_port, user, password, interval):
        threading.Thread.__init__(self)
        self.host_name = host_name
        self.host = host_ip
        self.port = server_port
        self.rtt = 0
        self.cpu = None
        self.mem = None
        self.num_of_proc = None

        self.update_interval = interval
        self.last_update = None
        self.user = user
        self.password = password
        self.ssh_con = None
        self.isConnected = False
        self.isRun = False

        global logger
        logger = logging.getLogger('Host ')
        self.ssh_connect()

        # if self.isConnected:
        #    self.update()

    def ssh_connect(self):
        try:
            # Connect SSH
            self.ssh_con = paramiko.SSHClient()
            self.ssh_con.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_con.load_system_host_keys()
            self.ssh_con.connect(self.host, username=self.user, password=self.password, timeout=10)  # , None)
            self.isConnected = True
        except paramiko.ssh_exception.SSHException as e:
            logger.error(self.host_name + ": ssh_connect ssh Exception:", e.message)
            self.isConnected = False
        except:
            logger.error(self.host_name + ": connection fail")

    def close(self):
        self.isConnected = False
        self.ssh_con.close()
        logger.debug(self.host + ': closed')

    def ping(self, count=3, timeout=2, pck_size=1024):
        '''
        ping to host for get avg_rtt
        :return: avg_rtt of ping if not succeed, return MAX_INT
        '''
        # TODO: PING from source not host T^T

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

    def vmstat(self):
        # find free mem and idle cpu
        free_mem, cpu_idle = None, None
        if not self.isConnected:
            self.ssh_connect()

        if self.isConnected:
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
        if not self.isConnected:
            self.ssh_connect()

        if self.isConnected:
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
                logger.error(self.host_name + ': ssh_connect ssh Exception:', e.message)
                self.ssh_connect()
            except ValueError:
                logger.error(self.host_name + ': Value error: \'pc aux\' not in defined format')

            return process_count

    def update(self):
        logger.debug(self.host_name + ': updated')
        self.last_update = time.localtime()

        if not self.isConnected:
            self.ssh_connect()
        if self.isConnected:
            self.mem, self.cpu = self.vmstat()
            self.rtt = self.ping(count=4)
            self.num_of_proc = self.pc_count()

            # print self.host, self.mem, self.cpu, self.rtt

    def run(self):
        self.isRun = True
        while self.isRun:
            self.update()
            time.sleep(self.update_interval)
        self.close()

    def terminate(self):
        self.isRun = False
