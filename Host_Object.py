import paramiko
import subprocess
from sys import maxint
import re
from platform import system
import time


class HostInfo():
    def __init__(self, host_ip, user, password):
        self.ip = host_ip
        self.rtt = 0
        self.cpu = None
        self.mem = None
        self.last_update = None
        self.user = user
        self.password = password
        self.ssh_con = None
        self.status = False

    def ssh_connect(self):
        try:
            # Connect SSH
            self.ssh_con = paramiko.SSHClient()
            self.ssh_con.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_con.load_system_host_keys()
            self.ssh_con.connect(self.host, username=self.user, password=self.password, timeout=10)  # , None)
            self.status = True
        except paramiko.ssh_exception.SSHException as e:
            text = e.args[0]
            print self.name, ": ssh_connect ssh Exception:", text
            self.status = False

    def close(self):
        self.status = False
        self.ssh_con.close()

    def ping(self, count=2, timeout=2, pck_size=1024):
        '''
        ping to host for get avg_rtt
        :return: avg_rtt of ping if not succeed, return MAX_INT
        '''

        ping_result = None
        avg_rtt = maxint

        try:
            if system().lower() == 'windows:':
                # for windows
                ping_out = subprocess.check_output(
                    ['ping', host, '-n', str(count), '-l', str(pck_size), '-w', str(timeout * 1000)]).splitlines()
            else:
                # for else assume linux
                ping_out = subprocess.check_output(
                    ['ping', host, '-c', str(count), '-s', str(pck_size), '-W', str(timeout)]).splitlines()

            len_ping = len(ping_out)

            if len_ping > 0:
                ping_result = ping_out[len_ping - 1]

            if len(ping_result) != 0:
                # linux
                # INDEX:  [ 0   ,  1   ,  2   ,  3   ,  4    ,  5 ,  6     ,  7     ,  8     ,  9     ,  10 ]
                # EX_TXT: ['rtt', 'min', 'avg', 'max', 'mdev', '=', '0.189', '0.195', '0.202', '0.015', 'ms']
                ping_result = re.split(' |/', ping_result)
                _INDEX_AVG_RTT = 7
                avg_rtt = int(ping_result[_INDEX_AVG_RTT])
                # TODO: windows ping

        except subprocess.CalledProcessError as e:
            print 'CalledProcessError: ' + e.message
        except ValueError:
            print 'ValueError: ' + 'check ping result format'
        except Exception as e:
            print 'Unknown error: ' + e.message
        finally:
            return avg_rtt

    def vmstat(self):
        free_mem, cpu_ideal = None, None
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
                cpu_ideal = int(vmstat_result[14])

            except paramiko.ssh_exception.SSHException as e:
                print self.name, ": ssh_connect ssh Exception:", e.message
                self.ssh_connect()
            except ValueError:
                print "Value error: vmstat not in defined format"

            return free_mem, cpu_ideal

    def update(self):
        print "Update host: " + self.ip
        self.last_update = time.localtime()
        self.mem, self.cpu = self.vmstat()
        self.rtt = self.ping(count=4)
