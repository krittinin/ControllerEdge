import paramiko
import logging
import time
'''
This file for test ssh connection and ping command from controller to host
Need root execution
'''

# assume host is localhost with 'host' user

hostname = '127.0.0.1'  # localhost
username = 'host'
password = '123qweasd'
# port = 22 #use init_ssh_port

vm_name = 'host1-ubuntu14.04-2'

# TODO: make a class for whole connection if possible

''''
command list
vish start + vm_name
vish list [--all|--inactive|--state-running]

dommemstat domain [--period seconds] [[--config] [--live] | [--current]]
       Get memory stats for a running domain.


domstats [--raw] [--enforce] [--backing] [--state] [--cpu-total] [--balloon] [--vcpu] [--interface] [--block]
   [[--list-active] [--list-inactive] [--list-persistent] [--list-transient] [--list-running] [--list-paused]
   [--list-shutoff] [--list-other]] | [domain ...]
       Get statistics for multiple or all domains. Without any argument this command prints all available statistics for
       all domains.

'''

# TODO: Separate commands

command = #'virsh start '+vm_name #'virsh list --all' #''ip a'


#logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s',filename=time.strftime('%y%m%d_%H%M%S',time.localtime())+'.log')
logging.basicConfig(level=logging.ERROR, format='%(name)s: %(message)s')#,filename=time.strftime('%y%m%d_%H%M%S',time.localtime())+'.log')
logger = logging.getLogger('SSH')


try:
    logger.debug('Initial ssh connection')
    client_ssh = paramiko.SSHClient()
    client_ssh.load_system_host_keys()
    client_ssh.set_missing_host_key_policy(paramiko.WarningPolicy())

    logger.debug('Connect client')
    client_ssh.connect(hostname,username=username,password=password)


    # TODO: check connection before perform
    logger.debug('command '+command)

    stdin,stdout,stderr = client_ssh.exec_command(command)

    if stderr > 0:
        for line in stderr.readlines():
            print line.strip()
    else:
        for line in stdout.readlines():
            print line.strip()
except paramiko.SSHException as e:
    logger.error(msg=e.message)

finally:
    logger.debug('Close connection')
    client_ssh.close()

