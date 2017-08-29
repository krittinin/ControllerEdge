import threading
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import subprocess as sub
import os
import logging
import yaml
import time

streamformat = "%(asctime)s %(name)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.DEBUG,
                    format=streamformat, filename='./log/server_' + time.strftime("%m%d-%H%M") + '.log',
                    filemode='w')
global logger
logger = logging.getLogger('HTTP_SERVER')

RECORD_LATENCY_FILE = 'record.txt'

DEFAULT_USER = 'host'
SERVICE_NAME = 'Sudoku_Service'


def ps_aux_grep(user, service):
    # pc = None
    p1 = sub.Popen(['ps', 'aux'], stdout=sub.PIPE)
    p2 = sub.Popen(['grep', user], stdin=p1.stdout, stdout=sub.PIPE)
    p3 = sub.Popen(['grep', service], stdin=p2.stdout, stdout=sub.PIPE)
    p4 = sub.Popen(['wc', '-l'], stdin=p3.stdout, stdout=sub.PIPE, stderr=sub.PIPE)

    p1.stdout.close()
    p2.stdout.close()
    p3.stdout.close()

    out, err = p4.communicate()
    pc = out

    return pc


def vmstat():
    p1 = sub.Popen(['vmstat'], stdout=sub.PIPE, stderr=sub.PIPE)
    out, err = p1.communicate()

    out = out.splitlines()
    out = out[len(out) - 1]
    out = out.split()

    mem, cpu = '', ''
    if len(out) >= 14:
        mem = out[3]
        cpu = out[14]

    return mem, cpu


def read_record_file(file):
    # comm, comp = 0.0, 0.0
    record_data = 'comp: 0.0\np-num: 0\nsources:'
    try:
        assert os.path.isfile(file), '{} not exist'.format(file)
        with open(file, 'r') as f:
            record_data = f.read()
            # data = yaml.load(dataF)
            # data  = {comp: 0.0, p-num: 0, sources: {s1:{ip: '120', comm: 0.0, s_num: 0}, s2: {} } }
            # comm, comp = data['comm'], data['comp']
    except IOError as e:
        logger.error(e.message)
    except yaml.YAMLError:
        logger.error('{} not in YAML format'.format(file))
    return record_data


# Create custom HTTPRequestHandler class
class HostHTTPRequestHandler(BaseHTTPRequestHandler):
    # handle GET command
    def do_GET(self):
        # rootdir = '/home/pao/PycharmProjects/HTTPSERVER/'  # file location
        try:
            # send code 200 response
            self.send_response(200)
            # send header first
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            message = ps_aux_grep(DEFAULT_USER, SERVICE_NAME)
            mem, cpu = vmstat()
            recd = read_record_file(RECORD_LATENCY_FILE)
            # send file content to client
            self.wfile.write('proc: {}\nmem: {}\ncpu: {}\n{}'.format(message, mem, cpu, recd))
            # f.close()
            return

        except IOError:
            self.send_error(404, 'file not found')
            logger.error('file not found')
        except:
            logger.error('http error: cannot send the update')


class HostHTTPThread(threading.Thread):
    def __init__(self, host_user, server_ip, server_port, service_name):
        threading.Thread.__init__(self)
        # self.user = host_user
        self.address = (server_ip, server_port)
        self.httpd = HTTPServer(self.address, HostHTTPRequestHandler)

        global DEFAULT_USER, SERVICE_NAME
        DEFAULT_USER = host_user
        SERVICE_NAME = service_name

    def run(self):
        logger.info('HTTP server is running...')
        self.httpd.serve_forever()

    def close(self):
        self.httpd.server_close()
        logger.info('HTTP server is closed.')
