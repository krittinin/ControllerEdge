import socket
import threading
import time
import sys

buffer_size = 1024

_serverAddress = '131.112.21.86'
_port = 12123


class clientThread(threading.Thread):
    def __init__(self, ip, port, msg):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.message = msg

    def client_action(self, ip, port, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))
        sock.sendall(message)
        response = sock.recv(buffer_size)
        print "Received: {}".format(response)
        sock.close()

    def run(self):
        self.client_action(self.ip, self.port, self.message)


a = ['123456789', '123456789', '123456789',
     '123456789', '123456789', '123456789',
     '123456789', '123456789', '123456789']

max_latency = 100  # ms
mssage = str(max_latency) + ',' + str(sys.getsizeof(a))

th1 = clientThread(_serverAddress, _port, mssage)
th2 = clientThread(_serverAddress, _port, mssage + '1')
th3 = clientThread(_serverAddress, _port, mssage + '2')
# time.sleep(1)

th1.start()
# time.sleep()
th2.start()
time.sleep(2)
th3.start()
'''
for i in range(2):
    th1.run()
    # time.sleep()
    th2.run()
    time.sleep(2)
    th3.run()
    '''
