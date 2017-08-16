import socket
import threading
import time
import Queue

buffer_size = 1024

ip = '192.168.11.10'
port = 12541

q = Queue.Queue()
# q =
lock = threading.Lock()

total_req = 0


def do_something(item, t1):
    t2 = time.time()
    q.put((item, t2 - t1))


def do_anotherthing():
    while not q.empty():
        aa = q.get()
        print aa
        q.task_done()


class counterThread(threading.Thread):
    def __init__(self, wait_time):
        threading.Thread.__init__(self)
        self.wtime = wait_time
        self.event = threading.Event()
        self.isrun = True

    def run(self):
        while self.isrun:
            self.event.wait(self.wtime)
            do_anotherthing()

    def close(self):
        self.event.set()
        self.isrun = False


class clientThread(threading.Thread):
    def __init__(self, ip, port, msg, que):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.message = msg
        self.queue = que

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))
        t1 = time.time()
        global total_req
        try:
            sock.sendall(bytes(self.message))
            response = str(sock.recv(buffer_size))
            # lock.acquire()
            # try:
            # self.queue.put(response)
            total_req += 1
            do_something(response, t1)
            # finally:
            #    lock.release()
            # print("Received: {}".format(response))
        finally:
            sock.close()


# now connect to the web server on port 8

a = ['123456789', '123456789', '123456789',
     '123456789', '123456789', '123456789',
     '123456789', '123456789', '123456789']
threadpool = []
mth = counterThread(10)
mth.daemon = True
mth.start()

for i in range(5):
    th = clientThread(ip, port, str(a), q)
    th.daemon = True
    th.start()
    threadpool.append(th)
    time.sleep(0.5)

for t in threadpool:
    t.join()

mth.close()
mth.join()
'''
while q.qsize() != 0:
    r = q.get()
    print r[0]
'''

# q.join()

print "exit", total_req

exit()
