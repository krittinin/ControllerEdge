import time
import sys
from socket import *

# Check command line arguments
if len(sys.argv) != 3:
    print "Usage: python UDPPingerClient <server ip address> <server port no>"
    sys.exit()

# Create a UDP socket
# Notice the use of SOCK_DGRAM for UDP packets
clientSocket = socket(AF_INET, SOCK_DGRAM)

# To set waiting time of one second for reponse from server
clientSocket.settimeout(1)

# Declare server's socket address
remoteAddr = (sys.argv[1], int(sys.argv[2]))

# Ping ten times
max_rtt, avg_rtt, min_rtt = 0, 0, 0
counter = 0

for i in range(10):

    sendTime = time.time()
    message = 'PING ' + str(i + 1) + " " + str(time.strftime("%H:%M:%S"))
    clientSocket.sendto(message, remoteAddr)

    try:
        data, server = clientSocket.recvfrom(1024)
        recdTime = time.time()
        rtt = recdTime - sendTime
        avg_rtt += rtt
        max_rtt = rtt if rtt > max_rtt else max_rtt
        min_rtt = rtt if rtt < min_rtt or min_rtt == 0 else min_rtt
        counter += 1
        # print "Message Received", data
        # print "Round Trip Time", rtt
        # print

    except timeout:
        # print 'REQUEST TIMED OUT'
        # print
        pass

print '{} packets transmitted, {} received, {}% packet loss'.format(10, counter, (1.0 - counter / 10.0) * 100)
print 'rtt min/avg/max = {}/{}/{} ms'.format(min_rtt * 1000, avg_rtt / counter * 1000 if counter > 0 else avg_rtt,
                                             max_rtt * 1000)
