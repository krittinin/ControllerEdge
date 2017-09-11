import time
import random
import sys
from socket import *

# Check command line arguments
if len(sys.argv) != 2:
    print "Usage: python UDPPingerServer <server port no> (Defualt 51149)"
    sys.exit()

# Create a UDP socket
# Notice the use of SOCK_DGRAM for UDP packets
serverSocket = socket(AF_INET, SOCK_DGRAM)
# Assign IP address and port number to socket
port = int(sys.argv[1])
serverSocket.bind(('', port))

print 'UDP server is runing on port {}'.format(port)

while True:
    # Generate random number in the range of 0 to 10
    rand = random.randint(0, 10)
    # Receive the client packet along with the address it is coming from
    message, address = serverSocket.recvfrom(1024)
    # Capitalize the message from the client
    message = message.upper()
    # If rand is less is than 4, we consider the packet lost and do not respond
    if rand < 2:
        #    continue
        # Otherwise, the server responds
        print '{}:Receive ping from {}'.format(time.strftime("%m%d-%H%M"), address)
    serverSocket.sendto(message, address)
