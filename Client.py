import socket

buffer_size = 1024

_serverAddress = '131.112.21.86'
_port = 12076


def client_action(ip, port, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    try:
        sock.sendall(message)
        response = sock.recv(buffer_size)
        print "Received: {}".format(response)
    finally:
        sock.close()


message = "Hello from client"
client_action(_serverAddress, _port, message)
