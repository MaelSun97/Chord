import socket
import json
from message import send_msg, recv_msg
import sys

class ChordClient:
    def __init__(self, chordNode):
        self.host = chordNode[0]
        self.port = chordNode[1]
        self.sock = self.connect()

    def connect(self):
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.settimeout(5)
            try:
                sock.connect((self.host, self.port))
                print(f"client-connecting to host {self.host} on port {self.port}")
                break
            except socket.error:
                print(f"client-failed to connect to host {self.host} on port {self.port}\nRetry Connection...")
                return None
        return sock

    def send_request(self, req):
        req = json.dumps(req)
        req = req.encode('utf-8')
        while True:
            try:
                send_msg(self.sock, req)
                response = recv_msg(self.sock)
                if not response:
                    break
                response = response.decode('utf-8')
                response = json.loads(response)
                break
            except socket.error:
                print(f'client-failed send/receive with Node')
                return None, False
        return response, True

    def insert(self, key, value):
        msg = {'method': 'insert', 'key': key, 'value': value, 'nodeFound': False}
        response, status = self.send_request(msg)
        if status:
            print(response)
        else:
            print(f'error with request {msg}')

    def lookup(self, key):
        msg = {'method': 'lookup', 'key': key, 'nodeFound': False}
        response, status = self.send_request(msg)
        if status:
            print(response)
        else:
            print(f'error with request {msg}')

    def remove(self, key):
        msg = {'method': 'remove', 'key': key, 'nodeFound': False}
        response, status = self.send_request(msg)
        if status:
            print(response)
        else:
            print(f'error with request {msg}')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('client-invalid arguments for HashTableClient.py. Arguments should be the host and port')
        exit()
    host, port = sys.argv[1], int(sys.argv[2])
    client = ChordClient((host, port))
    client.insert('a', '10')
    client.insert('b', '100')
    client.insert('c', '1000')
    client.lookup('a')
    client.lookup('b')
    client.lookup('c')

