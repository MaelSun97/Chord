import hashlib
import socket
import json
import sys
import datetime
import os
import requests
import threading
from time import sleep
import selectors
import types

from HashTable import HashTable
from message import send_msg, recv_msg


class ChordNode:
    def __init__(self):
        self.mbit = 16
        self.host = socket.gethostbyname(socket.gethostname())
        self.port = 0
        self.nsAddressPort = ('catalog.cse.nd.edu', 9097)
        self.nodeId = None
        self.nsNode = None
        self.fingerTable = [None] * self.mbit
        self.fingerTableIds = [None] * self.mbit
        self.predecessor = None
        self.successor = None
        self.successor2 = None
        self.sel = selectors.DefaultSelector()
        self.ht = HashTable()
        self.backup = {}
        self.conn_dict = {}

    def start(self):
        # self.recover()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            self.port = sock.getsockname()[1]
            self.nodeId = (int.from_bytes(hashlib.sha1((self.host+str(self.port)).encode()).digest(), sys.byteorder)) % pow(2, self.mbit)
            self.fingerTable = [(self.host, self.port)] * self.mbit
            sock.listen()
            self.join()
            # self.altFingerTableCalc()
            # self.fingerTableUpdate()
            self.neighborMonitor()
            print(f"Node-listening on {self.host} port", self.port)
            sock.setblocking(False)
            self.sel.register(sock, selectors.EVENT_READ, data=None)

            try:
                while True:
                    events = self.sel.select(timeout=None)
                    for key, mask in events:
                        if key.data is None:
                            self.accept_wrapper(key.fileobj)
                        else:
                            self.service_connection(key, mask)
            except KeyboardInterrupt:
                print("Caught keyboard interrupt, exiting")
            finally:
                self.sel.close()

    def name_register(self):
        udpsock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        name = {"type": "hashtable",
                "owner": "rsun2",
                "port": self.port,
                "project": 'chord'+str(self.nodeId)}
        msg = json.dumps(name).encode('utf-8')
        regularns = threading.Thread(target=self.timerNameReg, args=(msg, udpsock))
        regularns.start()

    def timerNameReg(self, msg, udpsock):
        while True:
            udpsock.sendto(msg, self.nsAddressPort)
            sleep(60)

    def timerValidate(self):
        while True:
            node, status = self.predRequest(self.successor)     # try to connect to the successor and get the pred(succ(curr node))
            if not status:                            # if the successor can not be connected then the actual successor failed
                self.successor = self.successor2      # fix topology
                succ2, status = self.succ2Request(self.successor)
                if status:
                    self.successor2 = succ2
                self.predModify(self.successor, (self.host, self.port))
            else:
                if node is None:                        # if the predecessor of successor is none, then the actual successor failed
                    self.predModify(self.successor, (self.host, self.port))      # fix topology
                else:
                    # host, port = node
                    if node != (self.host, self.port):                   # if the successor's predecessor is reachable but different than current node, then new node has been added.
                        self.successor2 = self.successor
                        self.successor = node
                        self.predModify(self.successor, (self.host, self.port))
                    else:
            self.htBackup()                             # send current node's hashtable to the successor as the latter's backup
            if self.predecessor:
                sock = self.connect(self.predecessor)       # try to connect to the predecessor
                if sock is None:                            # if the predecessor can not be connected, then predecessor failed and should be marked as none.
                    self.predecessor = None
                sock.close()
            self.altFingerTableCalc()
            self.fingerTableUpdate()
            print('nodeID:', self.nodeId)
            print('fingerTableIds:', self.fingerTableIds)
            print('prev:', self.predecessor)
            print('next:', self.successor)
            print('next of the next:', self.successor2)
            sleep(20)

    def neighborMonitor(self):
        regularNM = threading.Thread(target=self.timerValidate, args=())
        regularNM.start()

    def fingerTableUpdate(self):
        # print(self.fingerTable)
        # print(self.fingerTableIds)
        if self.successor == (self.host, self.port):
            return
        node, status = self.succRequest(self.successor, 1 + self.nodeId)
        if status:
            self.fingerTable[0] = node
        for i in range(1, self.mbit):
            prevNodeId = self.fingerTableIds[i-1]
            if pow(2, i) + self.nodeId <= prevNodeId:
                self.fingerTable[i] = self.fingerTable[i-1]
            else:
                node, status = self.succRequest(self.successor, 1 + self.nodeId)
                if status:
                    self.fingerTable[i] = node
        self.altFingerTableCalc()

    def join(self):
        if self.isFirst():              # when a new node start the chord
            self.name_register()
            node = (self.host, self.port)
            for i in range(self.mbit):
                self.fingerTable[i] = node
                self.fingerTableIds[i] = self.nodeId
            self.successor = node
            self.successor2 = node
            self.predecessor = node
        else:
            print('Join an exisiting Chord system.')
            node, status = self.succRequest(self.nsNode, 1 + self.nodeId)
            print(node, status)
            if status:
                self.successor = node
                self.successor2 = self.succ2Request(node)
                self.fingerTable[0] = self.successor
                self.ht._hashtable = self.htMove(self.successor, self.nodeId, self.host, self.port)              # when a new node join an existing chord, the successor of the new node should move part of the HashTable to the new node. Other modifications is done in timerValidate().

    def isFirst(self):
        catalog = requests.get('http://catalog.cse.nd.edu:9097/query.json')
        # print(catalog.json())
        latest_time = 0
        latest_address = None
        for service in catalog.json():
            if 'project' in service and service['project'][0:5] == 'chord' and service['lastheardfrom'] > latest_time:
                latest_time = service['lastheardfrom']
                latest_address = (service['address'], service['port'])
        if latest_address:
            # print(f"client-get the address of server with name {self.server_name} in the catalog")
            print("test if the chord node on name server active... ")
            sock = self.connect(latest_address)
            if sock:
                print("Active! test done. Join the Chord.")
                self.nsNode = latest_address
                return False
            else:
                print("Not Active! test done. Start a New Chord.")
                return True
        else:
            return True
        # return True

    def leave(self):
        self.htMerge(self.successor)            # when a node leaves, it should notify the successor to merge its backup with its hashtable. Other modifications is done in timerValidate().
        # if this node is in Name server, let the successor to be in the name server.

    def predModify(self, targetNode, newPred):
        sock = self.connect(targetNode)
        if not sock:
            return
        msg = {'method': 'predModify', 'newPredHost': newPred[0], 'newPredPort': newPred[1]}
        self.send_request(sock, msg)
        sock.close()

    def predRequest(self, targetNode):
        sock = self.connect(targetNode)
        msg = {'method': 'predecessor'}
        if not sock:
            return None, False
        else:
            response, status = self.send_request(sock, msg)
            sock.close()
            if response['host'] != 'None':
                predecessor = response['host'], response['port']
                return predecessor, True
            else:
                return None, True

    def succRequest(self, targetNode, key):
        sock = self.connect(targetNode)
        if not sock:
            return None, False
        else:
            msg = {'method': 'succ', 'key': key}
            response, status = self.send_request(sock, msg)
            sock.close()
            if status:
                node = response['host'], response['port']
                return node, status
            else:
                return None, False

    def succ2Request(self, targetNode):
        sock = self.connect(targetNode)
        if not sock:
            return None, False
        else:
            msg = {'method': 'successor2'}
            response, status = self.send_request(sock, msg)
            sock.close()
            if not status:
                return None, False
            else:
                succ2 = response['host'], response['port']
                return succ2

    def htMove(self, targetNode, prevId, host, port):
        sock = self.connect(targetNode)
        if not sock:
            return None, False
        else:
            msg = {'method': 'htMove', 'prevId': prevId, 'host':host, 'port':port}
            response, status = self.send_request(sock, msg)
            sock.close()
            if not status:
                return None, False
            else:
                prevht = response['prevht']
                return prevht

    def htBackup(self):
        sock = self.connect(self.successor)
        if not sock:
            return None, False
        else:
            msg = {'method': 'htBackup', 'backup': self.ht._hashtable}
            self.send_request(sock, msg)
            sock.close()

    def htMerge(self, targetNode):
        sock = self.connect(targetNode)
        if not sock:
            return None, False
        else:
            msg = {'method': 'htMerge'}
            self.send_request(sock, msg)
            sock.close()

    def service_connection(self, key, mask):
        sock = key.fileobj
        data = key.data
        try:
            if mask & selectors.EVENT_READ:
                recv_data = recv_msg(sock)
                if recv_data:
                    msg = recv_data.decode('utf-8')
                    msg_json = json.loads(msg)
                    response = self.handle(msg_json)
                    msg_send = response.encode('utf-8')
                    data.outb += msg_send
                else:
                    # print(f"Closing connection to {data.addr}")
                    self.sel.unregister(sock)
                    sock.close()
            if mask & selectors.EVENT_WRITE:
                if data.outb:
                    send_msg(sock, data.outb)
                    data.outb = b""
        except socket.error:
            # print(f"Closing connection to {data.addr}")
            self.sel.unregister(sock)
            sock.close()

    def handle(self, req) -> str:
        lack_key_error = json.dumps({'status': 'invalid', 'error': 'invalid request format - no key specified'})
        lack_val_error = json.dumps({'status': 'invalid', 'error': 'invalid request format - no value specified'})
        lack_method_error = json.dumps(
            {'status': 'invalid', 'error': 'invalid request format - no request type specified'})
        if 'method' not in req:
            return lack_method_error
        method = req['method']
        if method == 'insert':
            if 'key' not in req:
                return lack_key_error
            if 'value' not in req:
                return lack_val_error
            nodeFound = req['nodeFound']
            if nodeFound:
                key = req['key']
                value = req['value']
                status = self.ht.insert(key, value)
                if status:
                    return json.dumps({'status': 'success', 'method': method, 'key': key, 'value': value})
                else:
                    return json.dumps({'status': 'failure', 'method': method,
                                       'failure': f'key {key} already exists with a different value, so it can not be inserted'})
            else:
                key = req['key']
                new_key = (int.from_bytes(hashlib.sha1(key.encode()).digest(), sys.byteorder)) % pow(2, self.mbit)
                idx, isfound = self.fingerTableLocate(new_key)
                sock = self.connect(self.fingerTable[idx])
                if not sock:
                    return json.dumps({'status': 'failure', 'method': method, 'failure': f'connection to the chord node {self.fingerTable[idx]} with id {self.fingerTableIds[idx]} failed'})
                if isfound:
                    req['nodeFound'] = True
                response, status = self.send_request(sock, req)
                sock.close()
                if status:
                    return response
                else:
                    return json.dumps({'status': 'failure', 'method': method, 'failure': f'connection to the chord node {self.fingerTable[idx]} with id {self.fingerTableIds[idx]} failed'})
        elif method == 'lookup':
            if 'key' not in req:
                return lack_key_error
            nodeFound = req['nodeFound']
            if nodeFound:
                key = req['key']
                value = self.ht.lookup(key)
                if value:
                    return json.dumps({'status': 'success', 'method': method, 'key': key, 'value': value})
                else:
                    return json.dumps({'status': 'failure', 'method': method,
                                       'failure': f'key {key} does not exists and can not be looked up'})
            else:
                key = req['key']
                new_key = (int.from_bytes(hashlib.sha1(key.encode()).digest(), sys.byteorder)) % pow(2, self.mbit)
                idx, isfound = self.fingerTableLocate(new_key)
                sock = self.connect(self.fingerTable[idx])
                if not sock:
                    return json.dumps({'status': 'failure', 'method': method, 'failure': f'connection to the chord node {self.fingerTable[idx]}  with id {self.fingerTableIds[idx]} failed'})
                if isfound:
                    req['nodeFound'] = True
                response, status = self.send_request(sock, req)
                sock.close()
                if status:
                    return response
                else:
                    return json.dumps({'status': 'failure', 'method': method, 'failure': f'connection to the chord node {self.fingerTable[idx]}  with id {self.fingerTableIds[idx]} failed'})
        elif method == 'remove':
            if 'key' not in req:
                return lack_key_error
            nodeFound = req['nodeFound']
            if nodeFound:
                key = req['key']
                self.ht.remove(key)
                return json.dumps({'status': 'success', 'method': method, 'key': key})
            else:
                key = req['key']
                new_key = (int.from_bytes(hashlib.sha1(key.encode()).digest(), sys.byteorder)) % pow(2, self.mbit)
                idx, isfound = self.fingerTableLocate(new_key)
                sock = self.connect(self.fingerTable[idx])
                if not sock:
                    return json.dumps({'status': 'failure', 'method': method, 'failure': f'connection to the chord node {self.fingerTable[idx]}  with id {self.fingerTableIds[idx]} failed'})
                if isfound:
                    req['nodeFound'] = True
                response, status = self.send_request(sock, req)
                sock.close()
                if status:
                    return response
                else:
                    return json.dumps({'status': 'failure', 'method': method, 'failure': f'connection to the chord node {self.fingerTable[idx]}  with id {self.fingerTableIds[idx]} failed'})
        elif method == 'predecessor':
            if self.predecessor:
                return json.dumps({'status': 'success', 'method': method, 'host': self.predecessor[0], 'port': self.predecessor[1]})
            else:
                return json.dumps({'status': 'success', 'method': method, 'host': 'None', 'port': 'None'})
        elif method == 'predModify':
            self.predecessor = (req['newPredHost'], req['newPredPort'])
            return json.dumps({'status': 'success', 'method': method})
        elif method == 'successor2':
            return json.dumps({'status': 'success', 'method': method, 'host': self.successor[0], 'port': self.successor[1]})
        elif method == 'htMove':
            prevht = self.ht.slice(int(req['prevId']), self.nodeId)
            self.predecessor = (req['host'], req['port'])
            if self.successor == (self.host, self.port):
                self.successor = self.predecessor
            return json.dumps({'status': 'success', 'method': method, 'prevht': prevht})
        elif method == 'htBackup':
            self.backup = req['backup']
            return json.dumps({'status': 'success', 'method': method})
        elif method == 'htMerge':
            self.ht._hashtable.update(self.backup)
            self.backup = {}
            return json.dumps({'status': 'success', 'method': method})
        elif method == 'succ':
            key = req['key'] & pow(2, self.mbit)
            idx, isfound = self.fingerTableLocate(key)
            host, port = self.fingerTable[idx]
            if isfound:
                return json.dumps({'status': 'success', 'method': method, 'host': host, 'port': port})
            else:
                sock = self.connect(self.fingerTable[idx])
                if not sock:
                    return json.dumps({'status': 'failure', 'method': method, 'failure': f'connection to the chord node {self.fingerTable[idx]}  with id {self.fingerTableIds[idx]} failed'})
                response, status = self.send_request(sock, req)
                sock.close()
                if status:
                    return response
                else:
                    return json.dumps({'status': 'failure', 'method': method, 'failure': f'connection to the chord node {self.fingerTable[idx]}  with id {self.fingerTableIds[idx]} failed'})
        else:
            return json.dumps(
                {'status': 'invalid', 'error': 'invalid request format - incorrect request type: ' + method})

    def altFingerTableCalc(self):
        for i in range(self.mbit):
            hostport = self.fingerTable[i][0]+str(self.fingerTable[i][0])
            self.fingerTableIds[i] = (int.from_bytes(hashlib.sha1(hostport.encode()).digest(), sys.byteorder)) % pow(2, self.mbit)

    def fingerTableLocate(self, key):
        if key < self.fingerTableIds[0] or self.nodeId > self.fingerTableIds[0]:
            return 0, True
        for i in range(1, self.mbit):
            if key > self.fingerTableIds[i-1] and key < self.fingerTableIds[i]:
                return i, False
        return self.mbit-1, False


    def accept_wrapper(self, sock):
        conn, addr = sock.accept()
        # print('server-connected by', addr)
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(conn, events, data=data)

    def connect(self, targetNode):
        if targetNode is None:
            return None
        targetHost, targetPort = targetNode
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.settimeout(5)
            try:
                sock.connect(targetNode)
                # print(f"client-connecting to host {self.host} on port {self.port}")
                break
            except socket.error:
                print(f"client-failed to connect to host {targetHost} on port {targetPort}")
                return None
        return sock

    def send_request(self, sock, req):
        req = json.dumps(req)
        req = req.encode('utf-8')
        while True:
            try:
                send_msg(sock, req)
                response = recv_msg(sock)
                if not response:
                    break
                response = response.decode('utf-8')
                response = json.loads(response)
                break
            except socket.error:
                print(f'Chord Node-failed send/receive with Node \nRetrying Connection...')
                return None, False
        return response, True


if __name__ == '__main__':
    chordnode = ChordNode()
    chordnode.start()
