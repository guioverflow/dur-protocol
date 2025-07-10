

import threading
import socket
import json

import abcast
from utils import get_sequencer, load_data_store
import utils.unicast as uni

class Replica:
    def __init__(self, host, read_port, abcast_port):
        self.host = host
        self.read_port = read_port
        self.abcast_port = abcast_port

        self.last_commited = 0
        self.lock = threading.Lock()
        self.data_store = load_data_store()

    def handle_scan(self, message, conn):
        # {
        #     "type": "SCAN",
        #     "cid": 4050
        # }

        conn.sendall(json.dumps(self.data_store).encode())

    def handle_read(self, message, conn):
        # {
        #     "type": "READ",
        #     "cid": 4050,
        #     "key": "gui"
        # }

        cid = message['cid']
        key = message['key']

        value, version = self.data_store.get(key, (None, 0))

        response = json.dumps({"key": key, "value": value, "version": version}).encode()

        try: conn.sendall(response)
        except Exception as e: print(f"[{self.host}:{self.read_port}] Falha ao enviar resposta READ: {e}")

    def handle_commit(self, conn):
        # {
        #     "type": "COMMIT_REQ"
        #     "cid": 10, 
        #     "tid": 123sadd-asxsad-safaewqd,
        #     "rs": [("gui", 40, 3), ("gui", 42, 4), ("gui", 100, 5)],
        #     "ws": [("gui", 123), ("teste", 456)]
        # }

        message = abcast.deliver(conn)

        cid = message['cid']
        tid = message['tid']
        rs = message['rs']
        ws = message['ws']

        abort = False
        with self.lock:
            for key, rs_version in rs:
                current_value, current_version = self.data_store.get(key, (None, 0))
                if current_version > rs_version:
                    print(f"[{self.host}:{self.abcast_port}] Abortando transação {tid} (stale)")
                    abort = True

            if not abort:
                self.last_commited += 1
                for key, value in ws:
                    old_value, old_version = self.data_store.get(key, (None, 0))
                    self.data_store[key] = (value, old_version + 1)
                print(f"[{self.host}:{self.abcast_port}] Transação {tid} commitada")
    
        response = {
            "tid": tid,
            "status": 'ABORTED' if abort else 'COMMITED'
        }

        uni.send('localhost', cid, response)

    def listen_read_requests(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.read_port))
            s.listen()
            print(f"[{self.host}:{self.read_port}] Escutando requisições READ")
            while True:
                conn, _ = s.accept()
                message = uni.receive(conn, close=False)

                if message['type'] == 'READ':
                    threading.Thread(target=self.handle_read, args=(message, conn), daemon=True).start()
                elif message['type'] == 'SCAN':
                    threading.Thread(target=self.handle_scan, args=(message, conn), daemon=True).start()

    def listen_abcast_commits(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.abcast_port))
            s.listen()
            print(f"[{self.host}:{self.abcast_port}] Escutando commits ABCAST")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_commit, args=(conn,), daemon=True).start()


    def start(self):
        threading.Thread(target=self.listen_read_requests, daemon=True).start()
        threading.Thread(target=self.listen_abcast_commits, daemon=True).start()

        threading.Event().wait()
