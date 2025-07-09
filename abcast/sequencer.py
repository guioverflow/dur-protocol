
import socket
import threading
import json
import random

from utils import load_replicas, get_sequencer

class AbcastChannel:
    def __init__(self):
        self.host, self.port = get_sequencer()
        self.replicas = load_replicas()

        self.seq = 0
        self.lock = threading.Lock()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"[Sequencer] Escutando em {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_request, args=(conn,)).start()

    def handle_request(self, conn):
        with conn:
            data = conn.recv(4096)
            if not data: return
            message = json.loads(data.decode())

            with self.lock:
                self.seq += 1
                message['seq'] = self.seq
            self.broadcast(message)

    def broadcast(self, message):
        for ip, port in self.replicas:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((ip, port))
                    s.sendall(json.dumps(message).encode())
            except Exception as e:
                print(f"[Sequencer] Falha no envio para {ip}:{port} - {e}")

