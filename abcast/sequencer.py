
import socket
import threading
import json
import random

from utils import load_replicas, get_sequencer
from utils.unicast import send as unicast_send

class AbcastChannel:
    def __init__(self):
        self.host, self.port = get_sequencer()
        self.replicas = load_replicas()

        self.seq = 0
        self.lock = threading.Lock()

    def start(self):
        threading.Thread(target=self.listen_requests, args=(conn,)).start()

    def listen_requests(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"[Sequencer] Escutando em {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                message = deliver(conn)

                with self.lock:
                    self.seq += 1
                    message['seq'] = self.seq
                self.broadcast(message)

    def broadcast(self, message):
        for host, port in self.replicas:
            unicast_send(host, port, message)

