
import socket
import threading
import json
import random

from utils import load_replicas, get_sequencer
from utils.unicast import receive, send as unicast_send

class AbcastChannel:
    def __init__(self):
        self.host, self.port = get_sequencer()
        self.replicas = load_replicas()

        self.seq = 0
        self.lock = threading.Lock()

    def start(self):
        threading.Thread(target=self.listen_requests, daemon=True).start()

    def listen_requests(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"[Sequencer] Escutando em {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                message = receive(conn)

                with self.lock:
                    self.seq += 1
                    print(f"[Sequencer] Recebido mensagem {message} de {addr}. seq={self.seq}")
                    message['seq'] = self.seq
                self.broadcast(message)

    def broadcast(self, message):
        print(f"[Sequencer] Realizando Atomic Broadcast")
        for host, _, abcast_port in self.replicas:
            unicast_send(host, abcast_port, message)

