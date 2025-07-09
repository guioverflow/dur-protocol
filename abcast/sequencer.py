
import socket
import threading
import json
import random

class AbcastChannel:
    def __init__(self, port):
        self.host = 'localhost'
        self.port = port
        self.replicas = list()
        self.seq = 0
        self.lock = threading.Lock()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"[Sequencer] Escutando em {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_request, args=(conn, addr)).start()

    def send(message, port):
        import socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            with client_socket.connect(('localhost', port)) as conn:
                client_socket.sendall(message.encode())
        except Exception as e:
            print(f"Mensagem falhou {ip}:{port} - {e}")

    def handle_request(self, conn, addr):
        with conn:
            data = conn.recv(4096)
            if not data: return
            message = json.loads(data.decode())

            # if message['type'] == 'JOIN':
            #     ip, port = addr
            #     self.replicas.append((ip, port))
            #     print(f"[Sequencer] {addr} se juntou ao grupo.")

            # if message['type'] == 'EXIT':
            #     if addr in self.replicas:
            #         self.replicas.remove(addr)
            #         print(f"[Sequencer] {addr} saiu do grupo.")

            # if message['type'] == 'CHOOSE':
            #     choosen_replica = random.choice(self.replicas)
            #     s_host, s_port = choosen_replica
            #     c_host, c_port = addr

            #     response = {
            #         'host': s_host,
            #         'port': s_port
            #     }

            #     self.send(response, c_port)

            if message['type'] == 'BCAST':
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

