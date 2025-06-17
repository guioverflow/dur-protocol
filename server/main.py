
from enum import Enum
class Operation(Enum):
    READ = "READ"
    WRITE = "WRITE"
    ABORT = "ABORT"
    COMMIT = "COMMIT"

class Server:
    def __init__(self, port):
        self.port = port
        self.last_commited = 0
        self.data = dict() # chave = (valor, version)

    def send(message, port):
        import socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        with client_socket.connect(('localhost', port)) as conn:
            client_socket.sendall(message.encode())

    def receive():
        import socket

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('localhost', self.port))

        server_socket.listen(1)
        conn, addr = server_socket.accept()

        data_bytes = conn.recv(1024)
        data = data_bytes.decode()

        if type(data) != tuple or len(data) != 3:
            return None

        operation, key, client_port = data

        return (operation, key, client_port)

    
    def start():
        while True:
            operation, key, client_port = receive()
            if operation == Operation.READ:
                send(self.data[key], client_port)
            

