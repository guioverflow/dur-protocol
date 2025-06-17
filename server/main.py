
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
        self.data = dict() # chave = (valor, versão)

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
            operation, key, client_port = receive() # Colocar em uma thread
            if operation == Operation.READ:
                send(self.data[key], client_port)
            
            com_req, cid, tid, read_set, write_set = abcast.deliver() # Colocar em uma thread
            for i, read_item in enumerate(read_set):
                if self.data[read_item][1] > read_set[i][1]:
                    send((Operation.ABORT, tid), cid)
                    abort = True
            
            if not abort:
                self.last_commited += 1
                for j, write_item in enumerate(write_set):
                    self.data[write_item[1]] += 1
                    self.data[write_item[0]]
                send((Operation.COMMIT), tid)
            

