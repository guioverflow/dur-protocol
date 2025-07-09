
from utils.operation import Operation

class Transaction:

    def __init__(self, port, sequencer_port):
        self.port = port
        self.sequencer_port = sequencer_port
        self.replica = self.send({'type': 'CHOOSE'}, self.sequencer_port)

        self.ws = []
        self.rs = []

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

    def broadcast():
        pass

    def start():
        op = input('Operation:')
        while op != Operation.COMMIT and op != Operation.ABORT:
            if op == Operation.WRITE:
                item = input("Item: ")
                value = input("Value: ")
                self.ws.append((item, value))

            elif op == Operation.READ:
                read_item = input("Item: ")
                search = [item for item in self.ws if item[0] == read_item]
                
                if search: # item pertence ao ws
                    key, value = search[-1] # mais recente
                else:
                    request = {
                        'type': Operation.READ,
                        'cid': self.port,
                        'key': key
                    }

                    self.send(request, self.choosen_replica)


        if op == Operation.COMMIT:
            pass
        else:
            return Operation.ABORT
