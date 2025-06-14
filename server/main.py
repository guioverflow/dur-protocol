
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
        pass

    def receive():

        return (Operation.COMMIT, key, cid)

    
    def start():
        while True:
            request = receive()
            if request[0] == Operation.COMMIT:
                send(self.data[key], port)
            
            