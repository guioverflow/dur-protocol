
from utils.operation import Operation
import threading

class Server:
    def __init__(self, port, sequencer_port):
        self.port = port
        self.last_commited = 0
        self.data_store = dict() # key-store
        self.lock = threading.Lock()

        self.sequencer_port = sequencer_port
        self.seq = 0
        self.buffer = {}

    def send(message, port):
        import socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            with client_socket.connect(('localhost', port)) as conn:
                client_socket.sendall(message.encode())
        except Exception as e:
            print(f"Mensagem falhou {ip}:{port} - {e}")

    def receive():
        import socket

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('localhost', self.port))

        server_socket.listen(1)
        conn, addr = server_socket.accept()

        data_bytes = conn.recv(4096)
        data = data_bytes.decode()

        if type(data) != tuple or len(data) != 3:
            return None

        operation, key, client_port = data

        return (operation, key, client_port)


    def handle_message(self, conn, addr):
        with conn:
            data = conn.recv(4096)
            if not data:
                return
            message = json.loads(data.decode())
            if message['type'] == 'COMMIT':
                self.handle_commit(message)
            
            if message['type'] == 'READ':
                self.handle_read(message)

    def handle_read(self, message):
        """
        {
            "type": "READ",
            "cid": 4001, # Porta
            "key": "gui"
        }
        """

        cid = message['cid']
        key = message['key']

        value, version = self.data_store.get(key, (None, 0))

        response = {
            "key": key,
            "value": value,
            "version": version
        }

        self.send(response, cid)

    def handle_commit(self, message):
        """
        {
            "type": "COMMIT"
            "com_req": 4,
            "cid": 4001, # Porta 
            "tid": 123,
            "rs": [("gui", 40, 3), ("gui", 42, 4), ("gui", 100, 5)],
            "ws": [("gui", 123), ("teste", 456)]
        }
        """

        com_req = message['com_req']
        cid = message['cid']
        tid = message['tid']
        rs = message['rs']
        ws = message['ws']
        with self.lock:
            for key, rs_version in rs:
                current_value, current_version = self.data_store.get(key, (None, 0))
                if current_version > rs_version:
                    print(f"[Replica {self.port}] Abortando transação {tid} (stale).")
                    # self.abort = True
                    return

            self.last_commited += 1
            for key, value in ws:
                old_value, old_version = self.data_store.get(key, (None, 0))
                self.data_store[key] = (value, old_version + 1)
            print(f"[Replica {self.port}] Transação {tid} commitada req={com_req}.")
        
        response = {
            "type": "COMMIT",
            "tid": tid
        }

        self.send(response, cid)
    
    def start():
        self.send({'type': "JOIN"}, self.sequencer_port)

        # thread_deliver 
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"[Sistema] Nova réplica instanciada {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_message, args=(conn, addr)).start()


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
            
