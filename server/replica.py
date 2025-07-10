
from utils import get_sequencer
import threading

class Replica:
    def __init__(self, host, port, sequencer_port):
        self.host = host
        self.port = port
        _, self.sequencer_port = get_sequencer()

        self.data_store = dict() # key-store
        self.last_commited = 0
        self.lock = threading.Lock()

    def handle_read(self, conn, addr):
        # {
        #     "type": "READ",
        #     "cid": 4001,
        #     "key": "gui"
        # }

        with conn:
            data = conn.recv(4096)
            if not data:
                return
            message = json.loads(data.decode())

        cid = message['cid']
        key = message['key']

        value, version = self.data_store.get(key, (None, 0))

        self.send(*addr, {"key": key, "value": value, "version": version})

    def handle_commit(self, message, addr):
        # {
        #     "type": "COMMIT"
        #     "com_req": 4,
        #     "cid": 4001, # Porta 
        #     "tid": 123,
        #     "rs": [("gui", 40, 3), ("gui", 42, 4), ("gui", 100, 5)],
        #     "ws": [("gui", 123), ("teste", 456)]
        # }

        com_req = message['com_req']
        cid = message['cid']
        tid = message['tid']
        rs = message['rs']
        ws = message['ws']

        abort = False
        with self.lock:
            for key, rs_version in rs:
                current_value, current_version = self.data_store.get(key, (None, 0))
                if current_version > rs_version:
                    print(f"[Replica {self.port}] Abortando transação {tid} (stale).")
                    abort = True

            if not abort:
                self.last_commited += 1
                for key, value in ws:
                    old_value, old_version = self.data_store.get(key, (None, 0))
                    self.data_store[key] = (value, old_version + 1)
                print(f"[Replica {self.port}] Transação {tid} commitada req={com_req}.")
    
        response = {
            "tid": tid,
            "status": 'ABORTED' if abort else 'COMMITED'
        }

        self.send(*addr, response)
    
    def start():

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
            
