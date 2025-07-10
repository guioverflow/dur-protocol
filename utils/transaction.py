
import random
import time
import uuid

from utils import load_replicas
from utils.unicast import request, receive, send as unicast_send
import abcast

class Transaction:
    def __init__(self, cid):
        self.cid = cid
        self.tid = str(uuid.uuid4())
        self.read_set = []
        self.write_set = []

        self.replica = random.choice(load_replicas())

        self.result = None
    
    def _check_finished():
        if self.result:
            print(f"Transação já finalizou com resultado: {self.result}")
            return {'type': self.result, 'tid': self.tid}

    def read(self, key, think_time=0):
        _check_finished()
        time.sleep(think_time)

        # primeiro procura no write_set
        for search_key, value, version in reversed(self.write_set): # retorna o valor escrito mais recente
            if search_key == key:
                return {'type': "READ", 'key': key, 'value': value, 'version': version}

        value, version = request(*self.replica, {'type': "READ", 'key': key, 'cid': self.cid})
        self.read_set.append((value, version))


    def write(self, key, value, think_time=0):
        _check_finished()
        time.sleep(think_time)
        self.write_set.append((key, value))
        
        return {'type': "WRITE", 'key': key, 'value': value}

    def abort(self):
        _check_finished()
        self.result = "ABORTED"

        return {'type': self.result, 'tid': self.tid}

    def commit(self):
        abcast.send({
            'type': 'COMMIT_REQ',
            'cid': self.cid,
            'tid': self.tid,
            'rs': self.read_set,
            'ws': self.write_set
        })

        # quem manda é a replica?
        # como sincronizar com abcast?
        # pode ser qualquer uma
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.listen(1)
            self.result = receive(conn) 
        
        return {'type': self.result, 'tid': self.tid}
