
import random
import time
import uuid
import socket

from utils import choose_replica
from utils.unicast import request, receive, send as unicast_send
import abcast

class Transaction:
    def __init__(self, cid):
        self.cid = cid
        self.tid = str(uuid.uuid4())
        self.read_set = []
        self.write_set = []

        self.replica = choose_replica()

        self.result = None
    
    def _check_finished(self):
        if self.result:
            print(f"[Cliente {self.cid}] Transação já finalizou com resultado: {self.result}")
            return {'type': self.result, 'tid': self.tid}

    def scan(self, think_time=0):
        print(f"[Cliente {self.cid}] Efetuando SCAN no datastore")
        time.sleep(think_time)

        response = request(*self.replica, {'type': "SCAN", 'cid': self.cid})
        response = {k: tuple(v) for k, v in response.items()} # JSON não suporta tuplas, dessa forma é necessário normalizar
        return response

    def read(self, key, think_time=0):
        print(f"[Cliente {self.cid}] Efetuando leitura na chave {key}")
        self._check_finished()
        time.sleep(think_time)

        # primeiro procura no write_set
        for search_key, value in reversed(self.write_set): # retorna o valor escrito mais recente
            if search_key == key:
                print(f"[Cliente {self.cid}] Valor encontrado no read_set")
                return {'type': "READ", 'key': key, 'value': value}

        print(f"[Cliente {self.cid}] Valor não encontrado localmente, requisitando valor ao banco")
        value, version = request(*self.replica, {'type': "READ", 'key': key, 'cid': self.cid})
        self.read_set.append((key, value, version))


    def write(self, key, value, think_time=0):
        print(f"[Cliente {self.cid}] Efetuando escrita na chave '{key}' com valor '{value}'")
        self._check_finished()
        time.sleep(think_time)
        self.write_set.append((key, value))
        
        return {'type': "WRITE", 'key': key, 'value': value}

    def abort(self):
        print(f"[Cliente {self.cid}] Abortando transação {self.tid}")
        self._check_finished()
        self.result = "ABORTED"

        return {'type': self.result, 'tid': self.tid}

    def commit(self):
        print(f"[Cliente {self.cid}] Commitando transação {self.tid}")
        self._check_finished()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', self.cid))
            s.settimeout(7)
            s.listen(1)

            abcast.send({
                'type': 'COMMIT_REQ',
                'cid': self.cid,
                'tid': self.tid,
                'rs': self.read_set,
                'ws': self.write_set
            })

            try: conn, addr = s.accept()
            except socket.timeout:
                print(f"[Cliente {self.cid}] Espera do commit sofreu timeout")
                self.result = "UNDEFINED"
            else:
                self.result = receive(conn)

        # quem manda é a replica?
        # como sincronizar com abcast?
        # pode ser qualquer uma
        
        return {'type': self.result, 'tid': self.tid}
