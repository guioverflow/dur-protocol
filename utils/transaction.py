
import random
import time
import uuid
import socket
import json

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
    
    def _check_result(self):
        if self.result:
            print(f"[Cliente {self.cid}] Transação já finalizou com resultado: {self.result}")
            return (self.tid, self.result)
        return None

    def scan(self, think_time=0):
        result = self._check_result()
        if result: return result

        print(f"[Cliente {self.cid}] Efetuando SCAN no datastore")
        time.sleep(think_time)

        response = request(*self.replica, {'type': "SCAN", 'cid': self.cid}, id=f'Cliente {self.cid}')
        response = {k: tuple(v) for k, v in response.items()} # JSON não suporta tuplas, dessa forma é necessário normalizar
        return response

    def read(self, key, think_time=0):
        result = self._check_result()
        if result: return result

        print(f"[Cliente {self.cid}] Efetuando leitura na chave {key}")
        time.sleep(think_time)

        # primeiro procura no write_set
        for search_key, value in reversed(self.write_set): # retorna o valor escrito mais recente
            if search_key == key:
                print(f"[Cliente {self.cid}] Valor encontrado no read_set")
                return (key, value)

        print(f"[Cliente {self.cid}] Valor não encontrado localmente, requisitando valor ao banco")

        response = request(*self.replica, {'type': 'READ', 'key': key, 'cid': self.cid}, id=f'Cliente {self.cid}')

        value = response["value"]
        version = response["version"]

        self.read_set.append((key, value, version))
        return (key, value, version)


    def write(self, key, value, think_time=0):
        result = self._check_result()
        if result: return result

        print(f"[Cliente {self.cid}] Efetuando escrita na chave '{key}' com valor '{value}'")
        time.sleep(think_time)
        self.write_set.append((key, value))
        
        return (key, value)

    def abort(self):
        result = self._check_result()
        if result: return result

        print(f"[Cliente {self.cid}] Abortando transação {self.tid}")
        self.result = (self.tid, "ABORTED")

        return self.result

    def commit(self):
        result = self._check_result()
        if result: return result

        print(f"[Cliente {self.cid}] Commitando transação {self.tid}")

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
                response = receive(conn, id=f'Cliente {self.cid}')
                self.result = (response['tid'], response['status'])
        
        return self.result
