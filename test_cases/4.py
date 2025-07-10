
"""
WRITE, ESPERA, READ

Uma Transação T1 faz um write, espera um tempo, e depois faz read e commit.
Mas na espera, uma transação T2 efetua operações e já commita.

Resultado esperado: Commit com sucesso, pois apesar do WRITE de T1 ter sido feito antes (cronologicamente),
chega ao servidor depois do WRITE de T2, garantindo serializabilidade e consistência no READ de T1.
"""

import threading
import time
from utils.transaction import Transaction

def tx1():
    t = Transaction(cid=4051)
    t.write("x", 100, think_time=1)
    
    print("[TX1] Esperando...")
    time.sleep(4)

    x = t.read("x", think_time=1)
    print(f"[TX1] {x}")
    
    t.commit()
    print("[TX1] Result:", t.result)

def tx2():
    time.sleep(2) # Começa após o write de T1, mas antes do read de T1
    t = Transaction(cid=4052)
    t.write("x", 555, think_time=1)
    t.commit()
    print("[TX2] Result:", t.result)

t1 = threading.Thread(target=tx1)
t2 = threading.Thread(target=tx2)

t1.start()
t2.start()

t1.join()
t2.join()
