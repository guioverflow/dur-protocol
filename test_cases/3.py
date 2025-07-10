
"""
READ, WRITE, E DEMORA NO COMMIT

Uma Transação T1 faz um read e write, mas "esquece" de fazer commit.
Nesse tempo, uma transação T2 efetua operações e já commita.
Após alguns segundos, T1 "lembra" que não commitou e tenta aplicar o commit.

Resultado esperado: Abort em T1, pois o read foi feito no datastore com uma versão anterior ao aplicado por T2
"""


import threading
import time
from utils.transaction import Transaction

def tx1():
    t = Transaction(cid=4051)
    x = t.read("x", think_time=1)
    print(f"[TX1] {x}")
    t.write("x", 100, think_time=1)
    
    print("[TX1] Usuário esqueceu de commitar")
    time.sleep(5)
    
    t.commit()
    print("[TX1] Result:", t.result)

def tx2():
    time.sleep(3)
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