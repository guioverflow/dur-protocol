
"""
READS ALTERNADOS

Duas transações aplicam alternadamente reads em uma mesma chave.
Ambas conseguem commitar, e como não há nenhuma escrita, ambas passam da validação de readset.
"""

import threading
import time
from utils.transaction import Transaction

def tx1():
    t = Transaction(cid=4051)

    x = t.read("x", think_time=1)
    print(f"[TX1] {x}")

    x = t.read("x", think_time=2)
    print(f"[TX1] {x}")

    x = t.read("x", think_time=2)
    print(f"[TX1] {x}")

    t.commit()
    print("[TX1] Result:", t.result)

def tx2():
    t = Transaction(cid=4052)

    x = t.read("x", think_time=2)
    print(f"[TX2] {x}")

    x = t.read("x", think_time=2)
    print(f"[TX2] {x}")

    x = t.read("x", think_time=2)
    print(f"[TX2] {x}")

    t.commit()
    print("[TX2] Result:", t.result)


t1 = threading.Thread(target=tx1)
t2 = threading.Thread(target=tx2)

t1.start()
t2.start()

t1.join()
t2.join()

