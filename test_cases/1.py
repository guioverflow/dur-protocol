
"""
WRITES ALTERNADOS

Duas transações aplicam alternadamente writes em uma mesma chave.
Ambas conseguem commitar, mas apenas a última transação "vence" a disputa e tem seu valor gravado.
"""

import threading
import time
from utils.transaction import Transaction

def tx1():
    t = Transaction(cid=4051)
    t.write("x", 20, think_time=1)
    t.write("x", 20, think_time=2)
    t.write("x", 20, think_time=2)

    t.commit()
    print("[TX1] Result:", t.result)

def tx2():
    t = Transaction(cid=4052)
    t.write("x", 30, think_time=2)
    t.write("x", 30, think_time=2)
    t.write("x", 30, think_time=2)

    t.commit()
    print("[TX2] Result:", t.result)


t1 = threading.Thread(target=tx1)
t2 = threading.Thread(target=tx2)

t1.start()
t2.start()

t1.join()
t2.join()

