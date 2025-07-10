
"""
INCREMENTO

Três transações leêm uma variável x.
T1 incremeta x+1 e tenta atualizar x.
T2 incrementa x+5 e tenta atualizar x.
T3 incrementa x+10, e grava numa variável temp_x.

As 3 tentam commitar, T1 e T2 disputam a gravação em x, e T3 tenta gravar em outra variável.
Resultado: Apenas uma das 3 commita, pois as 3 dependem de um valor de X que terá sua versão alterada.
"""


import threading
from utils.transaction import Transaction

def run_transaction_1():
    t = Transaction(cid=4051)

    x = t.read("x", think_time=0)
    print(f"[TX1] READ X: {x}")

    new_x = x[1] + 1
    print(f"[TX1] X NEW VALUE = {new_x}")
    t.write("x", new_x, think_time=2)

    t.commit()
    print(f"[TX1] Result: {t.result}")

def run_transaction_2():
    t = Transaction(cid=4052)

    x = t.read("x", think_time=1)
    print(f"[TX2] READ X: {x}")

    new_x = x[1] + 5
    print(f"[TX1] X NEW VALUE = {new_x}")

    t.write("x", new_x, think_time=1)
    t.commit()

    print(f"[TX2] Result: {t.result}")

def run_transaction_3():
    t = Transaction(cid=4053)

    x = t.read("x", think_time=1)
    print(f"[TX3] READ X: {x}")

    new_x = x[1] + 10
    print(f"[TX3] X NEW VALUE = {new_x}")

    t.write("temp_x", new_x, think_time=4)
    t.commit()

    print(f"[TX3] Result: {t.result}")

t1 = threading.Thread(target=run_transaction_1)
t2 = threading.Thread(target=run_transaction_2)
t3 = threading.Thread(target=run_transaction_3)

t1.start()
t2.start()
t3.start()

t1.join()
t2.join()
t3.join()
