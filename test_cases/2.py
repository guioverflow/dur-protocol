

from utils.transaction import Transaction


def run_transaction_1():
    t = Transaction(cid=10)
    t.write("x", 100, think_time=2)
    t.read("x", think_time=1)
    t.commit()

    print(f"TX1: {t.result}")
    

def run_transaction_2():
    t = Transaction(cid=20)

    temp = t.read("y", think_time=5)
    t.write("x", temp, think_time=1)
    t.commit()

    print(f"TX2: {t.result}")

t1 = threading.Thread(target=run_transaction_1)
t2 = threading.Thread(target=run_transaction_2)

t1.start()
t2.start()
t1.join()
t2.join()

