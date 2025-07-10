

from utils.transaction import Transaction
import threading

def run_transaction():
    t = Transaction(cid=4050)
    t.write("x", 100, think_time=0) # 2
    t.read("x", think_time=0) # 1
    t.commit()

    print(f"TX1: {t.result}")


t1 = threading.Thread(target=run_transaction)

t1.start()
t1.join()
