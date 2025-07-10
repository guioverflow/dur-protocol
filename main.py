
import threading

from server.replica import Replica
from abcast.sequencer import AbcastChannel
from utils import load_replicas, get_sequencer

def start_sequencer():
    host, port = get_sequencer()
    print(f"[Sistema] Iniciando sequenciador em host={host}, port={port}")
    s = AbcastChannel()
    s.start()

def start_replica(host, read_port, abcast_port):
    print(f"[Sistema] Iniciando replica em host={host}, read_port={read_port}, abcast_port={abcast_port}")
    r = Replica(host, read_port, abcast_port)
    r.start()


threads = []
for host, read_port, abcast_port in load_replicas():
    t = threading.Thread(target=start_replica, args=(host, read_port, abcast_port), daemon=True)
    threads.append(t)

t = threading.Thread(target=start_sequencer, daemon=True)
threads.append(t)

print("[Sistema] Iniciando Threads")
for thread in threads:
    thread.start()

threading.Event().wait()  # mantém o processo vivo

