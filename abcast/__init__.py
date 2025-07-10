
import configparser
from utils.unicast import receive, send as unicast_send
from utils import get_sequencer

def send(message):
    seq_host, seq_port = get_sequencer()
    unicast_send(seq_host, seq_port, message, id='ABCAST')

def deliver(conn):
    return receive(conn, id='ABCAST')
