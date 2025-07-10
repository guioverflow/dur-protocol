
import configparser

def load_data_store():
    return {
        'gui': (123, 1),
        'x': (20, 1),
        'y': (30, 1),
        'z': (50, 1)
    }

def load_replicas(conf='replicas.conf'):
    config = configparser.ConfigParser()
    config.read(conf)

    replicas = list()
    for section in config.sections():
        host = config[section]['host']
        read_port = int(config[section]['read_port'])
        abcast_port = int(config[section]['abcast_port'])

        replicas.append((host, read_port, abcast_port))

    return replicas

def get_sequencer(conf='sequencer.conf'):
    config = configparser.ConfigParser()
    config.read(conf)

    host = config['sequencer']['host']
    port = int(config['sequencer']['port'])
    return (host, port)

def choose_replica():
    import random

    replicas = load_replicas()
    chosen_replica = random.choice(replicas)
    
    # Cliente não precisa do abcast_port
    host, read_port, _ = chosen_replica
    return (host, read_port)