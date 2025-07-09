
import configparser


def load_replicas(conf='replicas.conf'):
    config = configparser.ConfigParser()
    config.read(conf)

    replicas = list()
    for section in config.sections():
        if section == 'sequencer': continue

        name = section
        host = config[section]['host']
        port = config[section]['port']

        replicas.append((name, host, port))

    return replicas

def get_sequencer(conf='replicas.conf'):
    config = configparser.ConfigParser()
    config.read(conf)

    host = config['sequencer']['host']
    port = config['sequencer']['port']
    return (host, port)
