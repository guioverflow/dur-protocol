
import socket
import json

def send(host, port, message, id='Unicast.send'):
    print(f"[{id}] Enviando para {host}:{port} a mensagem {message}")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(json.dumps(message).encode())
    except Exception as e:
        print(f"[{id}] Erro ao tentar fazer send para {host}:{port}")

def receive(conn, close=True, id='Unicast.receive'):
    message = conn.recv(4096)
    if close: conn.close()
    if not message:
        print(f"[{id}] Mensagem vazia")
        return None

    try:
        message = json.loads(message.decode())

        # json.dump e json.load tratam tuplas como lista, necessário normalizar
        if isinstance(message, dict):
            if 'ws' in message:
                message['ws'] = [tuple(item) for item in message['ws']]
            if 'rs' in message:
                message['rs'] = [tuple(item) for item in message['rs']]
        
        # print(f"[{id}] Mensagem recebida: {message}")
        return message

    except json.JSONDecodeError as e:
        print(f"[{id}] Falha ao decodificar JSON: {e}")

def request(host, port, message, id='Unicast.request'):
    print(f"[{id}] Enviando para {host}:{port} a mensagem {message}")

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(json.dumps(message).encode())
            response = s.recv(4096)
            
            return json.loads(response.decode())
    except Exception as e:
        print(f"[{id}] Erro ao tentar request para {host}:{port} - {e}")
        return None
