
import socket
import json

def send(host, port, message):
    print(f"[Unicast.request] Enviando para {host}:{port} a mensagem {message}")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(json.dumps(message).encode())
    except Exception as e:
        print(f"[Erro] {host}:{port} - {e}")

def receive(conn, close=True):
    message = conn.recv(4096)
    if close: conn.close()
    if not message:
        print(f"[Unicast.receive] Mensagem vazia")
        return None

    try:
        message = json.loads(message.decode())

        # json.dump e json.load tratam tuplas como lista, necessário normalizar
        if isinstance(message, dict):
            if 'ws' in message:
                message['ws'] = [tuple(item) for item in message['ws']]
            if 'rs' in message:
                message['rs'] = [tuple(item) for item in message['rs']]
        
        print(f"[Unicast.receive] Mensagem recebida: {message}")
        return message

    except json.JSONDecodeError as e:
        print(f"[Unicast.receive] Falha ao decodificar JSON: {e}")

def request(host, port, message):
    print(f"[Unicast.request] Enviando para {host}:{port} a mensagem {message}")

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(json.dumps(message).encode())
            response = s.recv(4096)
            
            return json.loads(response.decode())
    except Exception as e:
        print(f"[Erro] {host}:{port} - {e}")
        return None
