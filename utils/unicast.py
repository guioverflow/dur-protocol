
import socket

def send(host, port, message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(message.encode())
    except Exception as e:
        print(f"[Erro] {ip}:{port} - {e}")

def receive(conn):
    with conn:
        data = conn.recv(4096)
        if data:
            return data.decode()
        return None

def request(host, port, message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(message.encode())
            response = s.recv(4096)
            return response.decode()
    except Exception as e:
        print(f"[Erro] {ip}:{port} - {e}")
        return None
