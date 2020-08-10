import socket

server = socket.socket()
server.bind(('localhost', 9999))
server.listen(1)

while True:
    print("Waiting for the connecting")
    conn, addr = server.accept()
    print("Connection success! Connection is from %s"%addr[0])
    print("Sending data ...")
    conn.send("A small breakfast-room adjoined the drawing-room, I slipped inthere. It contained a bookcase".encode())
    conn.close()
    print("Connection is broken")
