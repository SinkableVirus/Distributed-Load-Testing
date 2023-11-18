import socket
import threading


def hanldeRequests(connection):
    while True:
        request = connection.recv(1024).decode()
        if not request:
            break
        response = "Hello"
        connection.send(response.encode())
        print(connection)

    connection.close()


if __name__ == "__main__":
    host = socket.gethostname()
    port = 8080

    serverSocket = socket.socket()
    serverSocket.bind((host, port))

    numberOfDrivers = 8

    serverSocket.listen(numberOfDrivers)

    while True:
        connections = []
        addresses = []
        threads = []

        for i in range(numberOfDrivers):
            connection, address = serverSocket.accept()
            connections.append(connection)
            addresses.append(address)
            thread = threading.Thread(target = hanldeRequests, args = (connection, ))
            threads.append(thread)

        for i in range(numberOfDrivers):
            threads[i].start()
        
        for i in range(numberOfDrivers):
            threads[i].join()