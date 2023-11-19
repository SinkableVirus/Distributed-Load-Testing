import sys
import random
import json
import socket
import time
import statistics
import threading
import requests
# import schedule
from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaProducer
from kafka import KafkaConsumer


class DriverNode:
    def __init__(self):
        self.id = random.randint(1, 1000)
        self.IP = "127.0.0.1"
        self.metrics = {
            "mean_latency": 0,
            "median_latency": 0,
            "min_latency": 0,
            "max_latency": 0,
            "number_of_requests": 0
        }
        self.latencies = []
        self.totalLatency = 0
        self.producer = KafkaProducer(bootstrap_servers = "localhost:9092")
        self.consumer = KafkaConsumer("test_config", "trigger")
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    # def connect(self):
    #     self.host = socket.gethostname()
    #     self.serverPort = 8080
    #     self.clientSocket = socket.socket()
    #     self.clientSocket.connect((self.host, self.serverPort))
    #     self.IP = self.clientSocket.getsockname()[0] + ":" + str(self.clientSocket.getsockname()[1])

    def request(self, message):
        start = time.time()
        # self.clientSocket.send(message.encode())
        # self.response = self.clientSocket.recv(1024).decode()
        self.response = requests.get(self.serverIP)
        end = time.time()
        print(self.response)
        latency = end - start
        if self.metrics["min_latency"] == 0:
            self.metrics["min_latency"] = latency
        if latency < self.metrics["min_latency"]:
            self.metrics["min_latency"] = latency
        if latency > self.metrics["max_latency"]:
            self.metrics["max_latency"] = latency
        self.latencies.append(latency)
        self.totalLatency += latency
        self.metrics["mean_latency"] = self.totalLatency / len(self.latencies)
        self.metrics["median_latency"] = statistics.median(self.latencies)
        self.metrics["number_of_requests"] += 1


    def register(self):
        self.producer.send("register", json.dumps({
            "node_id": self.id,
            "node_IP": self.IP,
            "message_type": "DRIVER_NODE_REGISTER"
        }).encode("utf-8"))
        self.producer.flush()

    def publishMetrics(self):
        self.producer.send("metrics", json.dumps({
            "node_id": self.id,
            "test_id": 0,
            "report_id": random.randint(10000, 99999),
            "metrics": {
                "mean_latency": self.metrics["mean_latency"],
                "median_latency": self.metrics["median_latency"],
                "min_latency": self.metrics["min_latency"],
                "max_latency": self.metrics["max_latency"],
                "number_of_requests": self.metrics["number_of_requests"]
            }
        }).encode("utf-8"))
        self.producer.flush()
        print("published")

    def sendHeatbeat(self):
        self.producer.send("heartbeat", json.dumps({
            "node_id": self.id,
            "heartbeat": "YES"
        }).encode("utf-8"))
        current_time = time.strftime("%H:%M:%S", time.localtime())
        print(current_time)
        print("sent", self.id)

    def startTest(self, delay = 0):
        # self.connect()
        self.register()
        self.scheduler.add_job(self.sendHeatbeat, 'interval', seconds = 1)
        while True:
            self.request("Hi")
            self.publishMetrics()
            time.sleep(delay)

    def listen(self):
        for message in self.consumer:
            if message.topic == "test_config":
                data = json.loads(message.value.decode("utf-8"))
                self.testID = data["test_id"]
                self.testType = data["test_type"]
                self.delay = data["test_message_delay"]
                self.messageCount = data["message_count_per_driver"]
                self.serverIP = data["target_server_ip"]
                # print(data)
                
            if message.topic == "trigger":
                data = json.loads(message.value.decode("utf-8"))
                # print(data)
                if data["trigger"] == "YES":
                    if self.testType == "AVALANCHE":
                        self.startTest()
                    elif self.testType == "TSUNAMI":
                        self.startTest(self.delay)

    def displayMetrics(self):
        print("Latencies:", self.latencies)
        print("Min:", self.metrics["min_latency"])
        print("Max:", self.metrics["max_latency"])
        print("Mean:", self.metrics["mean_latency"])
        print("Median:", self.metrics["median_latency"])