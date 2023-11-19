import json
import random
import time
from kafka import KafkaProducer
from kafka import KafkaConsumer
from apscheduler.schedulers.background import BackgroundScheduler


class OrchestratorNode:
    def __init__(self):
        self.metrics = {}
        self.heartbeat = {}
        self.producer = KafkaProducer(bootstrap_servers = "localhost:9092")
        self.consumer = KafkaConsumer("register", "metrics", "heartbeat")
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.scheduler2 = BackgroundScheduler()
        self.scheduler2.start()

    def testConfig(self):
        self.testID = random.randint(0, 100)
        testType = input("Enter the test type (A/T): ")
        if testType == "A":
            self.testType = "AVALANCHE"
            self.messageCount = int(input("Enter the message count per driver: "))
            self.delay = 0
        elif testType == "T":
            self.testType = "TSUNAMI"
            self.delay = int(input("Enter the delay: "))
            self.messageCount = int(input("Enter the message count per driver: "))
        self.producer.send("test_config", json.dumps({
            "test_id": self.testID,
            "test_type": self.testType,
            "test_message_delay": self.delay,
            "message_count_per_driver": self.messageCount,
            "target_server_ip": "http://127.0.0.1:8080/ping"
        }).encode("utf-8"))
        self.producer.flush()

    def trigger(self):
        self.producer.send("trigger", json.dumps({
            "test_id": self.testID,
            "trigger": "YES"
        }).encode("utf-8"))
        self.producer.flush()

    def printMetrics(self):
        print(json.dumps(self.metrics, indent = 4))
    
    def checkHeartbeat(self):
        for i in self.heartbeat:
            if(time.time() - self.heartbeat[i] > 1.5):
                print(f'Heartbeat from node {i} not received')
            else:
                print(f'Heartbeat from node {i} received')


    def listen(self):
        for message in self.consumer:
            data = json.loads(message.value.decode("utf-8"))
            if message.topic == "register":
                self.metrics[data["node_id"]] = {}
            
            if message.topic == "metrics":
                self.metrics[data["node_id"]] = data["metrics"]

            if message.topic == "heartbeat":
                # if data["node_id"] not in self.checkHeartbeat:
                self.heartbeat[data["node_id"]] = time.time()
                # else:
                #     if (time.time() - self.checkHeartbeat[data["node_id"]]) > 1.5:
                #         print(f'Received heartbeat from node {data["node_id"]} late')
                #     else:
                #         print(f'Received heartbeat from node {data["node_id"]} on time')
                #     self.checkHeartbeat[data["node_id"]] = time.time()

                



if __name__ == "__main__":
    orchestrator = OrchestratorNode()
    orchestrator.testConfig()
    orchestrator.trigger()
    orchestrator.scheduler.add_job(orchestrator.printMetrics, 'interval', seconds = 1)
    orchestrator.scheduler2.add_job(orchestrator.checkHeartbeat, 'interval', seconds = 1)
    orchestrator.listen()