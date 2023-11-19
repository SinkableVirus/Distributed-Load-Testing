import json
import random
import time
from kafka import KafkaProducer
from kafka import KafkaConsumer
from apscheduler.schedulers.background import BackgroundScheduler


class OrchestratorNode:
    def __init__(self):
        self.stop = 0
        self.metrics = {}
        self.heartbeat = {}
        self.activeNodeCount = 0
        self.producer = KafkaProducer(bootstrap_servers = "localhost:9092")
        self.consumer = KafkaConsumer("register", "metrics", "heartbeat")
        self.scheduler1 = BackgroundScheduler()
        self.scheduler1.start()
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
            self.delay = float(input("Enter the delay: "))
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
                self.activeNodeCount += 1
                print('Driver node', data['node_id'], 'registered')
                
            if message.topic == "metrics":
                self.metrics[data["node_id"]] = data["metrics"]

            if message.topic == "heartbeat":
                self.heartbeat[data["node_id"]] = time.time()
                if data["heartbeat"] == "NO":
                    self.activeNodeCount -= 1
                    if self.activeNodeCount == 0:
                        self.printMetrics()
                        exit(0)


if __name__ == "__main__":
    try:
        orchestrator = OrchestratorNode()
        orchestrator.testConfig()
        orchestrator.trigger()
        orchestrator.job1 = orchestrator.scheduler1.add_job(orchestrator.printMetrics, 'interval', seconds = 1)
        orchestrator.job2 = orchestrator.scheduler2.add_job(orchestrator.checkHeartbeat, 'interval', seconds = 1)
        orchestrator.listen()
    except KeyboardInterrupt:
        print("Orchestrator Stopped")