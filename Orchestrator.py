import json
import random
from kafka import KafkaProducer
from kafka import KafkaConsumer


class OrchestratorNode:
    def __init__(self):
        self.metrics = {}
        self.producer = KafkaProducer(bootstrap_servers = "localhost:9092")
        self.consumer = KafkaConsumer("register", "metrics", "heartbeat")

    def testConfig(self):
        self.testID = random.randint(0, 100)
        testType = input("Enter the test type (A/T): ")
        if testType == "A":
            self.testType = "AVALANCHE"
            self.delay = 0
        elif testType == "T":
            self.testType = "TSUNAMI"
            self.delay = int(input("Enter the delay: "))
        self.producer.send("test_config", json.dumps({
            "test_id": self.testID,
            "test_type": self.testType,
            "test_message_delay": self.delay,
        }).encode("utf-8"))
        self.producer.flush()

    def trigger(self):
        self.producer.send("trigger", json.dumps({
            "test_id": self.testID,
            "trigger": "YES"
        }).encode("utf-8"))
        self.producer.flush()

    def listen(self):
        for message in self.consumer:
            data = json.loads(message.value.decode("utf-8"))
            if message.topic == "register":
                self.metrics[data["node_id"]] = {}
            
            if message.topic == "metrics":
                self.metrics[data["node_id"]] = data["metrics"]
                print(json.dumps(self.metrics, indent = 4))


if __name__ == "__main__":
    orchestrator = OrchestratorNode()
    orchestrator.testConfig()
    orchestrator.trigger()
    orchestrator.listen()