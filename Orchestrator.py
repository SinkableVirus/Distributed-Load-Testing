import json
import random
import time
from kafka import KafkaProducer
from kafka import KafkaConsumer
from apscheduler.schedulers.background import BackgroundScheduler
import statistics


class OrchestratorNode:
    def __init__(self):
        self.metrics = {}
        self.heartbeat = {}
        self.activeNodeCount = 0
        self.producer = KafkaProducer(bootstrap_servers = "localhost:9092")
        self.consumer = KafkaConsumer("register", "metrics", "heartbeat")
        self.scheduler1 = BackgroundScheduler()
        self.scheduler1.start()
        self.scheduler2 = BackgroundScheduler()
        self.scheduler2.start()

        # self.aggregated_metrics = {
        #     "min_latency": [],
        #     "max_latency": [],
        #     "mean_latency": [],
        #     "median_latency": [],
        #     "mode_latency": []
        # }

        self.totalRequests = 0
        self.meanSum = 0
        self.medianSum = 0
        self.modeSum = 0

        self.overall_metrics = {
            "min_latency": 0,
            "max_latency": 0,
            "mean_latency": 0,
            "median_latency": 0,
            "mode_latency": 0
        }


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
        print(" Driver Wise Metrics ")
        print()
        print(json.dumps(self.metrics, indent = 4), flush = True)
        print()

        print(" Overall aggregated metrics ")
        print(json.dumps(self.overall_metrics, indent = 4), flush = True)
        print()


    
    
    def checkHeartbeat(self):
        for i in self.heartbeat:
            if(time.time() - self.heartbeat[i] > 1.5):
                print(f'Heartbeat from node {i} not received', flush = True)
            else:
                print(f'Heartbeat from node {i} received', flush = True)



    def listen(self):
        for message in self.consumer:
            data = json.loads(message.value.decode("utf-8"))

            if message.topic == "register":
                self.metrics[data["node_id"]] = {}
                self.activeNodeCount += 1
                print('Driver node', data['node_id'], 'registered', flush = True)
                
            if message.topic == "metrics":
                self.metrics[data["node_id"]] = data["metrics"]

                self.totalRequests += 1
                self.meanSum += data["metrics"]["mean_latency"]
                self.medianSum += data["metrics"]["median_latency"]
                self.modeSum += data["metrics"]["mode_latency"]

                self.overall_metrics["mean_latency"] = self.meanSum / self.totalRequests
                self.overall_metrics["median_latency"] = self.medianSum / self.totalRequests
                self.overall_metrics["mode_latency"] = self.modeSum / self.totalRequests

                if self.overall_metrics["min_latency"] == 0 or self.overall_metrics["min_latency"] > data["metrics"]["min_latency"]:
                    self.overall_metrics["min_latency"] = data["metrics"]["min_latency"]

                if self.overall_metrics["max_latency"] < data["metrics"]["max_latency"]:
                    self.overall_metrics["max_latency"] = data["metrics"]["max_latency"]

                # self.aggregated_metrics['mean_latency'].append(data['metrics']['mean_latency'])
                # self.aggregated_metrics['min_latency'].append(data['metrics']['min_latency'])
                # self.aggregated_metrics['max_latency'].append(data['metrics']['max_latency'])
                # self.aggregated_metrics['median_latency'].append(data['metrics']['median_latency'])
                # self.aggregated_metrics['mode_latency'].append(data['metrics']['mode_latency'])


                # self.overall_metrics['mean_latency'] = statistics.mean(self.aggregated_metrics['mean_latency'])
                # self.overall_metrics['min_latency'] = min(self.aggregated_metrics['min_latency'])
                # self.overall_metrics['max_latency'] = max(self.aggregated_metrics['max_latency'])
                # self.overall_metrics['median_latency'] = statistics.mean(self.aggregated_metrics['median_latency'])
                # self.overall_metrics['mode_latency'] = statistics.mean(self.aggregated_metrics['mode_latency'])
                

            if message.topic == "heartbeat":
                if data["heartbeat"] == "YES":
                    self.heartbeat[data["node_id"]] = time.time()
                if data["heartbeat"] == "NO":
                    self.activeNodeCount -= 1
                    if self.activeNodeCount == 0:
                        self.job1.remove()
                        self.job2.remove()
                        self.printMetrics()
                        self.metrics = {}
                        self.aggregated_metrics = {
                            "mean_latency": [],
                            "median_latency": [],
                            "min_latency": [],
                            "max_latency": [],
                            "mode_latency": [],
                        }
                        self.overall_metrics = {
                            "mean_latency": 0,
                            "median_latency": 0,
                            "min_latency": 0,
                            "max_latency": 0,
                            "mode_latency": 0,
                        }
                        break

        


if __name__ == "__main__":
    orchestrator = OrchestratorNode()
    while True:
        try:
            orchestrator.testConfig()
            orchestrator.trigger()
            orchestrator.job1 = orchestrator.scheduler1.add_job(orchestrator.printMetrics, 'interval', seconds = 1)
            orchestrator.job2 = orchestrator.scheduler2.add_job(orchestrator.checkHeartbeat, 'interval', seconds = 1)
            orchestrator.listen()
        except KeyboardInterrupt:
            print("Orchestrator Stopped")
            break