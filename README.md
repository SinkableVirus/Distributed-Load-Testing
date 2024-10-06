# Distributed Load Testing

* Distributed load-testing system that co-ordinates between multiple
driver nodes to run a highly concurrent, high-throughput load test on a
web server.
* Kafka as a communication service.

# Command to start Zookeeper and Kafka

*sudo /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties &
*sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &
