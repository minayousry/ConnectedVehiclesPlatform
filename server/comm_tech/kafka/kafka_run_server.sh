#!/bin/bash

# Start Zookeeper
echo "Starting Zookeeper..."
zookeeper-server-start.sh $HOME/kafka/config/zookeeper.properties &

# Wait a bit for Zookeeper to fully start
sleep 30

# Start Kafka
echo "Starting Kafka..."
kafka-server-start.sh $HOME/kafka/config/server.properties &

# Wait a bit for Kafka to fully start
sleep 30
