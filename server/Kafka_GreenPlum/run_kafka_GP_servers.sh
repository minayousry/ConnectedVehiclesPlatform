#!/bin/bash

# Start Zookeeper
echo "Starting Zookeeper..."
zookeeper-server-start.sh $HOME/kafka/config/zookeeper.properties &

# Wait a bit for Zookeeper to fully start
sleep 5

# Start Kafka
echo "Starting Kafka..."
kafka-server-start.sh $HOME/kafka/config/server.properties &

# Wait a bit for Kafka to fully start
sleep 5

# Setup Greenplum environment
echo "Setting up Greenplum environment..."
source /usr/local/greenplum-db-7.1.0/greenplum_path.sh

# Set the coordinator data directory for Greenplum
export COORDINATOR_DATA_DIRECTORY=/data/coordinator/gpseg-1

# Start Greenplum
echo "Starting Greenplum Database..."
gpstart -a

echo "All services started."
