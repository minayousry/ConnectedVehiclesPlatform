#!/bin/bash

echo "Starting Qpid Server"
~/qpid-broker/9.2.0/bin/qpid-server &

# Wait a bit for Qpid to fully start
sleep 7

# Start cassandra Server
echo "Starting cassandra Server"
echo 'export PATH=$PATH:/opt/cassandra/bin' >> ~/.bashrc
source ~/.bashrc
cassandra -f &

# Wait a bit for Redis to fully start
sleep 7

echo "Finished running cassandra Server"

echo "All services started."