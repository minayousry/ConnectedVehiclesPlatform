#!/bin/bash

# Start cassandra Server
echo "Starting cassandra Server"
echo 'export PATH=$PATH:/opt/cassandra/bin' >> ~/.bashrc
source ~/.bashrc
cassandra -f &
