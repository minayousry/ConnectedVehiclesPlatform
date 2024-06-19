#!/bin/bash

echo "Starting Qpid Server"
~/qpid-broker/9.2.0/bin/qpid-server &

# Wait a bit for Qpid to fully start
sleep 7
