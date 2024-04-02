#!/bin/bash

# Start Redis Server
echo "Starting Redis Server"
sudo systemctl start redis
sudo systemctl enable redis

# Wait a bit for Redis to fully start
sleep 5

