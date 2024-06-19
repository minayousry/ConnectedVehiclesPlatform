# Setup Greenplum environment
echo "Setting up Greenplum environment..."
source /usr/local/greenplum-db-7.1.0/greenplum_path.sh

# Set the coordinator data directory for Greenplum
export COORDINATOR_DATA_DIRECTORY=/data/coordinator/gpseg-1

# Start Greenplum
echo "Starting Greenplum Database..."
gpstart -a &

# Wait a bit for Kafka to fully start
sleep 5

echo "All services started."
