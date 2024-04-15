
# This script is used to start the MQTT Influx service.
# It should be executed to initiate the communication between MQTT and InfluxDB.
# Make sure to have the necessary dependencies installed before running this script.
# Usage: ./start_mqtt_influx.sh
#
echo "Starting influxdb Server"
sudo systemctl start influxdb
sudo systemctl enable influxdb

sleep 5

# Start the MQTT Influx service
echo "Starting MQTT Server"
sudo systemctl start mosquitto
sudo systemctl enable mosquitto

sleep 5

