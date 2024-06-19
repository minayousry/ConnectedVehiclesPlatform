# Start the MQTT Influx service
echo "Starting MQTT Server"
sudo systemctl start mosquitto
sudo systemctl enable mosquitto

sleep 5

