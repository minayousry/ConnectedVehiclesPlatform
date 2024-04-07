#restart the Influx service
echo "Restarting InfluxDB"
sudo systemctl restart influxdb


sleep 5

#restart the MQTT service
echo "Restarting MQTT Server"
sudo systemctl restart mosquitto
