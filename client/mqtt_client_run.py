import traci
import time
import pytz
import datetime
import json
import traci.constants as tc
import paho.mqtt.client as mqtt

import client_utilities as cl_utl

# Configuration for connecting to MQTT broker
mqtt_port = 1883 
mqtt_topic = 'mqtt/topic'


def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))

def runScenario(sumo_cmd,client):
    traci.start(sumo_cmd)
    try:
        while traci.simulation.getMinExpectedNumber() > 0:
            traci.simulationStep()
            vehicles = traci.vehicle.getIDList()

            for i in range(len(vehicles)):
                vehid = vehicles[i]
                x_pos, y_pos = traci.vehicle.getPosition(vehid)
                gps_lon, gps_lat = traci.simulation.convertGeo(x_pos, y_pos)
                spd = round(traci.vehicle.getSpeed(vehid) * 3.6, 2)
                edge = traci.vehicle.getRoadID(vehid)
                lane = traci.vehicle.getLaneID(vehid)
                displacement = round(traci.vehicle.getDistance(vehid), 2)
                turnAngle = round(traci.vehicle.getAngle(vehid), 2)
                acc = round(traci.vehicle.getAcceleration(vehid), 2)
                fuel_cons = round(traci.vehicle.getFuelConsumption(vehid), 2)
                co2_cons = round(traci.vehicle.getCO2Emission(vehid), 2)
                dece = round(traci.vehicle.getDecel(vehid), 2)

                veh_data = [
                    vehid,          # Vehicle ID as string
                    str(cl_utl.getdatetime()),       # Datetime string
                    float(x_pos),        # X position as float
                    float(y_pos),        # Y position as float
                    float(gps_lon),      # GPS longitude as float
                    float(gps_lat),      # GPS latitude as float
                    float(spd),          # Speed as float
                    edge,                # Road ID as string
                    lane,                # Lane ID as string
                    float(displacement), # Displacement as float
                    float(turnAngle),    # Turn angle as float
                    float(acc),          # Acceleration as float
                    float(fuel_cons),    # Fuel consumption as float
                    float(co2_cons),     # CO2 consumption as float
                    float(dece)          # Deceleration as float
                ]

                client.publish(mqtt_topic, json.dumps(veh_data))
                cl_utl.increaseMsgCount("mqtt")
        
        client.publish(mqtt_topic, json.dumps(["STOP"]))
    finally:
        traci.close()

def runMqttClient(sumo_cmd,remote_machine_ip_addr):
    
    mqtt_broker = remote_machine_ip_addr
    
    # Initialize an MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.connect(mqtt_broker, mqtt_port, 60)

    cl_utl.recordStartSimTime("mqtt")
    runScenario(sumo_cmd,client)
    cl_utl.recordEndSimTime("mqtt")
    
    client.disconnect()

    print(f"Messages sent to topic '{mqtt_topic}' on MQTT broker at {mqtt_broker}:{mqtt_port}")
