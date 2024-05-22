import traci
import time
import pytz
import datetime
import ujson as json
import traci.constants as tc
import paho.mqtt.client as mqtt

import client_utilities as cl_utl

# Configuration for connecting to MQTT broker
mqtt_port = 1883 
mqtt_topic = 'mqtt/topic'


def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))

def runScenario(sumo_cmd,client):
    
    sent_msg_count = 0
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

                veh_data = {
                    0:vehid,          # Vehicle ID as string
                    1:cl_utl.getdatetime(),       # Datetime string
                    2:x_pos,        # X position as float
                    3:y_pos,        # Y position as float
                    4:gps_lon,      # GPS longitude as float
                    5:gps_lat,      # GPS latitude as float
                    6:spd,          # Speed as float
                    7:edge,                # Road ID as string
                    8:lane,                # Lane ID as string
                    9:displacement, # Displacement as float
                    10:turnAngle,    # Turn angle as float
                    11:acc,          # Acceleration as float
                    12:fuel_cons,    # Fuel consumption as float
                    13:co2_cons,     # CO2 consumption as float
                    14:dece          # Deceleration as float
                }

                client.publish(mqtt_topic, json.dumps(veh_data))
                sent_msg_count += 1
                cl_utl.increaseMsgCount("mqtt")
        client.publish(mqtt_topic, json.dumps({0:"STOP",1:sent_msg_count}))
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
