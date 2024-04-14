import traci
import time
import pytz
import datetime
import json
import traci.constants as tc
import paho.mqtt.client as mqtt

# Configuration for connecting to MQTT broker
mqtt_broker = '34.90.73.165'
mqtt_port = 1883 
mqtt_topic = 'mqtt/topic'

# Confiurations for SUMO
sumoCmd = ["sumo", "-c", "osm.sumocfg"]

def getdatetime():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    currentDT = utc_now.astimezone(pytz.timezone("Atlantic/Reykjavik"))
    DATIME = currentDT.strftime("%Y-%m-%d %H:%M:%S")
    return DATIME

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))

def runScenario(client):
    traci.start(sumoCmd)
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
                    'vehid': vehid,
                    'datetime': getdatetime(),
                    'x_pos': x_pos,
                    'y_pos': y_pos,
                    'gps_lon': gps_lon,
                    'gps_lat': gps_lat,
                    'spd': spd,
                    'edge': edge,
                    'lane': lane,
                    'displacement': displacement,
                    'turnAngle': turnAngle,
                    'acc': acc,
                    'fuel_cons': fuel_cons,
                    'co2_cons': co2_cons,
                    'dece': dece
                }

                client.publish(mqtt_topic, json.dumps(veh_data))

    finally:
        traci.close()

if __name__ == '__main__':
    # Initialize an MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.connect(mqtt_broker, mqtt_port, 60)

    runScenario(client)

    client.disconnect()

    print(f"Messages sent to topic '{mqtt_topic}' on MQTT broker at {mqtt_broker}:{mqtt_port}")
