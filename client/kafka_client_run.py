import traci
import time
import traci.constants as tc
from kafka import KafkaProducer
import json

import client_utilities as cl_utl

# Configuration for connecting to Kafka server
topic_name = 'OBD2_data'


def runScenario(sumo_cmd,producer):
    
    sent_msg_count = 0
    try:    
        traci.start(sumo_cmd)
        while traci.simulation.getMinExpectedNumber() > 0:

            traci.simulationStep()
            vehicles = traci.vehicle.getIDList()

            for i in range(0,len(vehicles)):
                #Function descriptions
                #https://sumo.dlr.de/docs/TraCI/Vehicle_Value_Retrieval.html
                #https://sumo.dlr.de/pydoc/traci._vehicle.html
                vehid = vehicles[i]
                x_pos, y_pos = traci.vehicle.getPosition(vehicles[i])
                gps_lon, gps_lat = traci.simulation.convertGeo(x_pos, y_pos)
                spd = round(traci.vehicle.getSpeed(vehicles[i])*3.6,2) #Convert m/s to km/h
                edge = traci.vehicle.getRoadID(vehicles[i])
                lane = traci.vehicle.getLaneID(vehicles[i])
                displacement = round(traci.vehicle.getDistance(vehicles[i]),2) #distance to starting point
                turnAngle = round(traci.vehicle.getAngle(vehicles[i]),2) #degree within last step
                acc = round(traci.vehicle.getAcceleration(vehicles[i]),2)
                fuel_cons = round(traci.vehicle.getFuelConsumption(vehicles[i]),2)
                co2_cons = round(traci.vehicle.getCO2Emission(vehicles[i]),2)
                dece = round(traci.vehicle.getDecel(vehicles[i]),2)

                #Packing the vehicle data
                veh_data = [vehid,cl_utl.getdatetime(),x_pos,y_pos,
                            gps_lon,gps_lat,spd,edge,lane, 
                            displacement,turnAngle,acc,
                            fuel_cons,co2_cons,dece]
                     
                producer.send(topic_name, value=veh_data)
                sent_msg_count += 1
                cl_utl.increaseMsgCount("kafka")            
        producer.send(topic_name, value=["STOP",sent_msg_count])
        traci.close()
        
    except Exception as e:
        print(f"Error: {e}")


def runKafkaClient(sumo_cmd,remote_machine_ip_addr):
    
    kafka_server = remote_machine_ip_addr + ':9092'  # this to the Kafka server address
    
    # Initialize a Kafka producer
    producer = KafkaProducer(bootstrap_servers=[kafka_server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    cl_utl.recordStartSimTime("kafka")
    runScenario(sumo_cmd,producer)
    cl_utl.recordEndSimTime("kafka")
        
    # Ensure all messages are sent and then close the producer
    producer.flush()
    producer.close()

    print(f"Messages sent to topic '{topic_name}' on Kafka server at {kafka_server}")

    








