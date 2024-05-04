import traci
import time
import traci.constants as tc
import pytz
import datetime
import json
import asyncio
import websockets
import client_utilities as cl_utl


ws_port_no = "8765"


async def wsSendData(websocket,message):
    # Serialize the list to a JSON string
    json_data = json.dumps(message)
    await websocket.send(json_data)

async def runScenario(sumo_cmd,ws_server):
    uri = f"ws://{ws_server}"
    
    async with websockets.connect(uri) as websocket:
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
                                    
                await wsSendData(websocket,veh_data)
                cl_utl.increaseMsgCount("ws")
        traci.close()
    

def runWsClient(sumo_cmd,remote_machine_ip_addr):
    ws_server = remote_machine_ip_addr+":"+ws_port_no  
    cl_utl.recordStartSimTime("ws")
    asyncio.run(runScenario(sumo_cmd,ws_server))
    cl_utl.recordEndSimTime("ws")









