import traci
import time
import traci.constants as tc
import pytz
import datetime
import json
import asyncio
import websockets

# Configuration for connecting to websockets server
ws_server = '34.32.165.88:8765'  


# Confiurations for SUMO
sumoCmd = ["sumo", "-c", "osm.sumocfg"]

def getdatetime():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    currentDT = utc_now.astimezone(pytz.timezone("Atlantic/Reykjavik"))
    DATIME = currentDT.strftime("%Y-%m-%d %H:%M:%S")
    return DATIME


async def wsSendData(websocket,message):
    # Serialize the list to a JSON string
    json_data = json.dumps(message)
    await websocket.send(json_data)

async def runScenario():
    uri = f"ws://{ws_server}"

    async with websockets.connect(uri) as websocket:
        traci.start(sumoCmd)
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
                veh_data = [vehid,getdatetime(),x_pos,y_pos,
                        gps_lon,gps_lat,spd,edge,lane, 
                        displacement,turnAngle,acc,
                        fuel_cons,co2_cons,dece]
                                    
                await wsSendData(websocket,veh_data)

        traci.close()


if __name__ == '__main__':
    asyncio.run(runScenario())







