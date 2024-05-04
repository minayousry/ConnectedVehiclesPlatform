import traci
import time
import pytz
import datetime
import json
import asyncio
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
import threading
import queue
import time
import client_utilities as cl_utl

address = 'obd2_data_queue' 
amqp_port_no = "8888"

class Sender(MessagingHandler):
    def __init__(self, server_url, address,data_queue,stop_event):
        super(Sender, self).__init__()
        self.server_url = server_url
        self.address = address
        self.data_queue = data_queue
        self.sender = None
        self.stop_event = stop_event
        self.conn = None

    def on_start(self, event):
        print("on Start")
        self.conn = event.container.connect(self.server_url)
        self.sender = event.container.create_sender(self.conn, self.address)
        threading.Thread(target=self.send_data, daemon=True).start()
        
    def send_data(self):
        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                data = self.data_queue.get(True,timeout=1)  # Timeout to periodically check stop_event
                message = Message(body=data)
                self.sender.send(message)
            except queue.Empty:
                continue  # Continue checking if the stop_event is set
            except Exception as e:
                print(f"Error sending data: {e}")
        if self.conn:
            self.conn.close()

def start_sender(server_url,data_queue, stop_event):
    sender = Sender(server_url, address, data_queue,stop_event)
    Container(sender).run()

def run_scenario(sumo_cmd,data_queue,stop_event):

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
             
            data_queue.put(veh_data)
        
        # Sleep for 0.2 seconds
        #time.sleep(0.2)    

    traci.close()
    stop_event.set()  # Signal the sender thread to stop

def runQpidClient(sumo_cmd,remote_machine_ip_addr):
    
    server_url = "amqp://"+remote_machine_ip_addr+":"+amqp_port_no

    data_queue = queue.Queue()
    stop_event = threading.Event()

    # Start sender thread
    sender_thread = threading.Thread(target=start_sender, args=(server_url,data_queue,stop_event))
    sender_thread.start()
    
    # Run the simulation scenario, which feeds data into the queue
    run_scenario(sumo_cmd,data_queue,stop_event)
    
    sender_thread.join(timeout=10)  # Wait for up to 10 seconds for the thread to finish

    if sender_thread.is_alive():
        print("Warning: Sender thread is still alive after timeout.")
    
    
    







