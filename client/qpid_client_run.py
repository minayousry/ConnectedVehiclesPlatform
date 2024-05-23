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

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

import multiprocessing

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
import multiprocessing
import threading
import traci

sent_msg_count = 0

class Sender(MessagingHandler):
    def __init__(self, url, queue_name, data_queue, stop_event):
        super(Sender, self).__init__()
        self.url = url
        self.queue_name = queue_name
        self.data_queue = data_queue
        self.stop_event = stop_event
        self.sent = 0

    def on_start(self, event):
        print(f"Connecting to {self.url}")
        self.container = event.container
        self.conn = event.container.connect(self.url)
        self.sender = event.container.create_sender(self.conn, self.queue_name)
        print(f"Sender created for queue: {self.queue_name}")

    def on_sendable(self, event):
        global sent_msg_count
        #print(f"Sender is sendable, credit: {event.sender.credit}")
        while event.sender.credit and (not self.data_queue.empty() or not self.stop_event.is_set()):
            if not self.data_queue.empty():
                
                data = self.data_queue.get()
                if data[0] != "STOP":
                    data[1] = cl_utl.getdatetime()
                else:
                    cl_utl.setMsgCount("qpid",sent_msg_count)
                    data.append(sent_msg_count) 
                
                msg = Message(body=data)
                event.sender.send(msg)
                
                sent_msg_count += 1
                
                
                    
            
        if self.stop_event.is_set() and self.data_queue.empty():
            print("All messages sent, closing sender and connection")
            self.sender.close()
            self.conn.close()

def senderThread(server_url, data_queue, stop_event):
    handler = Sender(server_url, "obd2_data_queue", data_queue, stop_event)
    container = Container(handler)
    container.run()

def run_scenario(sumo_cmd, data_queue, stop_event):
    traci.start(sumo_cmd)
    while traci.simulation.getMinExpectedNumber() > 0:
        traci.simulationStep()
        vehicles = traci.vehicle.getIDList()
        for veh_id in vehicles:
            x_pos, y_pos = traci.vehicle.getPosition(veh_id)
            gps_lon, gps_lat = traci.simulation.convertGeo(x_pos, y_pos)
            spd = round(traci.vehicle.getSpeed(veh_id) * 3.6, 2)  # Convert m/s to km/h
            edge = traci.vehicle.getRoadID(veh_id)
            lane = traci.vehicle.getLaneID(veh_id)
            displacement = round(traci.vehicle.getDistance(veh_id), 2)
            turn_angle = round(traci.vehicle.getAngle(veh_id), 2)
            acc = round(traci.vehicle.getAcceleration(veh_id), 2)
            fuel_cons = round(traci.vehicle.getFuelConsumption(veh_id), 2)
            co2_cons = round(traci.vehicle.getCO2Emission(veh_id), 2)
            dece = round(traci.vehicle.getDecel(veh_id), 2)
            veh_data = [veh_id,0, x_pos, y_pos, gps_lon, gps_lat, spd, edge, lane, displacement, turn_angle, acc, fuel_cons, co2_cons, dece]
            data_queue.put(veh_data)
            
            
    data_queue.put(["STOP"])    
    traci.close()
    stop_event.set()  # Signal the sender thread to stop

def runQpidClient(sumo_cmd, remote_machine_ip_addr):
    amqp_port_no = "8888"
    server_url = f"amqp://{remote_machine_ip_addr}:{amqp_port_no}"
    data_queue = multiprocessing.Queue()
    stop_event = threading.Event()
    sender_thread = threading.Thread(target=senderThread, args=(server_url, data_queue, stop_event))
    sender_thread.start()

    cl_utl.recordStartSimTime("qpid")
    run_scenario(sumo_cmd, data_queue, stop_event)
    cl_utl.recordEndSimTime("qpid")

    sender_thread.join(timeout=10)  # Wait for up to 10 seconds for the thread to finish
    if sender_thread.is_alive():
        print("Warning: Sender thread is still alive after timeout.")






