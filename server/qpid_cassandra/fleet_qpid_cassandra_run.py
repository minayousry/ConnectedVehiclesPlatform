import multiprocessing
from cassandra.cluster import Cluster
from proton.handlers import MessagingHandler
from proton.reactor import Container 
from proton import Event
import time
from uuid import uuid4
from datetime import datetime
import pandas as pd

import sys
import os




#Qpid configurations
server_url = '0.0.0.0:8888' 
topic_name = 'obd2_data_queue'


#Cassandra Configurations
keyspace_name = "obd2_database"
table_name = "obd2_data"
server_address = '127.0.0.1'

db_batch_size = 100



class Receiver(MessagingHandler):
    
    def __init__(self, url,queue,no_of_received_msgs):
        super(Receiver, self).__init__()
        self.url = url
        self.senders = {}
        self.queue = queue
        self.container = None 
        self.no_of_received_msgs = no_of_received_msgs
        
    def on_connection_closed(self, event):
        print("Connection is closed")
        #end the connection
        self.queue.put("STOP")
        if self.container:
            self.container.stop()
        

    def on_start(self, event):
        print("Listening on", self.url)
        self.container = event.container
        self.acceptor = event.container.listen(self.url)
    
    def on_link_closed(self, event: Event):
        print("link is closed")
        self.container.stop()

    def on_message(self, event):
        message = event.message
        self.queue.put(message.body)
        with self.no_of_received_msgs.get_lock():
            self.no_of_received_msgs.value += 1
        
    def on_timer_task(self, event):
        if not self.messages_received:
            print("No messages received yet.")

def receiverProcess(queue, no_of_received_msgs):
    handler = Receiver(server_url, queue,no_of_received_msgs)
    container = Container(handler) 
    container.run()
    
    
def databaseProcess(queue,no_of_inserted_msgs):
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect(keyspace_name)

        while True:
            message = queue.get()
            
            #convert class list to list
            if  message is None or message == "STOP":
                print("Stopping the database process...")
                break
            
            # Parameterized query for security
            insert_query = f"INSERT INTO {keyspace_name}.{table_name} (id, vehicle_id,tx_time,x_pos,y_pos, \
                                                                       gps_lon, gps_lat, speed,road_id,lane_id, \
                                                                       displacement, turn_angle, acceleration, fuel_consumption,co2_consumption, \
                                                                       deceleration,storage_time \
                                                                       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            
            timestamp  = datetime.now()                                                   
            
            session.execute(insert_query,( uuid4(), message[0],message[1],message[2],message[3] \
                                            ,message[4],message[5],message[6],message[7],message[8] \
                                            ,message[9],message[10],message[11],message[12],message[13] \
                                            ,message[14],timestamp
                                        ) )
            with no_of_inserted_msgs.get_lock():
                no_of_inserted_msgs.value += 1
            
    except Exception as e:
        print(f"Error during database operation for {message}: {e}")
    finally:
        cluster.shutdown()
        

def insertBatch(session, batch, table_name):
    insert_query = f"INSERT INTO {table_name} (id, vehicle_id, tx_time, x_pos, y_pos, \
                                               gps_lon, gps_lat, speed, road_id, lane_id, \
                                               displacement, turn_angle, acceleration, fuel_consumption, co2_consumption, \
                                               deceleration, storage_time) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    timestamp = datetime.now()

    for message in batch:
        session.execute(insert_query, (uuid4(), message[0], message[1], message[2], message[3], \
                                        message[4], message[5], message[6], message[7], message[8], \
                                        message[9], message[10], message[11], message[12], message[13], \
                                        message[14], timestamp))      

def databaseBatchProcess(queue, keyspace_name, table_name):
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect(keyspace_name)

        batch_count = 0
        batch = []

        while True:
            message = queue.get()

            if message is None or message == "STOP":
                print("Stopping the database process...")
                break

            # Append message to the batch
            batch.append(message)

            # Check if batch size is reached
            if len(batch) >= db_batch_size:
                insertBatch(session, batch, table_name)
                batch_count += 1
                batch = []

        # Insert remaining records if any
        if batch:
            insertBatch(session, batch, table_name)
            batch_count += 1

    except Exception as e:
        print(f"Error during database operation: {e}")
    finally:
        cluster.shutdown()

  
def extractFromDatabase():
    
    result = None
    cluster = None
    session = None
    
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect(keyspace_name)
        

        select_query = f"""SELECT vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
                            lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
                            co2_consumption, deceleration, storage_time 
                            FROM {keyspace_name}.{table_name}"""
                        

        # Execute query and fetch data
        rows = session.execute(select_query)

        result = pd.DataFrame(rows.current_rows)
        

        print("succeded to extract info from database.")
    
    except Exception as e:
        print(f"Failed to extract information from the database: {e}")
    
    finally:
        session.shutdown()
        cluster.shutdown()
    
    return result



if __name__ == "__main__":
    
    
    queue = multiprocessing.Queue()
    no_of_received_msgs_obj = multiprocessing.Value('i', 0)
    no_of_inserted_msgs_obj = multiprocessing.Value('i', 0)
    

    # Start receiver process
    receiver = multiprocessing.Process(target=receiverProcess, args=(queue,no_of_received_msgs_obj))
    receiver.start()
    
    
    db = multiprocessing.Process(target=databaseProcess, args=(queue,no_of_inserted_msgs_obj))
    db.start()
    
    # Wait for the receiver and database process to finish
    receiver.join()
    
    # Signal to stop receiver and database process
    print("Sending stop signals...")
    
    #stop the database process once the receiver process is stopped
    queue.put("STOP")
    db.join()
    
    
    
    print("All processes have been stopped.")
    print(no_of_received_msgs_obj.value)
    print(no_of_inserted_msgs_obj.value)
    

    extractFromDatabase()
    
    