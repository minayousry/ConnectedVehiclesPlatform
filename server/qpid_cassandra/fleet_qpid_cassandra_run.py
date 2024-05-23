import multiprocessing
from cassandra.cluster import Cluster
from proton.handlers import MessagingHandler, TransactionHandler
from proton.reactor import Container
from proton import Message 
from proton import Event
import time
from uuid import uuid4
from datetime import datetime
import pandas as pd
from cassandra.policies import DCAwareRoundRobinPolicy
import sys
import os
import logging
from cassandra.query import SimpleStatement

received_msg_count = 0
inserted_msg_count = 0


#Qpid configurations
server_url = '0.0.0.0:8888' 
topic_name = 'obd2_data_queue'


#Cassandra Configurations
keyspace_name = "obd2_database"
table_name = "obd2_data"
server_address = '127.0.0.1'

db_batch_size = 100

def getcurrentTimestamp():
    now = datetime.now()
    formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    return formatted_date_time         


class Receiver(MessagingHandler):
    def __init__(self, url, queue, no_of_received_msgs, no_of_sent_msgs):
        super(Receiver, self).__init__()
        self.url = url
        self.queue = queue
        self.no_of_received_msgs = no_of_received_msgs
        self.no_of_sent_msgs = no_of_sent_msgs
        self.received_msg_count = 0
        self.container = None

    def on_start(self, event):
        print(f"Connecting to {self.url}")
        self.container = event.container
        self.acceptor = event.container.listen(self.url)
        print(f"Listening on {self.url}")

    def on_connection_opened(self, event):
        print("Connection opened")

    def on_connection_closed(self, event):
        print("Connection closed")
        self.queue.put("STOP")
        with self.no_of_received_msgs.get_lock():
            self.no_of_received_msgs.value = self.received_msg_count
        if self.container:
            self.container.stop()

    def on_message(self, event):
        try:
            message = event.message.body
            #print(f"Received message: {message}")
            if message[0] != "STOP":
                current_timestamp = getcurrentTimestamp()
                message.append(current_timestamp)
                self.queue.put(message)
                event.receiver.flow(1)
                self.received_msg_count += 1
            else:
                print("Received all messages")
                with self.no_of_sent_msgs.get_lock():
                    self.no_of_sent_msgs.value = message[1]
                self.queue.put("STOP")
                self.container.stop()
        except Exception as e:
            print(f"Error processing message: {message if message else 'None'}: {e}")

    def on_disconnected(self, event):
        print("Disconnected")
        if self.container:
            self.container.stop()


def receiverProcess(data_queue, no_of_received_msgs_obj, no_of_sent_msgs_obj):

    server_url = "amqp://0.0.0.0:8888/obd2_data_queue"
    handler = Receiver(server_url, data_queue, no_of_received_msgs_obj, no_of_sent_msgs_obj)
    container = Container(handler)
    container.run()


def getCluster():
    cluster = Cluster(
            contact_points=['localhost'],
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
            protocol_version=5
        )
    return cluster
    

    
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
def databaseProcess(queue,no_of_inserted_msgs,use_database_timestamp):
    
    global inserted_msg_count
    
    # Parameterized query for security
    insert_query = f"INSERT INTO {keyspace_name}.{table_name} (id, vehicle_id,tx_time,x_pos,y_pos, \
                                                                gps_lon, gps_lat, speed,road_id,lane_id, \
                                                                displacement, turn_angle, acceleration, fuel_consumption,co2_consumption, \
                                                                deceleration, rx_time, storage_time \
                                                                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    
    try:
        cluster = cluster = getCluster()
        session = cluster.connect(keyspace_name)

        while True:
            message = queue.get()
            
            #convert class list to list
            if  message is None or message == "STOP":
                print("Stopping the database process...")
                break
            
            
            
            storage_time  = getcurrentTimestamp()                                                 
            
            session.execute(insert_query,( uuid4(), message[0],message[1],message[2],message[3] \
                                            ,message[4],message[5],message[6],message[7],message[8] \
                                            ,message[9],message[10],message[11],message[12],message[13] \
                                            ,message[14], message[15], storage_time
                                        ) )
            inserted_msg_count += 1
            
            
            
    except Exception as e:
        print(f"Error during database operation for {message}: {e}")
    finally:
        with no_of_inserted_msgs.get_lock():
                no_of_inserted_msgs.value = inserted_msg_count
        if 'session' in locals():
            print("closing session")
            session.shutdown()
        if 'cluster' in locals():
            print("closing cluster")
            cluster.shutdown()
        

def insertBatch(session, batch,insert_query):


    storage_time = getcurrentTimestamp()

    for message in batch:
        session.execute(insert_query, (uuid4(), message[0], message[1], message[2], message[3], \
                                        message[4], message[5], message[6], message[7], message[8], \
                                        message[9], message[10], message[11], message[12], message[13], \
                                        message[14],message[15], storage_time))      

def databaseBatchProcess(queue,no_of_inserted_msgs,use_database_timestamp):
    
    global inserted_msg_count
    
    insert_query = f"INSERT INTO {table_name} (id, vehicle_id, tx_time, x_pos, y_pos, \
                                               gps_lon, gps_lat, speed, road_id, lane_id, \
                                               displacement, turn_angle, acceleration, fuel_consumption, co2_consumption, \
                                               deceleration, rx_time, storage_time) \
                                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    try:
        cluster = getCluster()
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
                insertBatch(session, batch,insert_query)
                batch_count += 1
                inserted_msg_count += db_batch_size
                batch = []

        # Insert remaining records if any
        if len(batch) > 0:
            insertBatch(session,batch,insert_query)
            inserted_msg_count += len(batch)
            batch_count += 1

    except Exception as e:
        print(f"Error during database operation: {e}")
    finally:
        with no_of_inserted_msgs.get_lock():
                no_of_inserted_msgs.value = inserted_msg_count
        if 'session' in locals():
            print("closing session")
            session.shutdown()
        if 'cluster' in locals():
            print("closing cluster")
            cluster.shutdown()


def extractFromDatabase(x):
    result = None
    cluster = None
    session = None
    
    try:
        cluster = getCluster()
        session = cluster.connect(keyspace_name)
        
        select_query = f"""SELECT vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
                            lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
                            co2_consumption, deceleration, rx_time, storage_time 
                            FROM {keyspace_name}.{table_name}"""

        statement = SimpleStatement(select_query, fetch_size=1000)
        rows = session.execute(statement)
        all_rows = []

        for row in rows:
            all_rows.append(row)

        while rows.has_more_pages:
            rows = session.execute(statement, paging_state=rows.paging_state)
            for row in rows:
                all_rows.append(row)

        result = pd.DataFrame(all_rows)
        
        logging.info("Succeeded to extract info from database.")
        logging.info(f"Number of records extracted: {len(result)}")

    except Exception as e:
        logging.error(f"Failed to extract information from the database: {e}")
    
    finally:
        if session:
            session.shutdown()
        if cluster:
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
    
    