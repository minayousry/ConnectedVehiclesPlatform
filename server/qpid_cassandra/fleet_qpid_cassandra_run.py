import multiprocessing
import uuid
from cassandra.cluster import Cluster
from proton.handlers import MessagingHandler
from proton.reactor import Container 
from proton import Event, Message
import time
from uuid import uuid4
from datetime import datetime
import pandas as pd
    
#Qpid configurations
server_url = '0.0.0.0:8888' 
topic_name = 'obd2_data_queue'


#Cassandra Configurations
keyspace_name = "obd2_database"
table_name = "obd2_data"
server_address = '127.0.0.1'


class Receiver(MessagingHandler):
    
    def __init__(self, url,queue):
        super(Receiver, self).__init__()
        self.url = url
        self.senders = {}
        self.queue = queue
        self.last_message_time = time.time() 
        self.container = None 
        
    def on_connection_closed(self, event):
        print("Connection is closed")
        #end the connection
        queue.put("STOP")
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
        self.last_message_time = time.time()
        
    def on_timer_task(self, event):
        print("onTimerTask func called")
        if not self.messages_received:
            print("No messages received yet.")

def receiverProcess(queue):
    handler = Receiver(server_url, queue)
    container = Container(handler) 
    container.run()
    
    
def databaseProcess(queue):
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect(keyspace_name)

        while True:
            message = queue.get()
            
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
            
    except Exception as e:
        print(f"Error during database operation: {e}")
    finally:
        cluster.shutdown()

        
def createExcelFile():
    cluster = Cluster(['localhost'])
    session = cluster.connect(keyspace_name)
    
    try:
        select_query = f"""SELECT vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
                            lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
                            co2_consumption, deceleration, storage_time 
                            FROM {keyspace_name}.{table_name}"""
                        

        # Execute query and fetch data
        rows = session.execute(select_query)
        
        df = pd.DataFrame(rows.current_rows)

        time_diff = df['storage_time'] - df['tx_time']
        
        # Convert time difference to seconds (assuming all values are valid)
        df['time_diff_seconds'] = time_diff.dt.total_seconds()
            
        # Generate Excel report
        df.to_excel("obd2_data_report.xlsx", index=False)
        #print("Excel file has been created.")
    
    except Exception as e:
        print(f"Failed to create excel file: {e}")
    
    finally:
        session.shutdown()
        cluster.shutdown()

if __name__ == "__main__":
    
    """ 
    queue = multiprocessing.Queue()

    # Start receiver process
    receiver = multiprocessing.Process(target=receiverProcess, args=(queue,))
    receiver.start()
    
    
    db = multiprocessing.Process(target=databaseProcess, args=(queue,))
    db.start()
    
    # Wait for the receiver and database process to finish
    receiver.join()
    
    # Signal to stop receiver and database process
    print("Sending stop signals...")
    
    #stop the database process once the receiver process is stopped
    queue.put("STOP")
    db.join()
    
    print("All processes have been stopped.")
    """
    createExcelFile()