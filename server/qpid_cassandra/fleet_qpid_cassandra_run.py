import multiprocessing
import uuid
from cassandra.cluster import Cluster
from proton.handlers import MessagingHandler
from proton.reactor import Container 
from proton import Message
import time
from proton import SASL, Transport

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
        

    def on_start(self, event):
        print("Listening on", self.url)
        self.container = event.container
        self.acceptor = event.container.listen(self.url)

    def on_link_opening(self, event):
        if event.link.is_sender:
            if event.link.remote_source and event.link.remote_source.dynamic:
                event.link.source.address = str(uuid.uuid4())
                self.senders[event.link.source.address] = event.link
            elif event.link.remote_target and event.link.remote_target.address:
                event.link.target.address = event.link.remote_target.address
                self.senders[event.link.remote_target.address] = event.link
            elif event.link.remote_source:
                event.link.source.address = event.link.remote_source.address
        elif event.link.remote_target:
            event.link.target.address = event.link.remote_target.address

    def on_message(self, event):
        print("onMessage func called")
        
        message = event.message
        
        if message.body == "STOP":
            event.receiver.close()
            event.connection.close()
            return
        
        self.queue.put(message.body)

def receiverProcess(queue):
    handler = Receiver(server_url, queue) 
    Container(handler).run()

def databaseProcess(queue):
    
    cluster = Cluster(['localhost'])
    session = cluster.connect(keyspace_name)
    
    while True:
        message = queue.get() 
        if message == "STOP":
            break
        print(f"Inserting into DB: {message}")
    
        
        insert_query = f"""
        INSERT INTO {keyspace_name}.{table_name} ( \
            vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, \
            speed, road_id, lane_id, displacement, turn_angle,  \
            acceleration, fuel_consumption, co2_consumption, deceleration, storage_time \
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, toTimeStamp(now()))
        """

        # Assuming message is a tuple containing values for each column in the table
        session.execute(insert_query, message)

    cluster.shutdown()

if __name__ == "__main__":
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
    queue.put("STOP")

    db.join()

    print("All processes have been stopped.")