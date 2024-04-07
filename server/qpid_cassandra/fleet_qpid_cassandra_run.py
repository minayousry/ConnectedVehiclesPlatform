import multiprocessing
from cassandra.cluster import Cluster
from proton.handlers import MessagingHandler
from proton.reactor import Container 
from proton import ContainerSASL
from proton import Message
import time

#Qpid configurations
server_url = 'amqp://guest:guest@localhost' 
topic_name = 'obd2_data_queue'


#Cassandra Configurations
keyspace_name = "obd2_database"
table_name = "obd2_data"
server_address = '127.0.0.1'


class Receiver(MessagingHandler):
    def __init__(self, server_url, queue,queue_name):
        super(Receiver, self).__init__()
        self.server_url = server_url
        self.queue_name = queue_name
        self.queue = queue
        print("init func called")

    def on_start(self, event):
        print("Attempting to connect...")
        sasl = event.container.sasl()
        sasl.allowed_mechs('PLAIN')  # Specify the allowed SASL mechanism
        event.container.connect(url=self.server_url)
        
        
    def on_message(self, event):
        print("onMessage func called")
        
        message = event.message
        
        if message.body == "STOP":
            event.receiver.close()
            event.connection.close()
            return
        
        self.queue.put(message.body)
    
    def on_transport_error(self, event):
        print("Transport error:", event.transport.condition)

    def on_connection_opened(self, event):
        print("Connection established.")
        event.container.create_receiver(event.connection, self.queue_name)

    def on_connection_error(self, event):
        print("Connection failed.")

    def on_disconnected(self, event):
        print("Disconnected.")


def receiverProcess(queue):
    handler = Receiver(server_url, queue ,topic_name) 
    Container(handler).run()

def databaseProcess(queue):
    cluster = Cluster(['localhost'])
    session = cluster.connect(keyspace_name)
    
    while True:
        message = queue.get() 
        if message == "STOP":
            break
        print(f"Inserting into DB: {message}")
        
        session.execute(
            f"INSERT INTO {keyspace_name}.{table_name} (id, message) VALUES (uuid(), %s)",
            (message,)
        )

    cluster.shutdown()

if __name__ == "__main__":
    queue = multiprocessing.Queue()

    # Start receiver process
    receiver = multiprocessing.Process(target=receiverProcess, args=(queue,))
    receiver.start()
    
    
    db = multiprocessing.Process(target=databaseProcess, args=(queue,))
    db.start()
    
    # Example run duration: 50 seconds
    print("Running for 50 seconds...")
    time.sleep(50)

    # Signal to stop receiver and database process
    print("Sending stop signals...")
    queue.put("STOP")

    # Wait for the receiver and database process to finish
    receiver.join()
    db.join()

    print("All processes have been stopped.")