from cassandra.cluster import Cluster
from proton.handlers import MessagingHandler
from proton.reactor import Container
from datetime import datetime
from cassandra.policies import DCAwareRoundRobinPolicy

received_msg_count = 0



#Qpid configurations
server_url = '0.0.0.0:8888' 
topic_name = 'obd2_data_queue'



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
            current_timestamp = getcurrentTimestamp()
            #print(f"Received message: {message}")
            if message[0] != "STOP":
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