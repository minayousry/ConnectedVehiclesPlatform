import paho.mqtt.client as mqtt
from datetime import datetime, timezone
import ujson as json

mqtt_broker_address = "localhost"
port_no = 1883
mqtt_comm_timeout = 5
socket_closed = False

sent_msg_count = 0
received_msg_count = 0


is_msg_received = False

def stringToFloatTimestamp(timestamp_str, format='%Y-%m-%d %H:%M:%S.%f'):
    dt = datetime.strptime(timestamp_str, format)
    float_timestamp = dt.replace(tzinfo=timezone.utc).timestamp()
    return float_timestamp


def getcurrentTimestamp():
    now = datetime.now()
    formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    return formatted_date_time  

# MQTT callback functions
def on_connect(client, userdata, flags, rc):
    print("Listening for MQTT messages...")
    client.subscribe("mqtt/topic")


def on_message(client, userdata, msg,queue):

    global is_msg_received
    global received_msg_count
    global sent_msg_count
    
    
    data_dict = json.loads(msg.payload.decode('utf-8'))
    current_timestamp = getcurrentTimestamp()
    data_list = list(data_dict.values())

    if data_list[0] != "STOP":
        data_list.append(current_timestamp)
        queue.put(data_list)  # Put the data into the queue
        
        received_msg_count += 1  
    else:
        print("Finished receiving messages")
        print("Queue size is ",queue.qsize())
        sent_msg_count = data_list[1]
        queue.put("STOP")    
        is_msg_received = True

def on_socket_close(client, userdata, msg):
    print(f"Socket closed")
    
def on_disconnect(client, userdata, rc):
    print(f"Disconnected with result code {rc}")

# MQTT process
def mqttProcess(queue,no_of_received_msgs_obj,no_of_sent_msgs_obj):
    
    global is_msg_received
    
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = lambda client, userdata, msg: on_message(client, userdata, msg, queue)
    mqtt_client.on_socket_close = on_socket_close
    mqtt_client.on_disconnect = on_disconnect
    
    mqtt_client.connect(mqtt_broker_address, port_no , 60)
    
    mqtt_client.loop_start()
    

    while True:
        if (is_msg_received and (queue.empty())):
            global received_msg_count
            with no_of_received_msgs_obj.get_lock():
                no_of_received_msgs_obj.value = received_msg_count
            with no_of_sent_msgs_obj.get_lock():
                no_of_sent_msgs_obj.value = sent_msg_count
            mqtt_client.loop_stop()
            print("MQTT communication timeout")
            queue.put("STOP")
            break