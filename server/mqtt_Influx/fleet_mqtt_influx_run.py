import time
from influxdb import InfluxDBClient
import multiprocessing
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
import pandas as pd
import sys
import os
import ujson as json

database_name = "obd2_database"
db_batch_size = 100

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

# InfluxDB process
def influxBatchProcess(queue,last_storage_timestamp_obj,use_database_timestamp):
    
    last_storage_timestamp = "NONE"
    inserted_msg_count = 0
    
    # Set up InfluxDB client
    influx_client = InfluxDBClient(
                    host='localhost',          # InfluxDB server host
                    port=8086,                 # InfluxDB server port
                    timeout=5,                 # Timeout for HTTP requests (in seconds)
                    verify_ssl=False,          # Enable SSL certificate verification
                    gzip=True,retries=3,pool_size=100
                    )
    influx_client.switch_database(database_name)
    
    measurement_body = []
    while True:

        data_list = queue.get()  # Get the data from the queue

        if data_list is not None and data_list != "STOP":   
            measurement = getMeasurement(inserted_msg_count,data_list,use_database_timestamp,last_storage_timestamp)
            measurement_body.append(measurement)
            inserted_msg_count += 1
            
            if inserted_msg_count % db_batch_size == 0:
                try:
                    influx_client.write_points(measurement_body)
                    last_storage_timestamp = getcurrentTimestamp()

                except Exception as e:
                    print(f"Error in inserting batch {measurement_body}: {e}")
                finally:
                    measurement_body = []
        else:
            break
    
    if len(measurement_body) > 0:
        influx_client.write_points(measurement_body)
        last_storage_timestamp = getcurrentTimestamp()
        
    with last_storage_timestamp_obj.get_lock():
            last_storage_timestamp_obj.value = stringToFloatTimestamp(last_storage_timestamp)  

        
    influx_client.close()
    
    
def getMeasurement(msg_id,data_list,use_database_timestamp,storage_time):
    
    measurement = {}
    
    if use_database_timestamp:
        measurement = {
            "measurement": str(msg_id),
            "fields": {
            "vehicle_id": data_list[0],
            "tx_time": data_list[1],
            "x_pos": data_list[2],
            "y_pos": data_list[3],
            "gps_lon": data_list[4],
            "gps_lat": data_list[5],
            "speed": data_list[6],
            "road_id": data_list[7],
            "lane_id": data_list[8],
            "displacement": data_list[9],
            "turn_angle": data_list[10],
            "acceleration": data_list[11],
            "fuel_consumption": data_list[12],
            "co2_consumption": data_list[13],
            "deceleration": data_list[14],
            "rx_time": data_list[15]
            }
        }
    else:
        measurement = {
            "measurement": str(msg_id),
            "fields": {
            "vehicle_id": data_list[0],
            "tx_time": data_list[1],
            "x_pos": data_list[2],
            "y_pos": data_list[3],
            "gps_lon": data_list[4],
            "gps_lat": data_list[5],
            "speed": data_list[6],
            "road_id": data_list[7],
            "lane_id": data_list[8],
            "displacement": data_list[9],
            "turn_angle": data_list[10],
            "acceleration": data_list[11],
            "fuel_consumption": data_list[12],
            "co2_consumption": data_list[13],
            "deceleration": data_list[14],
            "rx_time": data_list[15],
            "storage_time": storage_time
            }
        }
    
    return measurement    

def influxProcess(queue,last_storage_timestamp_obj,use_database_timestamp):
    

    last_storage_timestamp = "NONE"
    inserted_msg_count = 0
    

    # Set up InfluxDB client
    influx_client = InfluxDBClient(
                    host='localhost',          # InfluxDB server host
                    port=8086,                 # InfluxDB server port
                    timeout=5,                 # Timeout for HTTP requests (in seconds)
                    verify_ssl=False,           # Enable SSL certificate verification
                    gzip=True,retries=3,pool_size=100
                    )
    
    influx_client.switch_database(database_name)
    
    measurement_body = []
    while True:
        
        data_list = queue.get()  # Get the data from the queue
        if data_list is not None and data_list != "STOP":
            measurement = getMeasurement(inserted_msg_count,data_list,use_database_timestamp,last_storage_timestamp)
            last_storage_timestamp = getcurrentTimestamp()
            try:
                influx_client.write_points([measurement])
                inserted_msg_count += 1
            except Exception as e:
                print(f"Error in inserting {measurement}: {e}")
        else:
            # End the process
            print("end the Influx process")
            break
    
    
    with last_storage_timestamp_obj.get_lock():
        last_storage_timestamp_obj.value = stringToFloatTimestamp(last_storage_timestamp) 
    influx_client.close()    
    
        
def extractFromDatabase(use_database_timestamp):
    
    global is_database_timestamp_used
    
    influx_client = InfluxDBClient(host='localhost', port=8086)
    influx_client.switch_database(database_name)
    
    all_data_frames = [] 
    
   
    # Fetch all measurements
    measurements = influx_client.query('SHOW MEASUREMENTS').get_points()
    measurement_names = [measurement['name'] for measurement in measurements]

       
    dict_list = []
    
    for name in measurement_names:
        result = influx_client.query(f"SELECT * FROM \"{str(name)}\"")
        points = list(result.get_points())
        
        for i in range(len(points)):
            dict_list.append(points[i])
    
   
    df = pd.DataFrame(dict_list)
    

    if use_database_timestamp:
        print("using database timestamp")
        # Convert 'time' column to datetime format with timezone specifier 'Z'
        df['storage_time'] = pd.to_datetime(df['time'], format="%Y-%m-%dT%H:%M:%S.%fZ")
        df['storage_time'] = pd.to_datetime(df['storage_time'], format='%Y-%m-%d %H:%M:%S.%f')
        

    influx_client.close()
    
    return df,db_batch_size

        
if __name__ == '__main__':
    
    
    
    # Create a multiprocessing Queue for IPC
    data_queue = multiprocessing.Queue(maxsize=9900000)
    
    no_of_received_msgs_obj = multiprocessing.Value('i', 0)
    no_of_inserted_msgs_obj = multiprocessing.Value('i', 0)

    use_database_timestamp = True
    
    # Create and start the MQTT process
    mqtt_proc = multiprocessing.Process(target=mqttProcess,args=(data_queue,no_of_received_msgs_obj))
    mqtt_proc.start()

    # Create and start the InfluxDB process
    influx_proc = multiprocessing.Process(target=influxBatchProcess,args=(data_queue,no_of_inserted_msgs_obj,use_database_timestamp))
    influx_proc.start()


    # Wait for both processes to finish
    mqtt_proc.join()
    
    # Signal the InfluxDB process to stop
    data_queue.put("STOP")
    
    influx_proc.join()
    
    print(f"Number of received messages: {no_of_received_msgs_obj.value}")
    print(f"Number of inserted messages: {no_of_inserted_msgs_obj.value}")
    
    extractFromDatabase()

    print("End of program")
    
    
