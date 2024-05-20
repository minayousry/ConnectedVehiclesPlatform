import time
from influxdb import InfluxDBClient
import multiprocessing
import paho.mqtt.client as mqtt
from datetime import datetime
import pandas as pd
import sys
import os

database_name = "obd2_database"
db_batch_size = 100

mqtt_broker_address = "localhost"
port_no = 1883
mqtt_comm_timeout = 5
socket_closed = False

sent_msg_count = 0
received_msg_count = 0
inserted_msg_count = 0

start_time = time.time()

is_msg_received = False



def getcurrentTimestamp():
    now = datetime.now()
    formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    return formatted_date_time  

# MQTT callback functions
def on_connect(client, userdata, flags, rc):
    print("Listening for MQTT messages...")
    client.subscribe("mqtt/topic")


def on_message(client, userdata, msg,queue):
    global start_time
    global is_msg_received
    global received_msg_count
    global sent_msg_count
    
    # Get the current time in seconds
    start_time = time.time()
    
    data_list = msg.payload.decode().split(',')


    if data_list[0] != '["STOP"]':
        queue.put(data_list)  # Put the data into the queue
        received_msg_count += 1  
    else:
        sent_msg_count = 0   
        is_msg_received = True
    
    
    
    
    

def on_socket_close(client, userdata, msg):
    print(f"Socket closed")
    
def on_disconnect(client, userdata, rc):
    print(f"Disconnected with result code {rc}")

# MQTT process
def mqttProcess(queue,no_of_received_msgs_obj):
    
    global start_time
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
            mqtt_client.loop_stop()
            print("MQTT communication timeout")
            queue.put("STOP")
            break

# InfluxDB process
def influxBatchProcess(queue,no_of_inserted_msgs_obj,use_database_timestamp):
    
    global inserted_msg_count
    
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
            measurement = getMeasurement(inserted_msg_count,data_list,use_database_timestamp)
            measurement_body.append(measurement)
            inserted_msg_count += 1
            
            if inserted_msg_count % db_batch_size == 0:
                try:
                    influx_client.write_points(measurement_body)
                    with no_of_inserted_msgs_obj.get_lock():
                        no_of_inserted_msgs_obj.value += db_batch_size
                except Exception as e:
                    print(f"Error in inserting batch {measurement_body}: {e}")
                finally:
                    measurement_body = []
        else:
            break
    influx_client.write_points(measurement_body)
    with no_of_inserted_msgs_obj.get_lock():
        no_of_inserted_msgs_obj.value += len(measurement_body)
        
    influx_client.close()
    
    
def getMeasurement(msg_id,data_list,use_database_timestamp):
    
    measurement = {}
    
    if use_database_timestamp:
        measurement = {
            "measurement": str(msg_id),
            "fields": {
            "vehicle_id": data_list[0].replace('[',''),
            "tx_time": data_list[1],
            "x_pos": float(data_list[2]),
            "y_pos": float(data_list[3]),
            "gps_lon": float(data_list[4]),
            "gps_lat": float(data_list[5]),
            "speed": float(data_list[6]),
            "road_id": data_list[7],
            "lane_id": data_list[8],
            "displacement": float(data_list[9]),
            "turn_angle": float(data_list[10]),
            "acceleration": float(data_list[11]),
            "fuel_consumption": float(data_list[12]),
            "co2_consumption": float(data_list[13]),
            "deceleration": float(data_list[14].replace(']',''))
            }
        }
    else:
        current_timestamp = getcurrentTimestamp()
        measurement = {
            "measurement": str(msg_id),
            "fields": {
            "vehicle_id": data_list[0].replace('[',''),
            "tx_time": data_list[1],
            "x_pos": float(data_list[2]),
            "y_pos": float(data_list[3]),
            "gps_lon": float(data_list[4]),
            "gps_lat": float(data_list[5]),
            "speed": float(data_list[6]),
            "road_id": data_list[7],
            "lane_id": data_list[8],
            "displacement": float(data_list[9]),
            "turn_angle": float(data_list[10]),
            "acceleration": float(data_list[11]),
            "fuel_consumption": float(data_list[12]),
            "co2_consumption": float(data_list[13]),
            "deceleration": float(data_list[14].replace(']','')),
            "storage_time": current_timestamp
            }
        }
    
    return measurement    

def get_line_protocol(msg_id, data_list):
    fields = ",".join([f"{key}={value}" for key, value in zip(
        ["vehicle_id", "tx_time", "x_pos", "y_pos", "gps_lon", "gps_lat", 
         "speed", "road_id", "lane_id", "displacement", "turn_angle", 
         "acceleration", "fuel_consumption", "co2_consumption", "deceleration"],
        [data_list[0].replace('[',''), data_list[1], float(data_list[2]),
         float(data_list[3]), float(data_list[4]), float(data_list[5]), 
         float(data_list[6]), data_list[7], data_list[8], float(data_list[9]), 
         float(data_list[10]), float(data_list[11]), float(data_list[12]), 
         float(data_list[13]), float(data_list[14].replace(']',''))])
    ])
    return f"{msg_id} {fields}"
    
def influxProcess(queue,no_of_inserted_msgs_obj,use_database_timestamp):
    
    global inserted_msg_count
    
    
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
            measurement = getMeasurement(inserted_msg_count,data_list,use_database_timestamp)
            try:
                influx_client.write_points([measurement])
                inserted_msg_count += 1
            except Exception as e:
                print(f"Error in inserting {measurement}: {e}")
        else:
            # End the process
            print("end the Influx process")
            break
    
    
    with no_of_inserted_msgs_obj.get_lock():
        no_of_inserted_msgs_obj.value = inserted_msg_count
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
    
    
    df['tx_time'] = df['tx_time'].str.replace('\"','').str.strip()
    
    if use_database_timestamp:
        print("using database timestamp")
        # Convert 'time' column to datetime format with timezone specifier 'Z'
        df['storage_time'] = pd.to_datetime(df['time'], format="%Y-%m-%dT%H:%M:%S.%fZ")
        df['storage_time'] = pd.to_datetime(df['storage_time'], format='%Y-%m-%d %H:%M:%S.%f')
        

    influx_client.close()
    
    return df

        
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
    
    
