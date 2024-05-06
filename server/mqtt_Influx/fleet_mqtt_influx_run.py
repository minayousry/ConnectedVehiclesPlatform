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
mqtt_comm_timeout = 20
socket_closed = False

received_msg_count = 0
inserted_msg_count = 0


# MQTT callback functions
def on_connect(client, userdata, flags, rc):
    print("Listening for MQTT messages...")
    client.subscribe("mqtt/topic")


def on_message(client, userdata, msg,queue,no_of_received_msgs_obj):
    data_list = msg.payload.decode().split(',')
    queue.put(data_list)  # Put the data into the queue
    global received_msg_count
    received_msg_count += 1
    
def on_socket_close(client, userdata, msg):
    print(f"Socket closed")
    
def on_disconnect(client, userdata, rc):
    print(f"Disconnected with result code {rc}")
    global socket_closed
    socket_closed = True

# MQTT process
def mqttProcess(queue,no_of_received_msgs_obj):
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = lambda client, userdata, msg: on_message(client, userdata, msg, queue, no_of_received_msgs_obj)
    mqtt_client.on_socket_close = on_socket_close
    mqtt_client.on_disconnect = on_disconnect
    
    mqtt_client.connect(mqtt_broker_address, port_no , 60)
    
    mqtt_client.loop_start()
    
    
        
 
    # Get the current time in seconds
    start_time = time.time()
    

    while True:
            
        current_time = time.time()
        time_diff = current_time - start_time
        #print(time_diff)
        
        if (queue.empty() and ((time_diff > mqtt_comm_timeout) or socket_closed)):
            mqtt_client.loop_stop()
            print("MQTT communication timeout")
            queue.put("STOP")
            global received_msg_count
            with no_of_received_msgs_obj.get_lock():
                no_of_received_msgs_obj.value = received_msg_count
            mqtt_client.loop_stop()
            break
        elif not queue.empty():
            # Reset the start time
            start_time = time.time()
    
def getMeasurement(msg_id,data_list):
    
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
            #"storage_time": str(formatted_time)
        }
    }
    
    return measurement    

# InfluxDB process
def influxBatchProcess(queue,no_of_inserted_msgs_obj):
    
    global inserted_msg_count
    
    # Set up InfluxDB client
    influx_client = InfluxDBClient(host='localhost', port=8086)
    influx_client.switch_database(database_name)
    
    measurement_body = []
    while True:
        data_list = queue.get()  # Get the data from the queue
        if data_list is not None and data_list != "STOP":   
            measurement = getMeasurement(inserted_msg_count,data_list)
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
    
    
def influxProcess(queue,no_of_inserted_msgs_obj):
    
    global inserted_msg_count
    
    # Set up InfluxDB client
    influx_client = InfluxDBClient(host='localhost', port=8086)
    influx_client.switch_database(database_name)
    
    measurement_body = []
    while True:
        data_list = queue.get()  # Get the data from the queue
        if data_list is not None and data_list != "STOP":
            measurement = getMeasurement(inserted_msg_count,data_list)
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
    
        
def extractFromDatabase():
    
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
    
    
    # Convert 'time' column to datetime format with timezone specifier 'Z'
    df['storage_time'] = pd.to_datetime(df['time'], format="%Y-%m-%dT%H:%M:%S.%fZ")
    
    df['storage_time'] = pd.to_datetime(df['storage_time'], format='%Y-%m-%d %H:%M:%S.%f')

    influx_client.close()
    
    return df

        
if __name__ == '__main__':
    
    
    
    # Create a multiprocessing Queue for IPC
    data_queue = multiprocessing.Queue(maxsize=9000000)
    
    no_of_received_msgs_obj = multiprocessing.Value('i', 0)
    no_of_inserted_msgs_obj = multiprocessing.Value('i', 0)

    # Create and start the MQTT process
    mqtt_proc = multiprocessing.Process(target=mqttProcess,args=(data_queue,no_of_received_msgs_obj))
    mqtt_proc.start()

    # Create and start the InfluxDB process
    influx_proc = multiprocessing.Process(target=influxProcess,args=(data_queue,no_of_inserted_msgs_obj))
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
    
    
    


