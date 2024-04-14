import time
from influxdb import InfluxDBClient
import multiprocessing
import paho.mqtt.client as mqtt
from datetime import datetime
import pandas as pd

database_name = "obd2_database"
mqtt_broker_address = "localhost"
port_no = 1883


# MQTT callback functions
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("mqtt/topic")


def on_message(client, userdata, msg):
    #print(f"Received message: {msg.payload.decode()}")
    data_list = msg.payload.decode().split(',')
    data_queue.put(data_list)  # Put the data into the queue
    
def on_socket_close(client, userdata, msg):
    print(f"Socket closed")
    
def on_disconnect(client, userdata, rc):
    print(f"Disconnected with result code {rc}")
    global socket_closed
    socket_closed = True
    
    

# MQTT process
def mqtt_process(data_queue):
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
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
        
        if (time_diff > 20) and (data_queue.empty()):
            mqtt_client.loop_stop()
            print("finished")
            data_queue.put("STOP")
            mqtt_client.loop_stop()
            break
        elif not data_queue.empty():
            # Reset the start time
            start_time = time.time()
    
    

# InfluxDB process
def influx_process(influx_client, data_queue):
    msg_id = 0
    measurement_body = []
    while True:
        data_list = data_queue.get()  # Get the data from the queue
        if data_list is not None and data_list != "STOP":
            timestamp  = datetime.now() 
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
                    "storage_time": str(timestamp)
                }
            }
            #print(measurement)
            measurement_body.append(measurement)
            msg_id += 1
        else:
            # End the process
            break
    influx_client.write_points(measurement_body)
        
def create_excel_file(influx_client):
    
    print("Creating Excel file")

    all_data_frames = [] 
     
    # Fetch all measurements
    measurements = influx_client.query('SHOW MEASUREMENTS').get_points()
    measurement_names = [measurement['name'] for measurement in measurements]

    # Drop each measurement
    print(len(measurement_names))
    
    dict_list = []
    
    for name in measurement_names:
        result = influx_client.query(f"SELECT * FROM \"{str(name)}\"")
        points = list(result.get_points())
        
        
        for i in range(len(points)):
            dict_list.append(points[i])
        
    df = pd.DataFrame(dict_list)
    
    df.to_excel('obd2_data_report.xlsx', index=False)
    print("Excel file created")

if __name__ == '__main__':
    
    # Set up InfluxDB client
    influx_client = InfluxDBClient(host='localhost', port=8086)
    influx_client.switch_database(database_name)

    #create_excel_file(influx_client)
     
     
    # Create a multiprocessing Queue for IPC
    data_queue = multiprocessing.Queue()

    # Create and start the MQTT process
    mqtt_proc = multiprocessing.Process(target=mqtt_process,args=(data_queue,))
    mqtt_proc.start()

    # Create and start the InfluxDB process
    influx_proc = multiprocessing.Process(target=influx_process,args=(influx_client,data_queue,))
    influx_proc.start()


    # Wait for both processes to finish
    mqtt_proc.join()
    
    # Signal the InfluxDB process to stop
    data_queue.put("STOP")
    
    influx_proc.join()
    
    #mqtt_proc.terminate()
    create_excel_file(influx_client)
    
    influx_client.close()
    
    print("End of program")
    
    
    


