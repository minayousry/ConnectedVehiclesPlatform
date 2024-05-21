
import sys
import os
import pandas as pd
import requests
import datetime
import pytz

import configurations as cfg

# Global variables
kafka_received_msg_count = 0
mqtt_received_msg_count = 0
qpid_received_msg_count = 0
ws_received_msg_count = 0

kafka_sent_msg_count = 0
mqtt_sent_msg_count = 0
qpid_sent_msg_count = 0
ws_sent_msg_count = 0

greenplum_inserted_msg_count = 0
influx_inserted_msg_count = 0
cassandra_inserted_msg_count = 0
postgresql_inserted_msg_count = 0
redis_inserted_msg_count = 0


kafka_greenplum_start_reception_storage_time = None
mqtt_influx_start_reception_storage_time = None
qpid_cassandra_start_reception_storage_time = None
websocket_postgresql_start_reception_storage_time  = None
websocket_redis_start_reception_storage_time  = None

kafka_greenplum_end_reception_storage_time = None
mqtt_influx_end_reception_storage_time = None
qpid_cassandra_end_reception_storage_time = None
websocket_postgresql_end_reception_storage_time  = None
websocket_redis_end_reception_storage_time = None

average_communication_latency = 0
average_storage_latency = 0
average_transaction_latency = 0

no_of_cars = 0

technologies = ["mqtt_influx", "kafka_greenplum", "qpid_cassandra", "websocket_postgresql", "websocket_redis"]


def getdatetime():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    currentDT = utc_now.astimezone(pytz.timezone("Atlantic/Reykjavik"))
    DATIME = currentDT.strftime("%Y-%m-%d %H:%M:%S.%f")
    return DATIME

def resetMsgCount(technology):
    global kafka_received_msg_count
    global mqtt_received_msg_count
    global qpid_received_msg_count
    global ws_received_msg_count

    if technology == "kafka_greenplum":
        kafka_received_msg_count = 0
    elif technology == "mqtt_influx":
        mqtt_received_msg_count = 0
    elif technology == "qpid_cassandra":
        qpid_received_msg_count = 0
    elif technology == "websocket_postgresql":
        ws_received_msg_count = 0
    elif technology == "websocket_redis":
        ws_received_msg_count = 0
    else:
        print("Invalid client name.")
        exit(1)

def setReceivedMsgCount(technology, count):
    global kafka_received_msg_count
    global mqtt_received_msg_count
    global qpid_received_msg_count
    global ws_received_msg_count

    if technology == "kafka_greenplum":
        kafka_received_msg_count = count
    elif technology == "mqtt_influx":
        mqtt_received_msg_count = count
    elif technology == "qpid_cassandra":
        qpid_received_msg_count = count
    elif technology == "websocket_postgresql":
        ws_received_msg_count = count
    elif technology == "websocket_redis":
        ws_received_msg_count = count
    else:
        print("setReceivedMsgCount:Unknown Technology")
        exit(1)

def setSentMsgCount(technology, count):
    global kafka_sent_msg_count
    global mqtt_sent_msg_count
    global qpid_sent_msg_count
    global ws_sent_msg_count

    if technology == "kafka_greenplum":
        kafka_sent_msg_count = count
    elif technology == "mqtt_influx":
        mqtt_sent_msg_count = count
    elif technology == "qpid_cassandra":
        qpid_sent_msg_count = count
    elif technology == "websocket_postgresql":
        ws_sent_msg_count = count
    elif technology == "websocket_redis":
        ws_sent_msg_count = count
    else:
        print("setSentMsgCount:Unknown Technology")
        exit(1)


def setInsertedMsgCount(technology, count):
    global greenplum_inserted_msg_count
    global influx_inserted_msg_count
    global cassandra_inserted_msg_count
    global postgresql_inserted_msg_count
    global redis_inserted_msg_count

    if technology == "kafka_greenplum":
        greenplum_inserted_msg_count = count
    elif technology == "mqtt_influx":
        influx_inserted_msg_count = count
    elif technology == "qpid_cassandra":
        cassandra_inserted_msg_count = count
    elif technology == "websocket_postgresql":
        postgresql_inserted_msg_count = count
    elif technology == "websocket_redis":
        redis_inserted_msg_count = count
    else:
        print("setInsertedMsgCount:Unknown Technology")
        exit(1)

        
def recordStartreceptionStorageTime(technology):
    global kafka_greenplum_start_reception_storage_time
    global mqtt_influx_start_reception_storage_time
    global qpid_cassandra_start_reception_storage_time
    global websocket_postgresql_start_reception_storage_time
    global websocket_redis_start_reception_storage_time
    

    if technology == "kafka_greenplum":
        kafka_greenplum_start_reception_storage_time = getdatetime()
    elif technology == "mqtt_influx":
        mqtt_influx_start_reception_storage_time = getdatetime()
    elif technology == "qpid_cassandra":
        qpid_cassandra_start_reception_storage_time = getdatetime()
    elif technology == "websocket_postgresql":
        websocket_postgresql_start_reception_storage_time = getdatetime()
    elif technology == "websocket_redis":
        websocket_redis_start_reception_storage_time = getdatetime()
    else:
        print("Invalid client name.")
        exit(1)

def recordEndreceptionStorageTime(technology):
    global kafka_greenplum_end_reception_storage_time
    global mqtt_influx_end_reception_storage_time
    global qpid_cassandra_end_reception_storage_time
    global websocket_postgresql_end_reception_storage_time
    global websocket_redis_end_reception_storage_time
    
    if technology == "kafka_greenplum":
        kafka_greenplum_end_reception_storage_time = getdatetime()
    elif technology == "mqtt_influx":
        mqtt_influx_end_reception_storage_time = getdatetime()
    elif technology == "qpid_cassandra":
        qpid_cassandra_end_reception_storage_time = getdatetime()
    elif technology == "websocket_postgresql":
        websocket_postgresql_end_reception_storage_time = getdatetime()
    elif technology == "websocket_redis":
        websocket_redis_end_reception_storage_time = getdatetime()
    else:
        print("Invalid client name.")
        exit(1)
    
def calculatereceptionStorageDuration(technology):
    global kafka_greenplum_start_reception_storage_time
    global mqtt_influx_start_reception_storage_time
    global qpid_cassandra_start_reception_storage_time
    global websocket_postgresql_start_reception_storage_time
    global websocket_redis_start_reception_storage_time

    global kafka_greenplum_end_reception_storage_time
    global mqtt_influx_end_reception_storage_time
    global qpid_cassandra_end_reception_storage_time
    global websocket_postgresql_end_reception_storage_time
    global websocket_redis_end_reception_storage_time

    if technology == "kafka_greenplum":
        start_time = kafka_greenplum_start_reception_storage_time
        end_time = kafka_greenplum_end_reception_storage_time
    elif technology == "mqtt_influx":
        start_time = mqtt_influx_start_reception_storage_time
        end_time = mqtt_influx_end_reception_storage_time
    elif technology == "qpid_cassandra":
        start_time = qpid_cassandra_start_reception_storage_time
        end_time = qpid_cassandra_end_reception_storage_time
    elif technology == "websocket_postgresql":
        start_time = websocket_postgresql_start_reception_storage_time
        end_time = websocket_postgresql_end_reception_storage_time
    elif technology == "websocket_redis":
        start_time = websocket_redis_start_reception_storage_time
        end_time = websocket_redis_end_reception_storage_time
    else:
        print("Invalid client name.")
        exit(1)
    
    if start_time is None:
        print("reception stoarage start time is not recorded.")
        return None
    
    if end_time is None:
        print("reception stoarage end time is not recorded.")
        return None
        
    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f")
    end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S.%f")
    
    #calculate reception_storageulation duration in seconds
    reception_storage_duration = end_time - start_time
    
    return reception_storage_duration.total_seconds()

     
#report all information about the server performance behaviour        
def createProfilingReport(technology):

    reception_storage_duration = None
    received_msg_count = 0
    sent_msg_count = 0
    inserted_msg_count = 0
    
    if technology not in technologies:
        print("Unkonow Technology")
        exit(1)
    else:
        reception_storage_duration = calculatereceptionStorageDuration(technology)
        if technology == "kafka_greenplum":
            received_msg_count = kafka_received_msg_count
            sent_msg_count = kafka_sent_msg_count
            inserted_msg_count = greenplum_inserted_msg_count
        elif technology == "mqtt_influx":
            received_msg_count = mqtt_received_msg_count
            sent_msg_count = mqtt_sent_msg_count
            inserted_msg_count = influx_inserted_msg_count
        elif technology == "qpid_cassandra":
            received_msg_count = qpid_received_msg_count
            sent_msg_count = qpid_sent_msg_count
            inserted_msg_count = cassandra_inserted_msg_count
        elif technology == "websocket_postgresql":
            received_msg_count = ws_received_msg_count
            sent_msg_count = ws_sent_msg_count
            inserted_msg_count = postgresql_inserted_msg_count
        elif technology == "websocket_redis":
            received_msg_count = ws_received_msg_count
            sent_msg_count = ws_sent_msg_count
            inserted_msg_count = redis_inserted_msg_count
    
    print("Communication Performance Report:")        
    print(f"No of sent messages: {sent_msg_count}")
    print(f"No of Received messages: {received_msg_count}")
    print(f"communication Delivery percentage: {(received_msg_count/sent_msg_count)*100}%")
    print(f"Average communication latency:{average_communication_latency} seconds")
    print("-------------------------------------------------------------------------------")
    print("Storage Performance Report:")  
    print(f"No of Inserted records: {inserted_msg_count}")
    print(f"Storage suucess percentage: {(inserted_msg_count/received_msg_count)*100}%")
    print(f"Average storage latency: {average_storage_latency} seconds")
    print(f"Reception and storage duration: {reception_storage_duration} seconds")
    print("-------------------------------------------------------------------------------")
    print("Transaction Performance Report:")
    print(f"No of transactions:{received_msg_count}")
    print(f"No of cars:{no_of_cars}")
    print(f"Average transaction latency:{average_transaction_latency}")

def createExcelFile(obd2_data_frame,server_tech):

    global average_communication_latency
    global average_storage_latency
    global average_transaction_latency
    global no_of_cars
    
    generation_path = "./reports/"
    
    try:
        if(obd2_data_frame is None):
            print("Error:OBD2 Dataframe is None.")
            return
        
        if(obd2_data_frame.empty):
            print("Error:OBD2 Dataframe is empty.")
            return

        if(len(obd2_data_frame.columns) == 0):
            print("Error:Dataframe has no columns.")
            return
        
        if type(obd2_data_frame['tx_time'].iloc[0]) == str:
            print("Converting tx time to datetime")
            obd2_data_frame['tx_time'] = pd.to_datetime(obd2_data_frame['tx_time'], format='%Y-%m-%d %H:%M:%S.%f')

        if type(obd2_data_frame['storage_time'].iloc[0]) == str:
            print("Converting storage time to datetime")
            obd2_data_frame['storage_time'] = pd.to_datetime(obd2_data_frame['storage_time'], format='%Y-%m-%d %H:%M:%S.%f')

        
        print("Calculating time difference")
        
        
        comm_latency_sec = obd2_data_frame['rx_time'] - obd2_data_frame['tx_time']
        obd2_data_frame['comm_latency_sec'] = comm_latency_sec.dt.total_seconds().abs()
        average_communication_latency = obd2_data_frame['comm_latency_sec'].mean()
         
        write_latency_sec = obd2_data_frame['storage_time'] - obd2_data_frame['rx_time']
        obd2_data_frame['write_latency_sec'] = write_latency_sec.dt.total_seconds().abs()
        average_storage_latency = obd2_data_frame['write_latency_sec'].mean()

        obd2_data_frame['transaction_latency_sec'] = obd2_data_frame['comm_latency_sec'] + obd2_data_frame['write_latency_sec']
        average_transaction_latency = obd2_data_frame['transaction_latency_sec'].mean()

        no_of_cars = obd2_data_frame['VehicleId'].nunique()
        

        file_path = generation_path
        
        file_path += str(no_of_cars)
        file_name = str(no_of_cars)
        
        if cfg.enable_database_batch_inserion:
            file_path += "/batch"
            file_name += "_batch"
        else:
            file_path += "/single"
            file_name += "_single"
        
        file_name += ("_"+server_tech)
        
        file_name += "_obd2_data.xlsx"
        
        full_file_path = os.path.join(file_path, file_name)
        
        print("Full file path:", full_file_path)

        os.makedirs(os.path.dirname(full_file_path), exist_ok=True)
        # Generate Excel report
        obd2_data_frame.to_excel(full_file_path, index=False)
        print("Excel file has been created.")
    
    except Exception as e:
        print(f"Failed to create excel file: {e}")
        
def setFileMode(file_path, new_mode):
    # Check if the file exists
    
    if not os.path.exists(file_path):
        print(f"File '{file_path}' does not exist.")
        return
    
    # Close the file if it's already open
    with open(file_path, 'r') as file:
        pass  # Do nothing, just close the file

    # Reopen the file with the new mode
    with open(file_path, new_mode) as file:
        return file.read()
    
def getExternalIp():
    ip_address = None
    try:
        response = requests.get('https://httpbin.org/ip')
        if response.status_code == 200:
            data = response.json()
            return data['origin']
        else:
            print("Failed to retrieve IP:", response.status_code)
            return None
    except Exception as e:
        print("An error occurred:", e)
        return None
