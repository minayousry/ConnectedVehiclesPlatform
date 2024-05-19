
import sys
import os
import pandas as pd
import requests
import datetime
import pytz

import configurations as cfg

# Global variables
kafka_greenplum_received_msg_count = 0
mqtt_influx_received_msg_count = 0
qpid_cassandra_received_msg_count = 0
websocket_postgresql_received_msg_count = 0
websocket_redis_received_msg_count = 0

kafka_greenplum_inserted_msg_count = 0
mqtt_influx_inserted_msg_count = 0
qpid_cassandra_inserted_msg_count = 0
websocket_postgresql_inserted_msg_count = 0
websocket_redis_inserted_msg_count = 0


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


technologies = ["mqtt_influx", "kafka_greenplum", "qpid_cassandra", "websocket_postgresql", "websocket_redis"]


def getdatetime():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    currentDT = utc_now.astimezone(pytz.timezone("Atlantic/Reykjavik"))
    DATIME = currentDT.strftime("%Y-%m-%d %H:%M:%S.%f")
    return DATIME

def resetMsgCount(technology):
    global kafka_greenplum_received_msg_count
    global mqtt_influx_received_msg_count
    global qpid_cassandra_received_msg_count
    global websocket_postgresql_received_msg_count
    global websocket_redis_received_msg_count

    if technology == "kafka_greenplum":
        kafka_greenplum_received_msg_count = 0
    elif technology == "mqtt_influx":
        mqtt_influx_received_msg_count = 0
    elif technology == "qpid_cassandra":
        qpid_cassandra_received_msg_count = 0
    elif technology == "websocket_postgresql":
        websocket_postgresql_received_msg_count = 0
    elif technology == "websocket_redis":
        websocket_redis_received_msg_count = 0
    else:
        print("Invalid client name.")
        exit(1)

def setReceivedMsgCount(technology, count):
    global kafka_greenplum_received_msg_count
    global mqtt_influx_received_msg_count
    global qpid_cassandra_received_msg_count
    global websocket_postgresql_received_msg_count
    global websocket_redis_received_msg_count

    if technology == "kafka_greenplum":
        kafka_greenplum_received_msg_count = count
    elif technology == "mqtt_influx":
        mqtt_influx_received_msg_count = count
    elif technology == "qpid_cassandra":
        qpid_cassandra_received_msg_count = count
    elif technology == "websocket_postgresql":
        websocket_postgresql_received_msg_count = count
    elif technology == "websocket_redis":
        websocket_redis_received_msg_count = count
    else:
        print("setReceivedMsgCount:Unknown Technology")
        exit(1)

def setInsertedMsgCount(technology, count):
    global kafka_greenplum_inserted_msg_count
    global mqtt_influx_inserted_msg_count
    global qpid_cassandra_inserted_msg_count
    global websocket_postgresql_inserted_msg_count
    global websocket_redis_inserted_msg_count

    if technology == "kafka_greenplum":
        kafka_greenplum_inserted_msg_count = count
    elif technology == "mqtt_influx":
        mqtt_influx_inserted_msg_count = count
    elif technology == "qpid_cassandra":
        qpid_cassandra_inserted_msg_count = count
    elif technology == "websocket_postgresql":
        websocket_postgresql_inserted_msg_count = count
    elif technology == "websocket_redis":
        websocket_redis_inserted_msg_count = count
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
    global kafka_greenplum_received_msg_count
    global mqtt_influx_received_msg_count
    global qpid_cassandra_received_msg_count
    global websocket_postgresql_received_msg_count
    global websocket_redis_received_msg_count
    
    global kafka_greenplum_inserted_msg_count
    global mqtt_influx_inserted_msg_count
    global qpid_cassandra_inserted_msg_count
    global websocket_postgresql_inserted_msg_count
    global websocket_redis_inserted_msg_count

    reception_storage_duration = None
    received_msg_count = None
    inserted_msg_count = None
    
    if technology not in technologies:
        print("Unkonow Technology")
        exit(1)
    else:
        reception_storage_duration = calculatereceptionStorageDuration(technology)
        if technology == "kafka_greenplum":
            received_msg_count = kafka_greenplum_received_msg_count
            inserted_msg_count = kafka_greenplum_inserted_msg_count
        elif technology == "mqtt_influx":
            received_msg_count = mqtt_influx_received_msg_count
            inserted_msg_count = mqtt_influx_inserted_msg_count
        elif technology == "qpid_cassandra":
            received_msg_count = qpid_cassandra_received_msg_count
            inserted_msg_count = qpid_cassandra_inserted_msg_count
        elif technology == "websocket_postgresql":
            received_msg_count = websocket_postgresql_received_msg_count
            inserted_msg_count = websocket_postgresql_inserted_msg_count
        elif technology == "websocket_redis":
            received_msg_count = websocket_redis_received_msg_count
            inserted_msg_count = websocket_redis_inserted_msg_count
            
    
    print(f"No of Received messages: {received_msg_count}")
    print(f"No of Inserted records: {inserted_msg_count}")
    print(f"Reception and storage duration: {reception_storage_duration} seconds")
    

def createExcelFile(obd2_data_frame,generation_path,server_tech):

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
        time_diff = obd2_data_frame['storage_time'] - obd2_data_frame['tx_time']
        
        # Convert time difference to seconds (assuming all values are valid)
        obd2_data_frame['time_diff_seconds'] = time_diff.dt.total_seconds().abs()
        
        print(f"Average diff time:{obd2_data_frame['time_diff_seconds'].mean()}")
        file_name = generation_path+server_tech
        
        if cfg.enable_database_batch_inserion:
            file_name += "_batched"
        
        file_name += "_obd2_data_report.xlsx"
        # Generate Excel report
        obd2_data_frame.to_excel(file_name, index=False)
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
