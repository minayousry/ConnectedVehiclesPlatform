
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

max_communication_latency = 0
max_storage_latency = 0
max_transaction_latency = 0
    
min_communication_latency = 0
min_storage_latency = 0
min_transaction_latency = 0
    

no_of_cars = 0

technologies = ["mqtt_influx", "kafka_greenplum", "qpid_cassandra", "websocket_postgresql", "websocket_redis","kafka_redis","qpid_redis","qpid_greenplum"]


def getdatetime():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    currentDT = utc_now.astimezone(pytz.timezone("Atlantic/Reykjavik"))
    DATIME = currentDT.strftime("%Y-%m-%d %H:%M:%S.%f")
    return DATIME


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
    elif technology == "kafka_redis":
        kafka_received_msg_count = count
    elif technology == "qpid_redis":
        qpid_received_msg_count = count
    elif technology == "qpid_greenplum":
        qpid_received_msg_count = count
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
    elif technology == "kafka_redis":
        kafka_sent_msg_count = count
    elif technology == "qpid_redis":
        qpid_sent_msg_count = count
    elif technology == "qpid_greenplum":
        qpid_sent_msg_count = count
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
    elif technology == "kafka_redis":
        kafka_inserted_msg_count = count
    elif technology == "qpid_redis":
        qpid_inserted_msg_count = count
    elif technology == "qpid_greenplum":
        qpid_isnerted_msg_count = count
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
    elif technology == "kafka_redis":
        kafka_greenplum_start_reception_storage_time = getdatetime()
    elif technology == "qpid_redis":
        qpid_cassandra_start_reception_storage_time = getdatetime()
    elif technology == "qpid_greenplum":
        qpid_cassandra_start_reception_storage_time = getdatetime()
    else:
        print("Invalid client name in recordStartreceptionStorageTime.")
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
    elif technology == "kafka_redis":
        kafka_greenplum_end_reception_storage_time = getdatetime()
    elif technology == "qpid_redis":
        qpid_cassandra_end_reception_storage_time = getdatetime()
    elif technology == "qpid_greenplum":
        qpid_cassandra_end_reception_storage_time = getdatetime()
    else:
        print("Invalid client name in recordEndreceptionStorageTime.")
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
    elif technology == "kafka_redis":
        start_time = kafka_greenplum_start_reception_storage_time
        end_time = kafka_greenplum_end_reception_storage_time
    elif technology == "qpid_redis":
        start_time = qpid_cassandra_start_reception_storage_time
        end_time = qpid_cassandra_end_reception_storage_time
    elif technology == "qpid_greenplum":
        start_time = qpid_cassandra_start_reception_storage_time
        end_time = qpid_cassandra_end_reception_storage_time
    else:
        print("Invalid client name in calculatereceptionStorageDuration.")
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
        elif technology == "kafka_redis":
            received_msg_count = kafka_received_msg_count
            sent_msg_count = kafka_sent_msg_count
            inserted_msg_count = redis_inserted_msg_count
        elif technology == "qpid_redis":
            received_msg_count = qpid_received_msg_count
            sent_msg_count = qpid_sent_msg_count
            inserted_msg_count = redis_inserted_msg_count
        elif technology == "qpid_greenplum":
            received_msg_count = qpid_received_msg_count
            sent_msg_count = qpid_sent_msg_count
            inserted_msg_count = redis_inserted_msg_count
            
    
    print("Communication Performance Report:")        
    print(f"No of sent messages: {sent_msg_count}")
    print(f"No of Received messages: {received_msg_count}")
    if sent_msg_count == 0:
        print("Error:Communication Delivery percentage: 0%")
    else:
        print(f"communication Delivery percentage: {(received_msg_count/sent_msg_count)*100}%")
    print(f"Min communication latency:{min_communication_latency} seconds")
    print(f"Average communication latency:{average_communication_latency} seconds")
    print(f"Max communication latency:{max_communication_latency} seconds")
    print("-------------------------------------------------------------------------------")
    print("Storage Performance Report:")  
    print(f"No of Inserted records: {no_of_inserted_transactions}")
    if received_msg_count == 0:
        print("Error:Storage suucess percentage: 0%")
    else:
        print(f"Storage suucess percentage: {(no_of_inserted_transactions/received_msg_count)*100}%")
    
    print(f"Min Storage latency: {min_storage_latency} seconds")  
    print(f"Average storage latency: {average_storage_latency} seconds")
    print(f"Max Storage latency: {max_storage_latency} seconds")  
    print(f"Reception and storage duration: {reception_storage_duration} seconds")
    print("-------------------------------------------------------------------------------")
    print("Transaction Performance Report:")
    print(f"No of transactions:{received_msg_count}")
    print(f"No of cars:{no_of_cars}")
    print(f"Min transaction latency:{min_transaction_latency} seconds")
    print(f"Average transaction latency:{average_transaction_latency} seconds")
    print(f"Max transaction latency:{max_transaction_latency} seconds")

def createExcelFile(obd2_data_frame,server_tech,is_batch_insertion,db_batch_size,last_storage_timestamp):

    global average_communication_latency
    global average_storage_latency
    global average_transaction_latency
    
    global max_communication_latency
    global max_storage_latency
    global max_transaction_latency
    
    global min_communication_latency
    global min_storage_latency
    global min_transaction_latency
    
    
    global no_of_cars
    global no_of_inserted_transactions
    
    generation_path = "./reports/phase2/"
    
    no_of_inserted_transactions = len(obd2_data_frame)
    
    obd2_data_frame.sort_values(by='tx_time', inplace=True)
    
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
        
        if type(obd2_data_frame['rx_time'].iloc[0]) == str:
            print("Converting rx time to datetime")
            obd2_data_frame['rx_time'] = pd.to_datetime(obd2_data_frame['rx_time'], format='%Y-%m-%d %H:%M:%S.%f')
        
        
        print("last insertion timestamp is:")
        print(last_storage_timestamp)

        
        if is_batch_insertion:
            print("Batch Insertion")
            if no_of_inserted_transactions < db_batch_size:
                obd2_data_frame['storage_time'] = last_storage_timestamp
            else:
                #shift up by db_batch_size
                obd2_data_frame['storage_time'] = obd2_data_frame['storage_time'].shift(db_batch_size * -1)

                storage_time_list = list(obd2_data_frame['storage_time'])
                first_index = storage_time_list.index(None)
                
                

                if first_index != -1:

                    remaining_records_of_last_batch = no_of_inserted_transactions % db_batch_size
                    last_remaining_element_of_last_batch = first_index + (db_batch_size -remaining_records_of_last_batch)

                    prev_storage_time = obd2_data_frame['storage_time'].iloc[first_index - 1]

                    obd2_data_frame['storage_time'].iloc[first_index: last_remaining_element_of_last_batch] = prev_storage_time
                    #obd2_data_frame.loc[first_index: last_remaining_element_of_last_batch, obd2_data_frame.columns.get_loc('storage_time')] = prev_storage_time
                    

                    obd2_data_frame['storage_time'].iloc[last_remaining_element_of_last_batch:] = last_storage_timestamp
                    #obd2_data_frame.loc[last_remaining_element_of_last_batch:, 'storage_time'] = last_storage_timestamp
                    
                    
                else:
                    print("Error in shofting records")
                    
        else:
            print("Single Insertion")
            obd2_data_frame['storage_time'] = obd2_data_frame['storage_time'].shift(-1)
            obd2_data_frame.iloc[-1, obd2_data_frame.columns.get_loc('storage_time')] = last_storage_timestamp

        
        #drop the first row as the storage time is not accurate
        obd2_data_frame.drop(obd2_data_frame.index[0], inplace=True)
        
        if type(obd2_data_frame['storage_time'].iloc[0]) == str:
            print("Converting storage time to datetime")
            obd2_data_frame['storage_time'] = pd.to_datetime(obd2_data_frame['storage_time'], format='%Y-%m-%d %H:%M:%S.%f')
        
        
        print("Calculating time difference")
        
        
        comm_latency_sec = obd2_data_frame['rx_time'] - obd2_data_frame['tx_time']
        obd2_data_frame['comm_latency_sec'] = comm_latency_sec.dt.total_seconds()
        average_communication_latency = obd2_data_frame['comm_latency_sec'].mean()
        max_communication_latency = obd2_data_frame['comm_latency_sec'].max()
        min_communication_latency = obd2_data_frame['comm_latency_sec'].min()
         
        write_latency_sec = obd2_data_frame['storage_time'] - obd2_data_frame['rx_time']
        obd2_data_frame['write_latency_sec'] = write_latency_sec.dt.total_seconds()
        average_storage_latency = obd2_data_frame['write_latency_sec'].mean()
        max_storage_latency = obd2_data_frame['write_latency_sec'].max()
        min_storage_latency = obd2_data_frame['write_latency_sec'].min()
        

        obd2_data_frame['transaction_latency_sec'] = obd2_data_frame['comm_latency_sec'] + obd2_data_frame['write_latency_sec']
        average_transaction_latency = obd2_data_frame['transaction_latency_sec'].mean()
        max_transaction_latency = obd2_data_frame['transaction_latency_sec'].max()
        min_transaction_latency = obd2_data_frame['transaction_latency_sec'].min()

        #no_of_cars = obd2_data_frame['vehicle_id'].nunique()
        no_of_cars = no_of_inserted_transactions

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
        
        obd2_data_frame['tx_time'] = obd2_data_frame['tx_time'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        obd2_data_frame['rx_time'] = obd2_data_frame['rx_time'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        obd2_data_frame['storage_time'] = obd2_data_frame['storage_time'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        
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
