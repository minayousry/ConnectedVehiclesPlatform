import multiprocessing
import argparse


import comm_tech.kafka.kafka_run as kafka
import comm_tech.mqtt.mqtt_run as mqtt
import comm_tech.qpid.qpid_run as qpid
import comm_tech.websocket.websocket_run as websocket

import database_tech.greenplum.greenplum_run as greenplum
import database_tech.influx.influx_run as influx
import database_tech.cassandra.cassandra_run as cassandra
import database_tech.postgresql.postpresql_run as postgresql
import database_tech.redis.redis_run as redis

import ctypes

#import utilities
import server_utilities as server_utilities

import configurations as cfg

from datetime import datetime,timezone

comm_modules = {"kafka":kafka,"mqtt":mqtt,"qpid":qpid,"websocket":websocket}
databas_modules = {"greenplum":greenplum,"influx":influx,"cassandra":cassandra,"postgresql":postgresql,"redis":redis}

def floatToStringTimestamp(last_storage_timestamp_obj, format='%Y-%m-%d %H:%M:%S.%f'):
    float_timestamp = 0
    with last_storage_timestamp_obj.get_lock():
        float_timestamp = last_storage_timestamp_obj.value
        
    dt = datetime.fromtimestamp(float_timestamp, tz=timezone.utc)
    timestamp_str = dt.strftime(format)
    return timestamp_str

def runProcesses(server_tech,comm_process, database_process):
    
    result = False

    no_of_received_msgs_obj = multiprocessing.Value('i', 0)
    no_of_sent_msgs_obj = multiprocessing.Value('i', 0)
    last_storage_timestamp_obj = multiprocessing.Value('d', 0.0)
    
    total_size_bytes = 9900000
    if server_tech == "mqtt_influx":
        total_size_bytes = 0
    
    try:
        # Create a multiprocessing Queue for IPC
        data_queue = multiprocessing.Queue(maxsize=total_size_bytes)
            
        # Create and start the communication process
        comm_proc = multiprocessing.Process(target=comm_process, args=(data_queue,no_of_received_msgs_obj,no_of_sent_msgs_obj))
        comm_proc.start()
        
        # Create and start the database process
        db_proc = multiprocessing.Process(target=database_process, args=(data_queue,last_storage_timestamp_obj,cfg.use_database_timestamp))
        db_proc.start()
        
        # Wait for both processes to finish
        comm_proc.join()
        
        if comm_proc.exitcode != 0:
            print("comm Process terminated with exit code:",comm_proc.exitcode)
            result = False
        else:
            # Signal the database process to stop
            data_queue.put("STOP")

            db_proc.join()
        
            if db_proc.exitcode == 0:
                result = True
            else:
                print("DBC Process terminated with exit code:")
                result = False
    except Exception as e:
        print(f"An error occurred: {e}")
        result = False
    finally:
        timestamp_str = floatToStringTimestamp(last_storage_timestamp_obj)
        
        return result,no_of_received_msgs_obj.value,no_of_sent_msgs_obj.value,timestamp_str


def createReport(database_extract_func,server_tech,last_storage_timestamp):
    
    print("Extracting information from the database...")
    extracted_df,db_batch_size = database_extract_func(cfg.use_database_timestamp)
    
    if extracted_df is not None:
        print("Creating excel file...")
        server_utilities.createExcelFile(extracted_df,server_tech,cfg.enable_database_batch_inserion,db_batch_size,last_storage_timestamp)
    


   
if __name__ == '__main__':
    
    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add a string argument
    parser.add_argument('serevr_technology', type=str, help='select which technology to use for the server.')

    # Parse the arguments
    args = parser.parse_args()

    # Access the parsed argument
    server_tech = args.serevr_technology
    

    comm_process = None
    database_process = None
    database_extract_func = None
    reporting_module = None
    
    srever_techs = ["mqtt_influx", "kafka_greenplum", "qpid_cassandra", "websocket_postgresql", "websocket_redis","kafka_redis","qpid_redis","qpid_greenplum"]
    
    if server_tech not in srever_techs:
        print("Invalid server technology. Please select one of the following: mqtt_influx, kafka_greenplum, qpid_cassandra, websocket_postgresql or websocket_redis")
        exit(1)
    
    server_utilities.recordStartreceptionStorageTime(server_tech)
    
    
    if server_tech == "kafka_greenplum":
        comm_process = kafka.kafkaConsumerProcess
        
        if cfg.enable_database_batch_inserion:
            database_process = greenplum.storeInDatabaseBatchProcess
        else:
            database_process = greenplum.storeInDatabaseProcess
            
        database_extract_func = greenplum.extractFromDatabase

        
    elif server_tech == "mqtt_influx":
        comm_process = mqtt.mqttProcess
        
        if cfg.enable_database_batch_inserion:
            database_process = influx.influxBatchProcess
        else:
            database_process = influx.influxProcess
            
        database_extract_func = influx.extractFromDatabase
        
    elif server_tech == "qpid_cassandra":
        comm_process = qpid.receiverProcess
        
        if cfg.enable_database_batch_inserion:
            database_process = cassandra.databaseBatchProcess
        else:
            database_process = cassandra.databaseProcess
            
        database_extract_func = cassandra.extractFromDatabase
        
        
    elif server_tech == "websocket_postgresql":
        comm_process = websocket.websocketServerProcess
        
        if cfg.enable_database_batch_inserion:
            database_process = postgresql.storeInDatabaseBatchProcess
        else:
            database_process = postgresql.storeInDatabaseProcess
            
        database_extract_func = postgresql.extractFromDatabase
        
        
    elif server_tech == "websocket_redis":
        comm_process = websocket.websocketServerProcess
        if cfg.enable_database_batch_inserion:
            database_process = redis.dbBatchWriterProcess
        else:
            database_process = redis.dbWriterProcess
        database_extract_func = redis.extractFromDatabase
    
    elif server_tech == "kafka_redis":
        comm_process = kafka.kafkaConsumerProcess
        
        if cfg.enable_database_batch_inserion:
            database_process = redis.dbBatchWriterProcess
        else:
            database_process = redis.dbWriterProcess
        database_extract_func = redis.extractFromDatabase
        
    elif server_tech == "qpid_redis":
        comm_process = qpid.receiverProcess
        
        if cfg.enable_database_batch_inserion:
            database_process = redis.dbBatchWriterProcess
        else:
            database_process = redis.dbWriterProcess
        database_extract_func = redis.extractFromDatabase
        
    elif server_tech == "qpid_greenplum":
        comm_process = qpid.receiverProcess
        
        
        if cfg.enable_database_batch_inserion:
            database_process = kafka.storeInDatabaseBatchProcess
        else:
            database_process = greenplum.storeInDatabaseProcess
            
        database_extract_func = greenplum.extractFromDatabase
        
    try:
        
        result,no_of_received_msgs,no_of_sent_msgs,last_storage_timestamp = runProcesses(server_tech,comm_process, database_process)

        if result:
            print("Processes have finished successfully.")
            server_utilities.recordEndreceptionStorageTime(server_tech)
            server_utilities.setReceivedMsgCount(server_tech,no_of_received_msgs)
            server_utilities.setSentMsgCount(server_tech,no_of_sent_msgs)
            createReport(database_extract_func,server_tech,last_storage_timestamp)
            server_utilities.createProfilingReport(server_tech)

        else:
            print("Processes have terminated for some errors.")
            exit(1)
        
        
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)


