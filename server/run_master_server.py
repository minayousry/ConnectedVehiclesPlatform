import multiprocessing
import argparse


import Kafka_GreenPlum.fleet_kafka_GP_run as kafka_gp
import mqtt_Influx.fleet_mqtt_influx_run as mqtt_influx
import qpid_cassandra.fleet_qpid_cassandra_run as qpid_cassandra
import webSockets_Postgresql.fleet_ws_postpresql_run as websocket_postgresql
import webSockets_Redis.fleet_ws_redis_run as websocket_redis

#import utilities
import server_utilities as server_utilities


enable_database_batch_inserion = False

def runProcesses(comm_process, database_process):
    
    result = False

    no_of_received_msgs_obj = multiprocessing.Value('i', 0)
    no_of_inserted_msgs_obj = multiprocessing.Value('i', 0)
    
    try:
        # Create a multiprocessing Queue for IPC
        data_queue = multiprocessing.Queue(maxsize=9900000)
            
        # Create and start the communication process
        comm_proc = multiprocessing.Process(target=comm_process, args=(data_queue,no_of_received_msgs_obj))
        comm_proc.start()
        
        # Create and start the database process
        db_proc = multiprocessing.Process(target=database_process, args=(data_queue,no_of_inserted_msgs_obj))
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
        return result,no_of_received_msgs_obj.value,no_of_inserted_msgs_obj.value


def createReport(database_extract_func,generation_path):
    
    print("Extracting information from the database...")
    extracted_df = database_extract_func()
    
    if extracted_df is not None:
        print("Creating excel file...")
        server_utilities.createExcelFile(extracted_df,generation_path)
    


   
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
    generation_path = "fleetManager/server/Kafka_GreenPlum/"
    
    srever_techs = ["mqtt_influx", "kafka_greenplum", "qpid_cassandra", "websocket_postgresql", "websocket_redis"]
    
    if server_tech not in srever_techs:
        print("Invalid server technology. Please select one of the following: mqtt_influx, kafka_greenplum, qpid_cassandra, websocket_postgresql or websocket_redis")
        exit(1)
    
    server_utilities.recordStartreceptionStorageTime(server_tech)
    
    
    if server_tech == "kafka_greenplum":
        comm_process = kafka_gp.kafkaConsumerProcess
        
        if enable_database_batch_inserion:
            database_process = kafka_gp.storeInDatabaseBatchProcess
        else:
            database_process = kafka_gp.storeInDatabaseProcess
            
        database_extract_func = kafka_gp.extractFromDatabase
        generation_path = "./Kafka_GreenPlum/"
        
    elif server_tech == "mqtt_influx":
        comm_process = mqtt_influx.mqttProcess
        
        if enable_database_batch_inserion:
            database_process = mqtt_influx.influxBatchProcess
        else:
            database_process = mqtt_influx.influxProcess
            
        database_extract_func = mqtt_influx.extractFromDatabase
        generation_path = "./mqtt_Influx/"
        
    elif server_tech == "qpid_cassandra":
        comm_process = qpid_cassandra.receiverProcess
        
        if enable_database_batch_inserion:
            database_process = qpid_cassandra.databaseBatchProcess
        else:
            database_process = qpid_cassandra.databaseProcess
            
        database_extract_func = qpid_cassandra.extractFromDatabase
        generation_path = "./qpid_cassandra/"
        
        
    elif server_tech == "websocket_postgresql":
        comm_process = websocket_postgresql.websocketServerProcess
        
        if enable_database_batch_inserion:
            database_process = websocket_postgresql.storeInDatabaseBatchProcess
        else:
            database_process = websocket_postgresql.storeInDatabaseProcess
            
        database_extract_func = websocket_postgresql.extractFromDatabase
        generation_path = "./webSockets_Postgresql/"
        
        
    elif server_tech == "websocket_redis":
        comm_process = websocket_redis.websocketServerProcess
        if enable_database_batch_inserion:
            database_process = websocket_redis.dbBatchWriterProcess
        else:
            database_process = websocket_redis.dbWriterProcess
        database_extract_func = websocket_redis.extractFromDatabase
        generation_path = "./webSockets_Redis/"
        
 
    try:
        
        result,no_of_received_msgs,no_of_inserted_records = runProcesses(comm_process, database_process)
        
        if result:
            print("Processes have finished successfully.")
            server_utilities.recordEndreceptionStorageTime(server_tech)
            server_utilities.setReceivedMsgCount(server_tech,no_of_received_msgs)
            server_utilities.setInsertedMsgCount(server_tech,no_of_inserted_records)
            server_utilities.createProfilingReport(server_tech)
            createReport(database_extract_func,generation_path)

            

        else:
            print("Processes have terminated for some errors.")
            exit(1)
        
        
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)


