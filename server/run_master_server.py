from multiprocessing import Process, Queue
import argparse


import Kafka_GreenPlum.fleet_kafka_GP_run as kafka_gp
import mqtt_Influx.fleet_mqtt_influx_run as mqtt_influx
import qpid_cassandra.fleet_qpid_cassandra_run as qpid_cassandra
import webSockets_Postgresql.fleet_ws_postpresql_run as websocket_postgresql
import webSockets_Redis.fleet_ws_redis_run as websocket_redis

#import utilities
import server_utilities as server_utilities

def runProcesses(comm_process, database_process):
    try:
        # Create a multiprocessing Queue for IPC
        data_queue = Queue()

        # Create and start the communication process
        comm_proc = Process(target=comm_process, args=(data_queue,))
        comm_proc.start()
        
        # Create and start the database process
        db_proc = Process(target=database_process, args=(data_queue,))
        db_proc.start()
        
        # Wait for both processes to finish
        comm_proc.join()
        
        if comm_proc.exitcode != 0:
            print("comm Process terminated with exit code:",comm_proc.exitcode)
            return False

        # Signal the database process to stop
        data_queue.put("STOP")

        db_proc.join()
        
        if db_proc.exitcode != 0:
            print("DBC Process terminated with exit code:")
            return False
        
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False



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
    generation_path = "fleetManager/server/Kafka_GreenPlum/"
    
    
    if server_tech == "kafka_greenplum":
        comm_process = kafka_gp.kafkaConsumerProcess
        database_process = kafka_gp.storeInDatabaseProcess
        database_extract_func = kafka_gp.extractFromDatabase
        generation_path = "./Kafka_GreenPlum/"
        
    elif server_tech == "mqtt_influx":
        comm_process = mqtt_influx.mqttProcess
        database_process = mqtt_influx.influxProcess
        database_extract_func = mqtt_influx.extractFromDatabase
        generation_path = "./mqtt_Influx/"
        
        
    elif server_tech == "qpid_cassandra":
        comm_process = qpid_cassandra.receiverProcess
        database_process = qpid_cassandra.databaseProcess
        database_extract_func = qpid_cassandra.extractFromDatabase
        generation_path = "./qpid_cassandra/"
        
        
    elif server_tech == "websocket_postgresql":
        comm_process = websocket_postgresql.websocketServerProcess
        database_process = websocket_postgresql.storeInDatabaseProcess
        database_extract_func = websocket_postgresql.extractFromDatabase
        generation_path = "./webSockets_Postgresql/"
        
        
    elif server_tech == "websocket_redis":
        comm_process = websocket_redis.websocketServerProcess
        database_process = websocket_redis.dbWriterProcess
        database_extract_func = websocket_redis.extractFromDatabase
        generation_path = "./webSockets_Redis/"
        
        
    else:
        print("Invalid server technology. Please select one of the following: mqtt_influx, kafka_greenplum, qpid_cassandra, websocket_postgresql or websocket_redis")
        exit(1)
 
    try:        
        result = runProcesses(comm_process, database_process)
        
        if result:
            print("Processes have finished successfully.")
            
            createReport(database_extract_func,generation_path)
            

        else:
            print("Processes have terminated for some errors.")
            exit(1)
        
        
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)


