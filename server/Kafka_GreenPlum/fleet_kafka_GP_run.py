from kafka import KafkaConsumer
import psycopg2
import json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import multiprocessing
from psycopg2 import sql
import psycopg2.extras
from datetime import datetime


# Configuration for connecting to your Kafka server
kafka_server = '127.0.0.1:9092'  # this to the Kafka server address
topic_name = 'OBD2_data'
consumer_timeout_in_ms = 20000
received_msgs = []
time_diff_list = []

# Configuration for GreenPlum Database
user = 'mina_yousry_iti'
password = 'my_psw'
host = 'localhost'
port = '5432'  # Default port for Greenplum and PostgreSQL
default_dbname = "postgres"
dbname = "OBD2_Data_Fleet_database"
table_name = "OBD2_table"
db_batch_size = 100

def getcurrentTimestamp():
    now = datetime.now()
    formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    return formatted_date_time         

def getInsertionSqlQuery(use_database_timestamp):
    
    if use_database_timestamp:
        insertion_sql_query =f"""INSERT INTO {table_name} (
                            vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
                            lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
                            co2_consumption, deceleration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    else:
        insertion_sql_query =f"""INSERT INTO {table_name} (
                            vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
                            lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
                            co2_consumption, deceleration, storage_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    
    
    return insertion_sql_query
    
    

def kafkaConsumerProcess(queue,no_of_received_msgs_obj):
    
    exit_code = 0
    consumer = None
    
    try:
        consumer = KafkaConsumer(topic_name,
            bootstrap_servers=[kafka_server],
            auto_offset_reset='earliest',
            group_id='my-group',  # Consumer group ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        print('Listening for messages on topic '+ topic_name + '....')
        
        for message in consumer:
            received_msg = message.value
            if received_msg[0] == "STOP":
                print("Received STOP message")
                queue.put("STOP")
                break

            queue.put(received_msg)
            with no_of_received_msgs_obj.get_lock():
                no_of_received_msgs_obj.value += 1
            
                
            
    except Exception as e:
        print(f"Kafka consumer error: {e}")
        exit_code = 1
    finally:
        
        if consumer is not None:
            consumer.commit()
            consumer.close()
    
    exit(exit_code)
    
    
def connectToDatabase():
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = False  
    cursor = conn.cursor()

    return conn,cursor
    
def insertRecord(conn, record,no_of_inserted_msgs_obj,insert_sql_query):
    cursor = conn.cursor()
    try:
        cursor.execute(insert_sql_query,record)
        conn.commit()
        with no_of_inserted_msgs_obj.get_lock():
            no_of_inserted_msgs_obj.value += 1
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert record: {e}")

def storeInDatabaseProcess(queue,no_of_inserted_msgs_obj,use_database_timestamp):
    conn = None
    exit_code = 0

    try:
        conn,cursor = connectToDatabase()
        insert_sql_query = getInsertionSqlQuery(use_database_timestamp)
        while True:
            data = queue.get()
            if data == "STOP":
                break
            if not use_database_timestamp:
                current_timestamp = getcurrentTimestamp()
                data.append(current_timestamp)
            insertRecord(conn, data, no_of_inserted_msgs_obj,insert_sql_query)
    except Exception as e:
        print(f"An error occurred while inserting data into Database: {e}")
        exit_code = 1
    finally:
        closeDatabaseConnection(conn,cursor)
    
    exit(exit_code)

def closeDatabaseConnection(cursor,conn):
    # Close the cursor and connection
    cursor.close()
    conn.close()
      
def extractFromDatabase(use_database_timestamp):

    conn,cursor = connectToDatabase()

    # Define your SQL query
    query = f"""
            SELECT vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
            lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
            co2_consumption, deceleration, storage_time FROM {table_name};
            """

    # Execute the query and fetch all data
    df = pd.read_sql_query(query, conn)

    df.columns = ['VehicleId', 'tx_time', 'x_pos', 'y_pos', 'gps_lon', 'gps_lat',
                  'Speed', 'RoadID', 'LaneId', 'Displacement', 'TurnAngle', 'Acceleration',
                  'FuelConsumption', 'Co2Consumption', 'Deceleration', 'storage_time']

    closeDatabaseConnection(conn,cursor)
    
    return df



def insertRecords(conn, records,insert_sql_query):
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_batch(cursor, insert_sql_query, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert batch: {e}")


def storeInDatabaseBatchProcess(queue,no_of_inserted_msgs_obj,use_database_timestamp):
    
    exit_code = 0

    conn,cursor = connectToDatabase()

    # Turn autocommit off for batching
    conn.autocommit = False
    
    records_to_insert = []
    
    insert_sql_query = getInsertionSqlQuery(use_database_timestamp)
    
    try:
        while True:
            data = queue.get()
            
            
            if data == "STOP":
                if len(records_to_insert) > 0:
                    insertRecords(conn, records_to_insert,insert_sql_query)
                    with no_of_inserted_msgs_obj.get_lock():
                        no_of_inserted_msgs_obj.value += len(records_to_insert)
                
                break
            else:
                if not use_database_timestamp:
                    current_timestamp = getcurrentTimestamp()
                    data.append(current_timestamp)
                
                records_to_insert.append(tuple(data))
                if len(records_to_insert) >= db_batch_size:  # Adjust batch size as appropriate
                    insertRecords(conn, records_to_insert,insert_sql_query)
                    with no_of_inserted_msgs_obj.get_lock():
                        no_of_inserted_msgs_obj.value += db_batch_size
                    records_to_insert = []
                    
    except Exception as e:
        print(f"An error occurred while inserting data into Database: {e}")
        exit_code = 1

    finally:
        closeDatabaseConnection(conn,cursor)
    
    exit(exit_code)

if __name__ == '__main__':
    
    
    use_database_timestamp = True 
    # Create a Queue for inter-process communication
    queue = multiprocessing.Queue()
    
    no_of_received_msgs_obj = multiprocessing.Value('i', 0)
    no_of_inserted_msgs_obj = multiprocessing.Value('i', 0)
    

    # Create and start the Kafka consumer process
    consumer_process = multiprocessing.Process(target=kafkaConsumerProcess, args=(queue,no_of_received_msgs_obj))
    consumer_process.start()

    # Create and start the database inserter process
    db_process = multiprocessing.Process(target=storeInDatabaseProcess, args=(queue,no_of_inserted_msgs_obj,use_database_timestamp))
    db_process.start()
    
    consumer_process.join()

    # Once the consumer process is done, send a "STOP" message to the db_inserter process
    queue.put("STOP")
    db_process.join()
    
    extractFromDatabase()

    
