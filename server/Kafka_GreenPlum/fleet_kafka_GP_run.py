from kafka import KafkaConsumer
import psycopg2
import json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from multiprocessing import Process, Queue
from psycopg2 import sql
import psycopg2.extras

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

def kafkaConsumerProcess(queue):
    
    exit_code = 0
    consumer = None
    
    try:
        consumer = KafkaConsumer(topic_name,
            bootstrap_servers=[kafka_server],
            auto_offset_reset='earliest',  # Start reading at the earliest message
            group_id='my-group',  # Consumer group ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=consumer_timeout_in_ms
        )

        print('Listening for messages on topic '+ topic_name + '....')

        for message in consumer:
            received_msg = message.value
            
            queue.put(received_msg)
            
            
    except Exception as e:
        print(f"Kafka consumer error: {e}")
        exit_code = 1
    finally:
        if consumer is not None:
            consumer.close()
    
    exit(exit_code)
    
    
def connectToDatabase():
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = False  
    cursor = conn.cursor()

    return conn,cursor
    
def insertRecord(conn, record):
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            INSERT INTO {table_name} (
                vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
                lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
                co2_consumption, deceleration
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, record)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert record: {e}")

def storeInDatabaseProcess(queue):
    conn = None
    exit_code = 0

    try:
        conn,cursor = connectToDatabase()
        while True:
            data = queue.get()
            if data == "STOP":
                break
            insertRecord(conn, data)
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
      
def extractFromDatabase():

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




if __name__ == '__main__':
    
     
    # Create a Queue for inter-process communication
    queue = Queue()

    # Create and start the Kafka consumer process
    consumer_process = Process(target=kafkaConsumerProcess, args=(queue,))
    consumer_process.start()

    # Create and start the database inserter process
    db_process = Process(target=storeInDatabaseProcess, args=(queue,))
    db_process.start()
    
    consumer_process.join()

    # Once the consumer process is done, send a "STOP" message to the db_inserter process
    queue.put("STOP")
    db_process.join()
    
    extractFromDatabase()

    

def insertRecords(conn, records):
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_batch(cursor, f"""
                INSERT INTO {table_name} (
                    vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
                    lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
                    co2_consumption, deceleration
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert batch: {e}")


def storeInDatabasebatchProcess(queue):
    
    exit_code = 0

    conn,cursor = connectToDatabase()

    # Turn autocommit off for batching
    conn.autocommit = False
    
    records_to_insert = []
    
    try:
        while True:
            data = queue.get()
            if data == "STOP" and records_to_insert:
                # Insert any remaining records
                insertRecords(conn, records_to_insert)
                break
            elif data == "STOP":
                break
            
            records_to_insert.append(tuple(data))
            if len(records_to_insert) >= db_batch_size:  # Adjust batch size as appropriate
                insertRecords(conn, records_to_insert)
                records_to_insert = []
    except Exception as e:
        print(f"An error occurred while inserting data into Database: {e}")
        exit_code = 1

    finally:
        closeDatabaseConnection(conn,cursor)
    
    exit(exit_code)
