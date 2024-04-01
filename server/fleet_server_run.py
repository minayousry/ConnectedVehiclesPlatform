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
consumer_timeout_in_ms = 10000
received_msgs = []
time_diff_list = []

# Configuration for GreenPlum Database
user = 'mina_yousry_iti'
password = 'my_psw'
host = 'localhost'
port = '5432'  # Default port for Greenplum and PostgreSQL
default_dbname = "postgres"
dbname = "OBD2_Data_Fleet_database"
db_batch_size = 50

# Kafka Consumer to receive messages from the 'test' topic
def kafkaConsumer(queue):
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
    finally:
        queue.put("STOP") 


def createTable(cursor):

    # Create a table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS OBD2_table (
        id SERIAL PRIMARY KEY,
        vehicle_id TEXT,
        tx_time TIMESTAMP WITHOUT TIME ZONE,
        x_pos DOUBLE PRECISION,
        y_pos DOUBLE PRECISION,
        gps_lon DOUBLE PRECISION,
        gps_lat DOUBLE PRECISION,
        speed DOUBLE PRECISION,
        road_id TEXT,
        lane_id TEXT,
        displacement DOUBLE PRECISION,
        turn_angle DOUBLE PRECISION,
        acceleration DOUBLE PRECISION,
        fuel_consumption DOUBLE PRECISION,
        co2_consumption DOUBLE PRECISION,
        deceleration DOUBLE PRECISION,
        storage_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP                   
        );
    """)    

def connectToDatabase():
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = False  
    cursor = conn.cursor()

    return conn,cursor


def CreateDatabaseifNotExists():
    
    # Connect to Greenplum
    conn = psycopg2.connect(dbname=default_dbname, user=user, password=password, host=host)
    conn.autocommit = True  # Needed to create a database
    cursor = conn.cursor()

    # Check if the database exists
    cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"), (dbname,))
    result = cursor.fetchone() is not None

    if not result:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))

    # Close connection
    cursor.close()
    conn.close()

    return result


def insertRecords(conn, records):
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_batch(cursor, """
                INSERT INTO OBD2_table (
                    vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
                    lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
                    co2_consumption, deceleration
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert batch: {e}")
    finally:
        cursor.close()


def storeInDatabase(queue):

    conn,cursor = connectToDatabase()

    # Turn autocommit off for batching
    conn.autocommit = False
    
    createTable(cursor)
    clearTable(conn,cursor,)

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

    finally:
        calculateTimeDifference(conn,cursor)
        closeDatabaseConnection(conn,cursor)


def calculateTimeDifference(conn,cursor):

    # SQL query to calculate the time difference in seconds
    query = """
        SELECT id, tx_time, storage_time,
        EXTRACT(EPOCH FROM (storage_time - tx_time)) AS time_difference_seconds
        FROM OBD2_table;
    """

    try:
        # Execute the query
        cursor.execute(query)

        # Fetch and print the results
        results = cursor.fetchall()

        for row in results:
            time_diff_list.append(row[3])          
            print(f"ID: {row[0]}, TX Time: {row[1]}, Storage Time: {row[2]}, Time Difference (seconds): {row[3]}")

    except Exception as e:

        print(f"Failed to execute select query: {e}")
    
def createExcelFile():

    conn,cursor = connectToDatabase()

    # Define your SQL query
    query = """
    SELECT vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
    lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
    co2_consumption, deceleration, storage_time, 
    EXTRACT(EPOCH FROM (storage_time - tx_time)) AS time_difference_seconds
    FROM OBD2_table;
    """

    # Execute the query and fetch all data
    df = pd.read_sql_query(query, conn)

    df.columns = ['VehicleId', 'Tx_DateTime', 'x_pos', 'y_pos', 'gps_lon', 'gps_lat',
                  'Speed', 'RoadID', 'LaneId', 'Displacement', 'TurnAngle', 'Acceleration',
                  'FuelConsumption', 'Co2Consumption', 'Deceleration', 'database_storage_time',"time_difference_in_secs"]

    try:
        #received_msgs.append(time_diff_list)
        # Generate Excel file
        df.to_excel("obd2_data_report.xlsx", index=False)
    
    except Exception as e:
            print(f"Failed to create excel file: {e}")
    finally:
        closeDatabaseConnection(conn,cursor)

def closeDatabaseConnection(cursor,conn):
    # Close the cursor and connection
    cursor.close()
    conn.close()


def clearTable(conn,cursor):
    try:

        # SQL command to TRUNCATE table
        truncate_command = f"TRUNCATE TABLE OBD2_table;"
        
        # Execute the command
        cursor.execute(truncate_command)
        
        # Commit the transaction
        conn.commit()
        
        print(f"Table OBD2_table has been cleared.")
        
    except Exception as e:
        print(f"An error occurred: {e}")




if __name__ == '__main__':
    
    
    database_exist = CreateDatabaseifNotExists()

    # Create a Queue for inter-process communication
    queue = Queue()

    # Create and start the Kafka consumer process
    consumer_process = Process(target=kafkaConsumer, args=(queue,))
    consumer_process.start()

    # Create and start the database inserter process
    db_process = Process(target=storeInDatabase, args=(queue,))
    db_process.start()

    
    consumer_process.join()

    # Once the consumer process is done, send a "STOP" message to the db_inserter process
    queue.put("STOP")
    db_process.join()

    createExcelFile()
    
