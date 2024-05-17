import asyncio
import psycopg2.extras
import websockets
import multiprocessing
import aiopg
import pandas as pd
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
import psycopg2


# Configuration for postgresql Database
user = 'guest'
password = 'guest'
host = 'localhost'
port = '5432'
default_dbname = "postgres"
dbname = "obd2_content_database"
database_table = "obd2_data_table"
db_batch_size = 100

websocket_port = 8765


def connectToDatabase():
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = False  
    cursor = conn.cursor()

    return conn,cursor

def closeDatabaseConnection(cursor,conn):
    # Close the cursor and connection
    cursor.close()
    conn.close()

def insertRecord(conn, record,no_of_inserted_msgs_obj):
    cursor = conn.cursor()
    try:
        insert_query = f"""
                        INSERT INTO {database_table} (
                        vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
                        lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
                        co2_consumption, deceleration
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """
        cursor.execute(insert_query,record)
        conn.commit()
        with no_of_inserted_msgs_obj.get_lock():
            no_of_inserted_msgs_obj.value += 1
    except Exception as e:
        print(f"Failed to insert record {record} because of error {e}")
        conn.rollback()  

async def websocketServerHandler(websocket, path, queue,no_of_received_msgs_obj):
    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=40)
                queue.put(message)
                with no_of_received_msgs_obj.get_lock():
                    no_of_received_msgs_obj.value += 1
            except asyncio.TimeoutError:
                queue.put("STOP")
                await websocket.close()
                print("Timeout, sent STOP signal to db_writer.")
                break
    except websockets.ConnectionClosed:
        queue.put("STOP")
        await websocket.close()
        # Handle the connection closed, either by client or server
        print("WebSocket connection closed.")



def preprocessData(data):
    

    data = data[1:-1].split(',')
    
     # Strip double quotes from the timestamp field if present
    tx_time_str = data[1].strip('"')


    # Format the data accordingly
    record = (
        data[0],                # vehicle_id
        tx_time_str,                # tx_time
        float(data[2]),         # x_pos
        float(data[3]),         # y_pos
        float(data[4]),         # gps_lon
        float(data[5]),         # gps_lat
        float(data[6]),         # speed
        data[7],                # road_id
        data[8],                # lane_id
        float(data[9]),         # displacement
        float(data[10]),        # turn_angle
        float(data[11]),        # acceleration
        float(data[12]),        # fuel_consumption
        float(data[13]),        # co2_consumption
        float(data[14]),        # deceleration
    )

    return record

async def runWebsocketServer(queue,no_of_received_msgs_obj):
    
    server = await websockets.serve(lambda ws, path: websocketServerHandler(ws, path, queue,no_of_received_msgs_obj), "0.0.0.0", websocket_port)
    print("Listening for incoming websocket connections...")

    try:
        await asyncio.wait_for(asyncio.Future(), timeout=100)
    except asyncio.TimeoutError:
        print("Server timeout reached, shutting down.")
    finally:
        server.close()
        await server.wait_closed()

def websocketServerProcess(queue,no_of_received_msgs_obj):
    asyncio.run(runWebsocketServer(queue,no_of_received_msgs_obj))

def storeInDatabaseProcess(queue, no_of_inserted_msgs_obj):
    
    conn,cursor = connectToDatabase()

    try:
        while True:
            data = queue.get()
            if data == "STOP":
                print("Stopping the database process...")
                break
            insertRecord(conn, eval(data),no_of_inserted_msgs_obj)
    finally:
        closeDatabaseConnection(conn,cursor)


def insertRecords(conn, records):
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_batch(cursor, f"""
                INSERT INTO {database_table} (
                    vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
                    lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
                    co2_consumption, deceleration
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert batch: {e}")

def storeInDatabaseBatchProcess(queue, no_of_inserted_msgs_obj):

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
                with no_of_inserted_msgs_obj.get_lock():
                    no_of_inserted_msgs_obj.value += len(records_to_insert)
                break
            elif data == "STOP":
                break
            
            records_to_insert.append(preprocessData(data))
            
            if len(records_to_insert) >= db_batch_size:  # Adjust batch size as appropriate
                insertRecords(conn, records_to_insert)
                with no_of_inserted_msgs_obj.get_lock():
                    no_of_inserted_msgs_obj.value += db_batch_size
                records_to_insert = []

    finally:
        closeDatabaseConnection(conn,cursor)


    
def extractFromDatabase():

    conn,cursor = connectToDatabase()

    # Define your SQL query
    query = f"""
            SELECT vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
            lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
            co2_consumption, deceleration, storage_time 
            FROM {database_table};
            """

    # Execute the query and fetch all data
    df = pd.read_sql_query(query, conn)

    df.columns = ['VehicleId', 'tx_time', 'x_pos', 'y_pos', 'gps_lon', 'gps_lat',
                  'Speed', 'RoadID', 'LaneId', 'Displacement', 'TurnAngle', 'Acceleration',
                  'FuelConsumption', 'Co2Consumption', 'Deceleration', 'storage_time']

    closeDatabaseConnection(conn,cursor)
    
    return df




 
if __name__ == "__main__":
    queue = multiprocessing.Queue()

    no_of_received_msgs_obj = multiprocessing.Value('i', 0)
    no_of_inserted_msgs_obj = multiprocessing.Value('i', 0)
    
    ws_process = multiprocessing.Process(target=websocketServerProcess, args=(queue,no_of_received_msgs_obj))
    db_process = multiprocessing.Process(target=storeInDatabaseProcess, args=(queue,no_of_inserted_msgs_obj))
    

    db_process.start()
    ws_process.start()

    ws_process.join()
    
    # Once the consumer process is done, send a "STOP" message to the db_inserter process
    queue.put("STOP")
    db_process.join()

    extractFromDatabase()