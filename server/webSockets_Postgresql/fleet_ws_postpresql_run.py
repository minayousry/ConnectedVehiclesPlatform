import asyncio
import websockets
from multiprocessing import Process, Queue
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

received_msgs = []
time_diff_list = []


def connectToDatabase():
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = False  
    cursor = conn.cursor()

    return conn,cursor





async def websocketServerHandler(websocket, path, queue):
    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=20)
                queue.put(message)
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



def preprocess_data(data):
    

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

async def runWebsocketServer(queue):
    server = await websockets.serve(lambda ws, path: websocketServerHandler(ws, path, queue), "0.0.0.0", 8765)

    try:
        await asyncio.wait_for(asyncio.Future(), timeout=100)
    except asyncio.TimeoutError:
        print("Server timeout reached, shutting down.")
    finally:
        server.close()
        await server.wait_closed()

def websocketServerProcess(queue):
    asyncio.run(runWebsocketServer(queue))


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


def storeInDatabase(queue):

    conn,cursor = connectToDatabase()

    # Turn autocommit off for batching
    conn.autocommit = False
    
    #createTable(cursor)
    #clearTable(conn,cursor,)

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
            
            records_to_insert.append(preprocess_data(data))
            
            if len(records_to_insert) >= db_batch_size:  # Adjust batch size as appropriate
                insertRecords(conn, records_to_insert)
                records_to_insert = []

    finally:
        calculateTimeDifference(conn,cursor)
        closeDatabaseConnection(conn,cursor)


def calculateTimeDifference(conn,cursor):

    # SQL query to calculate the time difference in seconds
    query = f"""
        SELECT id, tx_time, storage_time,
        EXTRACT(EPOCH FROM (storage_time - tx_time)) AS time_difference_seconds
        FROM {database_table};
    """

    try:
        # Execute the query
        cursor.execute(query)

        # Fetch and print the results
        results = cursor.fetchall()

        for row in results:
            time_diff_list.append(row[3])          
            #print(f"ID: {row[0]}, TX Time: {row[1]}, Storage Time: {row[2]}, Time Difference (seconds): {row[3]}")

    except Exception as e:

        print(f"Failed to execute select query: {e}")
    
def createExcelFile():

    conn,cursor = connectToDatabase()

    # Define your SQL query
    query = f"""
    SELECT vehicle_id, tx_time, x_pos, y_pos, gps_lon, gps_lat, speed, road_id, 
    lane_id, displacement, turn_angle, acceleration, fuel_consumption, 
    co2_consumption, deceleration, storage_time, 
    EXTRACT(EPOCH FROM (storage_time - tx_time)) AS time_difference_seconds
    FROM {database_table};
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


if __name__ == "__main__":
    queue = Queue()

    ws_process = Process(target=websocketServerProcess, args=(queue,))
    db_process = Process(target=storeInDatabase, args=(queue,))
    

    db_process.start()
    ws_process.start()

    ws_process.join()
    
    # Once the consumer process is done, send a "STOP" message to the db_inserter process
    queue.put("STOP")
    db_process.join()

    createExcelFile()
    