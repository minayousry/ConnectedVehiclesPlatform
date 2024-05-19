import asyncio
import websockets
import multiprocessing
import aioredis
import pandas as pd
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor

websocket_port_number = 8765
db_batch_size = 100

websocket_port = 8765

async def writeBatchToRedis(redis, messages_batch):
    
    batch_size = len(messages_batch)
    starting_msg_id = await redis.incrby('message_id', batch_size) - batch_size + 1
    

    async with redis.pipeline() as pipeline:
        for i, message in enumerate(messages_batch):
            msg_id = starting_msg_id + i
            
            # Get the current datetime
            now = datetime.now()
            formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
            message_with_timestamp = f"{message},{formatted_date_time}"
        
            # Append message to Redis pipeline
            pipeline.set(f'message:{msg_id}', message_with_timestamp)
    
        # Execute the pipeline (batch write)
        await pipeline.execute()
    
async def dbBatchWriter(queue,no_of_inserted_msgs_obj):
    redis = None
    
    try:    
        # Connect to Redis using the new aioredis 2.0 syntax
        redis = await aioredis.from_url('redis://localhost', encoding="utf-8", decode_responses=True)
    
        messages_batch = []  # List to accumulate messages for batch write
        
        while True:
            message = queue.get()
            if message == "STOP":
                print("Received STOP, shutting down db_writer.")
                break
            
            # Append message to the batch
            messages_batch.append(message)
            
            # Check if batch size is reached
            if len(messages_batch) >= db_batch_size:
                await writeBatchToRedis(redis, messages_batch)
                with no_of_inserted_msgs_obj.get_lock():
                    no_of_inserted_msgs_obj.value += db_batch_size
                messages_batch = []  # Reset batch
            
        # Write any remaining messages in the last batch
        if len(messages_batch) > 0:
            await writeBatchToRedis(redis, messages_batch)
            with no_of_inserted_msgs_obj.get_lock():
                no_of_inserted_msgs_obj.value += len(messages_batch)
    except Exception as e:
        print(f"An error occurred: {e} in {message}")

    finally:
        # Close Redis connection explicitly
        if redis is not None:
            await redis.close()      


    
async def dbWriter(queue,no_of_inserted_msgs_obj):
    
    redis = None
    
    try:    
        # Connect to Redis using the new aioredis 2.0 syntax
        redis = await aioredis.from_url('redis://localhost', encoding="utf-8", decode_responses=True)
    
        while True:
            message = queue.get()
            if message == "STOP":
                print("Received STOP, shutting down db_writer.")
                break
            
            msg_id = await redis.incr('message_id')
        
            # Get the current datetime
            now = datetime.now()
            
            # Format as day and time
            formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
            message_with_timestamp = f"{message},{formatted_date_time}"
            await redis.set(f'message:{msg_id}', message_with_timestamp)
            with no_of_inserted_msgs_obj.get_lock():
                no_of_inserted_msgs_obj.value += 1
      
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close Redis connection explicitly
        if 'redis' in locals():
            await redis.close()      

def dbWriterProcess(queue,no_of_inserted_msgs_obj,use_database_timestamp):
    asyncio.run(dbWriter(queue,no_of_inserted_msgs_obj))

def dbBatchWriterProcess(queue,no_of_inserted_msgs_obj,use_database_timestamp):
    asyncio.run(dbBatchWriter(queue,no_of_inserted_msgs_obj))

async def websocketServerHandler(websocket, path, queue,no_of_received_msgs_obj):
    try:
        while True:
            message = await websocket.recv()
                 
            queue.put(message)
            with no_of_received_msgs_obj.get_lock():
                no_of_received_msgs_obj.value += 1
                    
    except websockets.ConnectionClosed:
        print("WebSocket connection closed.")
    except asyncio.TimeoutError:
        print("Timeout, sent STOP signal to db_writer.")
    finally:
        queue.put("STOP")
        await websocket.close()

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

async def fetchDataFromRedis():
    redis = await aioredis.from_url("redis://localhost", encoding="utf-8", decode_responses=True)
    
    # Example: Fetching multiple keys. Adjust based on how you've structured your data.
    keys = await redis.keys('message:*')
    data = []
    for key in keys:
        message = await redis.get(key)
        data.append({'Key': key, 'Message': message})

    return data

def extractFromDatabase(use_database_timestamp):
    

    data = asyncio.run(fetchDataFromRedis())
    
    data_list = []
    
    for elm in data:
        elm_list = elm['Message'].replace("[", "").replace("]", "").replace("\"", "").strip().split(',')
        data_list.append(elm_list)
        
        
    cols_names = ['VehicleId', 'tx_time', 'x_pos', 'y_pos', 'gps_lon', 'gps_lat',
                  'Speed', 'RoadID', 'LaneId', 'Displacement', 'TurnAngle', 'Acceleration',
                  'FuelConsumption', 'Co2Consumption', 'Deceleration','storage_time']
    
    # Convert the data to a pandas DataFrame.
    df = pd.DataFrame(data_list,columns=cols_names)
    
    df["tx_time"] = df['tx_time'].str.strip()
    df["storage_time"] = df['storage_time'].str.strip()
    
    return df
    

if __name__ == "__main__":
    
    queue = multiprocessing.Queue()
    no_of_received_msgs_obj = multiprocessing.Value('i', 0)
    no_of_inserted_msgs_obj = multiprocessing.Value('i', 0)

    use_database_timestamp = True
    ws_process = multiprocessing.Process(target=websocketServerProcess, args=(queue,no_of_received_msgs_obj))
    db_process = multiprocessing.Process(target=dbWriterProcess, args=(queue,no_of_inserted_msgs_obj,use_database_timestamp))
    

    db_process.start()
    ws_process.start()

    ws_process.join()
    db_process.join()
    ws_process.terminate()
    
    extractFromDatabase()