import asyncio
import websockets
from multiprocessing import Process, Queue
import aioredis
import pandas as pd
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor

db_batch_size = 100

async def writeBatchToRedis(redis, messages_batch):
    #Write messages batch to Redis
    pipeline = redis.pipeline()  # Use pipeline for atomic batch write
    
    # Add individual SET commands to the pipeline for each message
    for msg_id, message in enumerate(messages_batch, start=1):
        # Get the current datetime
        now = datetime.now()
        formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        message_with_timestamp = f"{message},{formatted_date_time}"
        
        # Append message to Redis pipeline
        pipeline.set(f'message:{msg_id}', message_with_timestamp)
    
    # Execute the pipeline (batch write)
    await pipeline.execute()
    
async def dbBatchWriter(queue):
    redis = None
    
    try:    
        # Connect to Redis using the new aioredis 2.0 syntax
        redis = await aioredis.from_url('redis://localhost', encoding="utf-8", decode_responses=True)
    
        messages_batch = []  # List to accumulate messages for batch write
        
        while True:
            message = await queue.get()
            if message == "STOP":
                print("Received STOP, shutting down db_writer.")
                break
            
            # Append message to the batch
            messages_batch.append(message)
            
            # Check if batch size is reached
            if len(messages_batch) >= db_batch_size:
                await writeBatchToRedis(redis, messages_batch)
                messages_batch = []  # Reset batch
            
        # Write any remaining messages in the last batch
        if messages_batch:
            await writeBatchToRedis(redis, messages_batch)
      
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close Redis connection explicitly
        if redis is not None:
            await redis.close()      


    
async def dbWriter(queue):
    
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
      
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close Redis connection explicitly
        if 'redis' in locals():
            await redis.close()      

def dbWriterProcess(queue):
    asyncio.run(dbWriter(queue))

def dbBatchWriterProcess(queue):
    asyncio.run(dbBatchWriter(queue))


async def websocketServerHandler(websocket, path, queue):
    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=40)
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


async def runWebsocketServer(queue):
    server = await websockets.serve(lambda ws, path: websocketServerHandler(ws, path, queue), "0.0.0.0", 8765)
    print("Listening for incoming websocket connections...")

    try:
        await asyncio.wait_for(asyncio.Future(), timeout=100)
    except asyncio.TimeoutError:
        print("Server timeout reached, shutting down.")
    finally:
        server.close()
        await server.wait_closed()

def websocketServerProcess(queue):
    asyncio.run(runWebsocketServer(queue))
 


async def fetchDataFromRedis():
    redis = await aioredis.from_url("redis://localhost", encoding="utf-8", decode_responses=True)
    
    # Example: Fetching multiple keys. Adjust based on how you've structured your data.
    keys = await redis.keys('message:*')
    data = []
    for key in keys:
        message = await redis.get(key)
        data.append({'Key': key, 'Message': message})

    return data

def extractFromDatabase():
    

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
    
    queue = Queue()

    db_process = Process(target=dbWriterProcess, args=(queue,))
    ws_process = Process(target=websocketServerProcess, args=(queue,))

    db_process.start()
    ws_process.start()

    ws_process.join()
    db_process.join()
    ws_process.terminate()
    
    extractFromDatabase()