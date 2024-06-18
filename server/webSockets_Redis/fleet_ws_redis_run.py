import asyncio
import websockets
import multiprocessing
import aioredis
import pandas as pd
from datetime import datetime,timezone
import time
from concurrent.futures import ThreadPoolExecutor

websocket_port_number = 8765


websocket_port = 8765

db_batch_size = 100



def stringToFloatTimestamp(timestamp_str, format='%Y-%m-%d %H:%M:%S.%f'):
    dt = datetime.strptime(timestamp_str, format)
    float_timestamp = dt.replace(tzinfo=timezone.utc).timestamp()
    return float_timestamp
        

def getcurrentTimestamp():
    now = datetime.now()
    formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    return formatted_date_time


async def writeBatchToRedis(redis, messages_batch,storage_timestamp):
    
    batch_size = len(messages_batch)
    starting_msg_id = await redis.incrby('message_id', batch_size) - batch_size + 1
    

    async with redis.pipeline() as pipeline:
        for i, message in enumerate(messages_batch):
            msg_id = starting_msg_id + i
            
            message_with_timestamp = f"{message},{storage_timestamp}"
        
            # Append message to Redis pipeline
            pipeline.set(f'message:{msg_id}', message_with_timestamp)
    
        # Execute the pipeline (batch write)
        await pipeline.execute()
    
async def dbBatchWriter(queue,last_storage_timestamp_obj):
    redis = None
    inserted_msg_count = 0
    
    
    last_storage_timestamp = "NONE"
    
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
                await writeBatchToRedis(redis, messages_batch,last_storage_timestamp)
                last_storage_timestamp = getcurrentTimestamp()
                messages_batch = []  # Reset batch
            
        # Write any remaining messages in the last batch
        if len(messages_batch) > 0:
            await writeBatchToRedis(redis, messages_batch,last_storage_timestamp)
            last_storage_timestamp = getcurrentTimestamp()
    except Exception as e:
        print(f"An error occurred: {e} in {message}")

    finally:
        with last_storage_timestamp_obj.get_lock():
            last_storage_timestamp_obj.value = stringToFloatTimestamp(last_storage_timestamp)
        
        # Close Redis connection explicitly
        if redis is not None:
            await redis.close()      


    
async def dbWriter(queue,last_storage_timestamp_obj):
    
    redis = None
    inserted_msg_count = 0
    
    last_storage_timestamp = "NONE"
    
    try:    
        # Connect to Redis using the new aioredis 2.0 syntax
        redis = await aioredis.from_url('redis://localhost', encoding="utf-8", decode_responses=True)
    
        while True:
            message = queue.get()
            if message == "STOP":
                print("Received STOP, shutting down db_writer.")
                break
            
            msg_id = await redis.incr('message_id')
            
            message_with_timestamp = f"{message},{last_storage_timestamp}"
            await redis.set(f'message:{msg_id}', message_with_timestamp)
            
            last_storage_timestamp = getcurrentTimestamp()
            inserted_msg_count += 1
      
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        with last_storage_timestamp_obj.get_lock():
            last_storage_timestamp_obj.value = stringToFloatTimestamp(last_storage_timestamp)
        
        # Close Redis connection explicitly
        if 'redis' in locals():
            await redis.close()      

def dbWriterProcess(queue,last_storage_timestamp_obj,use_database_timestamp):
    asyncio.run(dbWriter(queue,last_storage_timestamp_obj))

def dbBatchWriterProcess(queue,last_storage_timestamp_obj,use_database_timestamp):

    asyncio.run(dbBatchWriter(queue,last_storage_timestamp_obj))


async def websocketServerHandler(websocket, path, queue,no_of_received_msgs_obj,no_of_sent_msgs_obj,stop_event):
    received_msg_count = 0
    try:
        
        while True:
            message = await websocket.recv()
            current_timestamp = getcurrentTimestamp()
            record = eval(message) 
            if record[0] != "STOP":
                record.append(current_timestamp)
                queue.put(record)
                received_msg_count += 1
            else:
                print("Received all messages")
                with no_of_sent_msgs_obj.get_lock():
                    no_of_sent_msgs_obj.value = record[1]
                break
                   
    except websockets.ConnectionClosed:
        print("WebSocket connection closed.")
    except asyncio.TimeoutError:
        print("Timeout, sent STOP signal to db_writer.")
    finally:
        queue.put("STOP")
        
        with no_of_received_msgs_obj.get_lock():
            no_of_received_msgs_obj.value = received_msg_count
        stop_event.set()
        await websocket.close()


async def runWebsocketServer(queue,no_of_received_msgs_obj,no_of_sent_msgs_obj):
    
    stop_event = asyncio.Event()
    server = await websockets.serve(lambda ws, path: websocketServerHandler(ws, path, queue,no_of_received_msgs_obj,no_of_sent_msgs_obj,stop_event), "0.0.0.0", websocket_port)
    print("Listening for incoming websocket connections...")

    try:
        await stop_event.wait()  # Wait until stop_event is set
    finally:
        server.close()
        await server.wait_closed()

def websocketServerProcess(queue,no_of_received_msgs_obj,no_of_sent_msgs_obj):
    asyncio.run(runWebsocketServer(queue,no_of_received_msgs_obj,no_of_sent_msgs_obj))




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
        elm_list = elm['Message'].replace("[", "").replace("]", "").replace("'", "").strip().split(',')
        data_list.append(elm_list)
        
        
    cols_names = ['vehicle_id', 'tx_time', 'x_pos', 'y_pos', 'gps_lon', 'gps_lat',
                  'Speed', 'RoadID', 'LaneId', 'Displacement', 'TurnAngle', 'Acceleration',
                  'FuelConsumption', 'Co2Consumption', 'Deceleration','rx_time','storage_time']
    
    # Convert the data to a pandas DataFrame.
    df = pd.DataFrame(data_list,columns=cols_names)
    
    df["rx_time"] = df['rx_time'].str.strip()
    df["tx_time"] = df['tx_time'].str.strip()
    df["storage_time"] = df['storage_time'].str.strip()
    
    #print(df["tx_time"])
    
    
    return df,db_batch_size
    

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