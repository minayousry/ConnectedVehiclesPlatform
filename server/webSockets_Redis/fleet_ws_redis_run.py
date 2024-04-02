import asyncio
import websockets
from multiprocessing import Process, Queue
import aioredis
import pandas as pd
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor

async def db_writer(queue):
    
    redis = None
        
    # Connect to Redis using the new aioredis 2.0 syntax
    redis = await aioredis.from_url('redis://localhost', encoding="utf-8", decode_responses=True)
    await redis.flushdb()
    
    while True:
        message = queue.get()
        if message == "STOP":
            print("Received STOP, shutting down db_writer.")
            await redis.close()
            break
            
        msg_id = await redis.incr('message_id')
        
        # Get the current datetime
        now = datetime.now()
            
        # Format as day and time
        formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S")
            
        message+=","
        message+=formatted_date_time
        
        await redis.set(f'message:{msg_id}', message)
        #print(f"Stored message #{msg_id} in Redis.")
      
    # Close Redis connection explicitly if necessary
    await redis.close()
        

    

      

def db_writer_process(queue):
    asyncio.run(db_writer(queue))


async def websocket_server_handler(websocket, path, queue):
    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=100)
                # Here, you would typically handle the message, such as by echoing it back
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
    
    """    
    async for message in websocket:
        #print(f"Received message: {message}")
        queue.put(message)
    """ 


async def run_websocket_server(queue):
    server = await websockets.serve(lambda ws, path: websocket_server_handler(ws, path, queue), "0.0.0.0", 8765)
    
    try:
        await asyncio.wait_for(asyncio.Future(), timeout=20)
    except asyncio.TimeoutError:
        print("Server timeout reached, shutting down.")
    finally:
        server.close()
        await server.wait_closed()
        
def websocket_server_process(queue):
    asyncio.run(run_websocket_server(queue))


async def fetch_data_from_redis():
    redis = await aioredis.from_url("redis://localhost", encoding="utf-8", decode_responses=True)
    
    # Example: Fetching multiple keys. Adjust based on how you've structured your data.
    keys = await redis.keys('message:*')
    data = []
    for key in keys:
        message = await redis.get(key)
        data.append({'Key': key, 'Message': message})

    return data

def createExcelFile():
    
    print("creating excel sheet")

    data = asyncio.run(fetch_data_from_redis())
    
    
    
    data_list = []
    
    for elm in data:
        elm_list = elm['Message'].replace("[", "").replace("]", "").replace("\"", "").split(',')
        tx_time = elm_list[1].strip()
        storage_time = elm_list[-1].strip()
        
        date_object_tx_time = datetime.strptime(tx_time, '%Y-%m-%d %H:%M:%S')
        date_object_storage_time = datetime.strptime(storage_time, '%Y-%m-%d %H:%M:%S')
        date_object_time_diff = date_object_storage_time - date_object_tx_time
        
        days = date_object_time_diff.days
        seconds = date_object_time_diff.seconds
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Constructing a formatted string (not using %Y-%m-%d as it's not applicable to durations)
        time_diff_str = f"{days} days, {hours:02}:{minutes:02}:{seconds:02}"

        
        elm_list.append(time_diff_str)
        data_list.append(elm_list)
        
        
    cols_names = ['VehicleId', 'Tx_DateTime', 'x_pos', 'y_pos', 'gps_lon', 'gps_lat',
                  'Speed', 'RoadID', 'LaneId', 'Displacement', 'TurnAngle', 'Acceleration',
                  'FuelConsumption', 'Co2Consumption', 'Deceleration','storage_time','time_difference_in_sec']
    
    
    # Convert the data to a pandas DataFrame.
    df = pd.DataFrame(data_list,columns=cols_names)
    
    # Write the DataFrame to an Excel file.
    df.to_excel('obd2_data_report.xlsx', index=False)
    

if __name__ == "__main__":
    
    createExcelFile()
    """ 
    queue = Queue()

    db_process = Process(target=db_writer_process, args=(queue,))
    ws_process = Process(target=websocket_server_process, args=(queue,))

    db_process.start()
    ws_process.start()

    print("Here 1")
    ws_process.join()
    print("Here 2")
    #queue.put("STOP")  # Signal the DB writer to stop
    print("Here 3")
    db_process.join()
    print("Here 4")
    ws_process.terminate()
    print("Here 5")
    createExcelFile()
    """