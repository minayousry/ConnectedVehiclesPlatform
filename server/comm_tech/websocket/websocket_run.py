import asyncio
import websockets
from datetime import datetime
import psycopg2


websocket_port = 8765


def getcurrentTimestamp():
    now = datetime.now()
    formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    return formatted_date_time



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
