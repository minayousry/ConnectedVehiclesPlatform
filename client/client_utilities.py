import pytz
import datetime

kafka_sent_msg_count = 0
mqtt_sent_msg_count = 0
qpid_sent_msg_count = 0
ws_sent_msg_count = 0

kafka_start_sim_time = None
mqtt_start_sim_time = None
qpid_start_sim_time = None
ws_start_sim_time  = None

kafka_end_sim_time = None
mqtt_end_sim_time = None
qpid_end_sim_time = None
ws_end_sim_time  = None


def resetMsgCount(client_name):
    global kafka_sent_msg_count
    global mqtt_sent_msg_count
    global qpid_sent_msg_count
    global ws_sent_msg_count

    if client_name == "kafka":
        kafka_sent_msg_count = 0
    elif client_name == "mqtt":
        mqtt_sent_msg_count = 0
    elif client_name == "qpid":
        qpid_sent_msg_count = 0
    elif client_name == "ws":
        ws_sent_msg_count = 0
    else:
        print("Invalid client name.")
        exit(1)
        
def recordStartSimTime(client_name):
    global kafka_start_sim_time
    global mqtt_start_sim_time
    global qpid_start_sim_time
    global ws_start_sim_time

    if client_name == "kafka":
        kafka_start_sim_time = getdatetime()
    elif client_name == "mqtt":
        mqtt_start_sim_time = getdatetime()
    elif client_name == "qpid":
        qpid_start_sim_time = getdatetime()
    elif client_name == "ws":
        ws_start_sim_time = getdatetime()
    else:
        print("Invalid client name.")
        exit(1)

def recordEndSimTime(client_name):
    global kafka_end_sim_time
    global mqtt_end_sim_time
    global qpid_end_sim_time
    global ws_end_sim_time
    
    if client_name == "kafka":
        kafka_end_sim_time = getdatetime()
    elif client_name == "mqtt":
        mqtt_end_sim_time = getdatetime()
    elif client_name == "qpid":
        qpid_end_sim_time = getdatetime()
    elif client_name == "ws":
        ws_end_sim_time = getdatetime()
    else:
        print("Invalid client name.")
        exit(1)
    
def calculateSimDuration(client_name):
    global kafka_start_sim_time
    global mqtt_start_sim_time
    global qpid_start_sim_time
    global ws_start_sim_time

    global kafka_end_sim_time
    global mqtt_end_sim_time
    global qpid_end_sim_time
    global ws_end_sim_time

    if client_name == "kafka":
        start_time = kafka_start_sim_time
        end_time = kafka_end_sim_time
    elif client_name == "mqtt":
        start_time = mqtt_start_sim_time
        end_time = mqtt_end_sim_time
    elif client_name == "qpid":
        start_time = qpid_start_sim_time
        end_time = qpid_end_sim_time
    elif client_name == "ws":
        start_time = ws_start_sim_time
        end_time = ws_end_sim_time
    else:
        print("Invalid client name.")
        exit(1)
    
    if start_time is None or end_time is None:
        print("Simulation start or end time is not recorded.")
        return None
        
    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f")
    end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S.%f")
    
    #calculate simulation duration in seconds
    sim_duration = end_time - start_time
    
    return sim_duration.total_seconds()

   
def increaseMsgCount(client_name):
    global kafka_sent_msg_count
    global mqtt_sent_msg_count
    global qpid_sent_msg_count
    global ws_sent_msg_count

    if client_name == "kafka":
        kafka_sent_msg_count += 1
    elif client_name == "mqtt":
        mqtt_sent_msg_count += 1
    elif client_name == "qpid":
        qpid_sent_msg_count += 1
    elif client_name == "ws":
        ws_sent_msg_count += 1
    else:
        print("Invalid client name.")
        exit(1)
    
def getMsgCount(client_name):
    global kafka_sent_msg_count
    global mqtt_sent_msg_count
    global qpid_sent_msg_count
    global ws_sent_msg_count

    if client_name == "kafka":
        return kafka_sent_msg_count
    elif client_name == "mqtt":
        return mqtt_sent_msg_count
    elif client_name == "qpid":
        return qpid_sent_msg_count
    elif client_name == "ws":
        return ws_sent_msg_count
    else:
        return 0

def getdatetime():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    currentDT = utc_now.astimezone(pytz.timezone("Atlantic/Reykjavik"))
    DATIME = currentDT.strftime("%Y-%m-%d %H:%M:%S.%f")
    return DATIME