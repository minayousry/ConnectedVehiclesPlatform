import argparse
import asyncio
import subprocess


#import utilities
import server_utilities as server_utilities


import database_tech.greenplum.greenplum_create_db as greenplum
import database_tech.influx.influx_create_db as influx
import database_tech.cassandra.cassandra_create_db as cassandra
import database_tech.postgresql.postgresql_create_db as postgresql
import database_tech.redis.redis_create_db as redis



import configurations as cfg



def initServers(bash_comm_script_path,bash_db_script_path):
    
    result = subprocess.run(f". {bash_comm_script_path}",shell=True,check=True)

    # Check if the script ran successfully
    if result.returncode == 0:
        print("Comm Bash script executed successfully")
        result = subprocess.run(f". {bash_db_script_path}",shell=True,check=True)
        if result.returncode == 0:
            print("database Bash script executed successfully")    
            result = True
        else:
            result = False
    else:
        print("Error executing Bash script")
        print("Error message:")
        print(result.stderr)
        result = False
        
    return result


def setKafkaIpAddress(file_path,search_text,new_text):
    lines = []
    found_line = None
    print(file_path)
    
    file1 = open(file_path, 'r+')
    lines = file1.readlines()
    file_content = ''.join(lines)  
        
    for line in lines:
        if line.find(search_text) != -1: 
            found_line = line.strip()
            break
        
    if found_line is not None:
        modified_content = file_content.replace(found_line, new_text)

        file1.seek(0)
        file1.write(modified_content)
    file1.close()



   
if __name__ == '__main__':
    
    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add a string argument
    parser.add_argument('serevr_technology', type=str, help='select which technology to use for the server.')

    # Parse the arguments
    args = parser.parse_args()

    # Access the parsed argument
    server_tech = args.serevr_technology
    

    bash_script_path = None
    database_create_func = None
    comm_process = None
    database_process = None
    database_extract_func = None

    
    bash_comm_script_path = "./comm_tech/"
    bash_db_script_path = "./database_tech/"
    
    techs = server_tech.split("_")
    
    bash_comm_script_path += (techs[0] + "/"+techs[0] + "_run_server.sh")
    bash_db_script_path += (techs[1] + "/"+techs[1] + "_run_server.sh")
    
    
    if server_tech == "kafka_greenplum":
        database_create_func = greenplum.createDatabase
        kafka_cfg_path = "/home/mina_yousry_iti/kafka/config/server.properties"
        text_to_search = "advertised.listeners=PLAINTEXT:"
        kafka_port_num = "9092"
        kafka_server_address = server_utilities.getExternalIp()
        
        if kafka_server_address is None:
            print("Failed to retrieve IP address.")
            exit(1)
        else:
            new_kafka_server = text_to_search + """//"""+str(kafka_server_address)+":"+kafka_port_num
        
            server_utilities.setFileMode(kafka_cfg_path, 'r')
            setKafkaIpAddress(kafka_cfg_path,text_to_search,new_kafka_server)
        
    elif server_tech == "mqtt_influx":
        database_create_func = influx.createDatabase
    elif server_tech == "qpid_cassandra":
        database_create_func = cassandra.createDatabase
    elif server_tech == "websocket_postgresql":
        database_create_func = postgresql.createDatabase  
    elif server_tech == "websocket_redis":
        database_create_func = redis.createDatabase
    else:
        print("Invalid server technology. Please select one of the following: mqtt_influx, kafka_greenplum, qpid_cassandra, websocket_postgresql or websocket_redis")
        exit(1)
 
    try:
        result = initServers(bash_comm_script_path,bash_db_script_path)
         
        if result:
            print("Servers are running.")
            if(server_tech == "websocket_redis"):
                result = asyncio.run(database_create_func(cfg.use_database_timestamp))
            else:
                result = database_create_func(cfg.use_database_timestamp)
        else:
            print("Failed to run servers.")
            exit(1)
            
        
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)


