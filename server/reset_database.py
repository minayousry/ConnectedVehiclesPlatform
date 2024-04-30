    
import argparse

import Kafka_GreenPlum.greenplum_create_db as gp_db
import mqtt_Influx.influx_create_db as influx_db
import qpid_cassandra.cassandra_create_db as cassandra_db
import webSockets_Postgresql.postgresql_create_db as postgresql_db
import webSockets_Redis.redis_create_db as redis_db

   
if __name__ == '__main__':
    
    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add a string argument
    parser.add_argument('serevr_technology', type=str, help='select which technology to use for the server.')

    # Parse the arguments
    args = parser.parse_args()

    # Access the parsed argument
    server_tech = args.serevr_technology
    
    if server_tech == "kafka_greenplum":
        database_create_func = gp_db.createDatabase
    elif server_tech == "mqtt_influx":
        database_create_func = influx_db.createDatabase
    elif server_tech == "qpid_cassandra":
        database_create_func = cassandra_db.createDatabase
    elif server_tech == "websocket_postgresql":
        database_create_func = postgresql_db.createDatabase  
    elif server_tech == "websocket_redis":
        database_create_func = redis_db.createDatabase
    else:
        print("Invalid server technology. Please select one of the following: mqtt_influx, kafka_greenplum, qpid_cassandra, websocket_postgresql ot websocket_redis")
        exit(1)
 
    try:
        result = database_create_func()
        if result:
            print("Database created successfully.")
        else:
            print("Failed to create database.")
            exit(1)
    except Exception as e:
        print(f"Failed to create database brcause of: {e}")
        exit(1)


