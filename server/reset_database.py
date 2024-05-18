    
import argparse

import Kafka_GreenPlum.greenplum_create_db as gp_db
import mqtt_Influx.influx_create_db as influx_db
import qpid_cassandra.cassandra_create_db as cassandra_db
import webSockets_Postgresql.postgresql_create_db as postgresql_db
import webSockets_Redis.redis_create_db as redis_db
import asyncio
import configurations as cfg
   
if __name__ == '__main__':
    
    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add a string argument
    parser.add_argument('database_technology', type=str, help='select which database tech to use for the server.')

    # Parse the arguments
    args = parser.parse_args()

    # Access the parsed argument
    database_technology = args.database_technology
    
    if database_technology == "greenplum":
        database_create_func = gp_db.createDatabase
    elif database_technology == "influx":
        database_create_func = influx_db.createDatabase
    elif database_technology == "cassandra":
        database_create_func = cassandra_db.createDatabase
    elif database_technology == "postgresql":
        database_create_func = postgresql_db.createDatabase  
    elif database_technology == "redis":
        database_create_func = redis_db.createDatabase
    else:
        print("Invalid server technology. Please select one of the following: influx, greenplum, cassandra, postgresql or redis")
        exit(1)
 
    try:
        if database_technology == "redis":
          result = asyncio.run(database_create_func(cfg.use_database_timestamp))
        else: 
            result = database_create_func(cfg.use_database_timestamp)
            
        if result:
            print("Database created successfully.")
        else:
            print("Failed to create database.")
            exit(1)
    except Exception as e:
        print(f"Failed to create database brcause of: {e}")
        exit(1)


