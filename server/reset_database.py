import argparse

import database_tech.greenplum.greenplum_create_db as greenplum
import database_tech.influx.influx_create_db as influx
import database_tech.cassandra.cassandra_create_db as cassandra
import database_tech.postgresql.postgresql_create_db as postgresql
import database_tech.redis.redis_create_db as redis


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
        database_create_func = greenplum.createDatabase
    elif database_technology == "influx":
        database_create_func = influx.createDatabase
    elif database_technology == "cassandra":
        database_create_func = cassandra.createDatabase
    elif database_technology == "postgresql":
        database_create_func = postgresql.createDatabase  
    elif database_technology == "redis":
        database_create_func = redis.createDatabase
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


