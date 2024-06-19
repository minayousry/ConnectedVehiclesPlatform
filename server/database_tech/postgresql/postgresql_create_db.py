import psycopg2
from uuid import uuid4

#postpresql configurations
default_database_name = 'postgres'
database_name = "obd2_content_database"
table_name = "obd2_data_table"
server_address = '127.0.0.1'
port = 5432
username = 'postgres'
password = "guest"
new_username = 'guest'


def createTable(cursor,use_database_timestamp):

    if use_database_timestamp:
        
        sql_creation_query = f"""
                            CREATE TABLE IF NOT EXISTS {table_name} (
                            id SERIAL PRIMARY KEY,
                            vehicle_id TEXT,
                            tx_time TIMESTAMP WITHOUT TIME ZONE,
                            x_pos DOUBLE PRECISION,
                            y_pos DOUBLE PRECISION,
                            gps_lon DOUBLE PRECISION,
                            gps_lat DOUBLE PRECISION,
                            speed DOUBLE PRECISION,
                            road_id TEXT,
                            lane_id TEXT,
                            displacement DOUBLE PRECISION,
                            turn_angle DOUBLE PRECISION,
                            acceleration DOUBLE PRECISION,
                            fuel_consumption DOUBLE PRECISION,
                            co2_consumption DOUBLE PRECISION,
                            deceleration DOUBLE PRECISION,
                            rx_time TIMESTAMP WITHOUT TIME ZONE,
                            storage_time TIMESTAMP WITHOUT TIME ZONE """
                            
        sql_creation_query += "DEFAULT CURRENT_TIMESTAMP"
        
    else:
        sql_creation_query = f"""
                            CREATE TABLE IF NOT EXISTS {table_name} (
                            id SERIAL PRIMARY KEY,
                            vehicle_id TEXT,
                            tx_time TEXT,
                            x_pos DOUBLE PRECISION,
                            y_pos DOUBLE PRECISION,
                            gps_lon DOUBLE PRECISION,
                            gps_lat DOUBLE PRECISION,
                            speed DOUBLE PRECISION,
                            road_id TEXT,
                            lane_id TEXT,
                            displacement DOUBLE PRECISION,
                            turn_angle DOUBLE PRECISION,
                            acceleration DOUBLE PRECISION,
                            fuel_consumption DOUBLE PRECISION,
                            co2_consumption DOUBLE PRECISION,
                            deceleration DOUBLE PRECISION,
                            rx_time TEXT,
                            storage_time TEXT """
                            
    sql_creation_query += ");"
    
    cursor.execute(sql_creation_query)


def clearTable(conn,cursor):
    try:

        # SQL command to TRUNCATE table
        truncate_command = f"TRUNCATE TABLE {table_name};"
        
        # Execute the command
        cursor.execute(truncate_command)
        
        # Commit the transaction
        conn.commit()
        
        print(f"Table {table_name} has been cleared.")
        
    except Exception as e:
        print(f"An error occurred: {e}")

def dropTableIfExists(conn,cursor):
    try:
        drop_statement = f"DROP TABLE IF EXISTS {table_name};"
        
        # Execute the DROP TABLE statement
        cursor.execute(drop_statement)
        
        # Commit the transaction
        conn.commit()
        
        print(f"Table {table_name} dropped successfully (if it existed).")
    except Exception as e:
        print(f"An error occurred while dropping the table: {e}")
        
        

        
def createDatabase(use_database_timestamp):
    
    connection = psycopg2.connect(host=server_address, port=port, user=username, password=password,dbname=default_database_name)
    
    cursor = connection.cursor()
    
    database_exist = False
    
    result = True
    
    try:
        # Check if the database exists
        cursor.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = %s", (database_name,))
        exists = cursor.fetchone()
        
        if not exists:
            # Create the database
            cursor.execute(f"CREATE DATABASE {database_name}")
            print(f"Database '{database_name}' created.")
        else:
            database_exist = True
            print(f"Database '{database_name}' already exists.")
        
        cursor.close()
        connection.close()
        
        
        
        connection = psycopg2.connect(host=server_address, port=port, user=username, password=password,dbname=database_name)
        cursor = connection.cursor()
        
        
        dropTableIfExists(connection,cursor)     
        createTable(cursor,use_database_timestamp)
        clearTable(connection,cursor)
        
        # Commit the transaction
        connection.commit()
        
        # Modify session settings
        connection.autocommit = False
        
        
        if database_exist:
            # Create new user
            #cursor.execute(f"CREATE USER IF NOT EXISTS {new_username} WITH PASSWORD '{password}'")
            #print(f"User '{new_username}' created.")

            # Grant privileges to the new user on the specific database
            cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {database_name} TO {new_username}")
            print(f"Privileges granted to user '{new_username}' on database '{database_name}'.")
        
            # Grant privileges to the new user on all tables included in the database
            cursor.execute(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {new_username}")
            print(f"Privileges granted to user '{new_username}' on all tables of database '{database_name}'.")
            
            cursor.execute(f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {new_username}")
            print(f"Privileges granted to user '{new_username}' on all sequences of database '{database_name}'.")
            
            # Commit the transaction
            connection.commit()
        
    except psycopg2.Error as e:
        result = False
        print("Error:", e)
    
    finally:
        # Close cursor and connection
        cursor.close()
        connection.close()
        
    return result

if __name__ == "__main__":
    use_database_timestamp = True
    createDatabase(use_database_timestamp)
    

