import psycopg2
from psycopg2 import sql
import psycopg2.extras

# Configuration for connecting to your Kafka server
kafka_server = '127.0.0.1:9092'  # this to the Kafka server address
topic_name = 'OBD2_data'
consumer_timeout_in_ms = 500
received_msgs = []
time_diff_list = []

# Configuration for GreenPlum Database
user = 'mina_yousry_iti'
password = 'my_psw'
host = 'localhost'
port = '5432'  # Default port for Greenplum and PostgreSQL
default_dbname = "postgres"
dbname = "OBD2_Data_Fleet_database"
table_name = "OBD2_table"
db_batch_size = 100

def createTable(cursor,use_database_timestamp):

    print("Create Table")
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
                            
    if use_database_timestamp:
        sql_creation_query += "DEFAULT CURRENT_TIMESTAMP"
        
    sql_creation_query += ");"
    
    cursor.execute(sql_creation_query)


def connectToDatabase():
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = False  
    cursor = conn.cursor()

    return conn,cursor


def CreateDatabaseifNotExists():
    
    # Connect to Greenplum
    conn = psycopg2.connect(dbname=default_dbname, user=user, password=password, host=host)
    conn.autocommit = True  # Needed to create a database
    cursor = conn.cursor()

    # Check if the database exists
    cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"), (dbname,))
    result = cursor.fetchone() is not None

    if not result:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))

    # Close connection
    cursor.close()
    conn.close()

    return result




def closeDatabaseConnection(cursor,conn):
    # Close the cursor and connection
    cursor.close()
    conn.close()


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


def createDatabase(use_database_timestamp):
    CreateDatabaseifNotExists()
    conn,cursor = connectToDatabase()

    # Turn autocommit off for batching
    conn.autocommit = False
   
    try:     
        createTable(cursor,use_database_timestamp)
        clearTable(conn,cursor)
        closeDatabaseConnection(cursor,conn)
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


if __name__ == "__main__":
    use_database_timestamp = True
    createDatabase(use_database_timestamp)
    
    
