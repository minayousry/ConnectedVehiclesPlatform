from influxdb import InfluxDBClient
from uuid import uuid4

database_name = "obd2_database"
measurement_name = "obd2_data"
server_address = '127.0.0.1'
port = 8086

def resetDatabase(client):
    
    dblist = client.get_list_database()
    
    #check if database_name exist
    if any(db['name'] == database_name for db in dblist): 
        print("Dropping database...")
        client.drop_database(database_name)
    
    client.create_database(database_name)
    client.switch_database(database_name)
    print(f"Database '{database_name}' created.")

def createDatabase(use_database_timestamp):
    try:
        client = InfluxDBClient(server_address, port)
        print(f"Connected to InfluxDB server at {server_address}:{port}")
        resetDatabase(client)
        print("Closing connection...")
        client.close()
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    
if __name__ == "__main__":
    print("Creating database...")
    use_database_timestamp = True
    createDatabase(use_database_timestamp)
