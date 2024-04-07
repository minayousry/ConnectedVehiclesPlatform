from influxdb import InfluxDBClient
from uuid import uuid4

database_name = "obd2_database"
measurement_name = "obd2_data"
server_address = '127.0.0.1'
port = 8086

def create_database(client):
    
    dblist = client.get_list_database()
    
    #check if database_name exist
    if any(db['name'] == database_name for db in dblist):
        
         # Switch to the database
        client.switch_database(database_name)
        
        # Fetch all measurements
        measurements = client.query('SHOW MEASUREMENTS').get_points()
        measurement_names = [measurement['name'] for measurement in measurements]

        # Drop each measurement
        for name in measurement_names:
            client.query(f'DROP MEASUREMENT "{name}"')
        
        print(f"Database '{database_name}' already exists. Measurements dropped.")
    else:
        client.create_database(database_name)
        client.switch_database(database_name)
        print(f"Database '{database_name}' created.")
        
    print(f"Database '{database_name}' is ready.")



if __name__ == "__main__":
    client = InfluxDBClient(server_address, port)
    create_database(client)
    client.close()
