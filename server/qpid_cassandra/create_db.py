from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from uuid import uuid4

keyspace_name = "obd2_database"
table_name = "obd2_data"
server_address = '127.0.0.1'

def create_keyspace_and_table(cluster):
    session = cluster.connect()
    
    # Create keyspace if it does not exist
    keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """
    session.execute(keyspace_query)
    
    # Set keyspace
    session.set_keyspace(keyspace_name)
    
    # Create table if it does not exist
    table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id uuid PRIMARY KEY,
        vehicle_id text,
        tx_time timestamp,
        x_pos double,
        y_pos double,
        gps_lon double,
        gps_lat double,
        speed double,
        road_id text,
        lane_id text,
        displacement double,
        turn_angle double,
        acceleration double,
        fuel_consumption double,
        co2_consumption double,
        deceleration double,
        storage_time TIMESTAMP
    );
    """
    
    
    session.execute(table_query)
    print(f"Keyspace '{keyspace_name}' and table '{table_name}' are ready.")
    
    session.shutdown()


def drop_keyspace(cluster):
    session = cluster.connect()
    session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name}")
    print(f"Keyspace '{keyspace_name}' has been dropped.")
    

if __name__ == "__main__":
    cluster = Cluster([server_address])
    drop_keyspace(cluster)
    create_keyspace_and_table(cluster)
