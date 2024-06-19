from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from uuid import uuid4
import logging
from cassandra.policies import DCAwareRoundRobinPolicy

keyspace_name = "obd2_database"
table_name = "obd2_data"
server_address = '127.0.0.1'


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def createKeyspaceAndTable(session,use_database_timestamp):
    
    # Initialize logging
    

    try:
    
        # Create keyspace if it does not exist
        keyspace_query = f"""
                            CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
        """
        session.execute(keyspace_query)
    
        
        logging.info(f"Keyspace '{keyspace_name}' created or already exists.")
        
        # Set keyspace
        session.set_keyspace(keyspace_name)

        if use_database_timestamp:
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
                rx_time timestamp,
                storage_time timestamp
            );
            """
        else:
            table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                id uuid PRIMARY KEY,
                vehicle_id text,
                tx_time text,
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
                rx_time text,
                storage_time text
            );
            """
    
        session.execute(table_query)
        logging.info(f"Table '{table_name}' created or already exists.")
    
    except Exception as e:
        logging.error(f"Error creating keyspace or table: {e}")



def dropKeyspace(session):
    try:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name}")
        logging.info(f"Keyspace '{keyspace_name}' has been dropped.")
    except Exception as e:
        logging.error(f"Error dropping keyspace: {e}")


def createDatabase(use_database_timestamp):
    status = False
    try:
        cluster = Cluster(contact_points=[server_address],
                            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
                            protocol_version=5)
        session = cluster.connect()
        
        
        dropKeyspace(session)
        createKeyspaceAndTable(session,use_database_timestamp)
        status =  True
    except Exception as e:
        print("error while creating database.")
    finally:
        if 'session' in locals():
            print("closing session")
            session.shutdown()
        if 'cluster' in locals():
            print("closing cluster")
            cluster.shutdown()
    return status


if __name__ == "__main__":
    use_database_timestamp = False
    
    createDatabase(use_database_timestamp)
