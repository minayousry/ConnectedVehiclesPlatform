import argparse

import client_utilities as cl_utl

import kafka_client_run as kafka_cl
import mqtt_client_run as mqtt_cl
import qpid_client_run as qpid_cl
import ws_client_run as ws_cl

import client_utilities as cl_utl

remote_machine_ip_addr = "34.90.169.134"
simulation_path = "./simulation1/"
main_simulation_file = simulation_path+"osm.sumocfg"
cars_simulation_file = simulation_path+"osm.passenger.trips.xml"

# Confiurations for SUMO
sumo_cmd = ["sumo", "-c", main_simulation_file]

no_of_cars = 100



def configureNoOfCars():
    
    result = False
    
    # Set the number of cars to be generated in the simulation

    comment_text = "<!--"
    
    trip_id_veh_text = "<trip id=\"veh"    
    commented_trip_id_text = comment_text + trip_id_veh_text
    
 
    # Read the contents of the file
    with open(cars_simulation_file, 'r+') as file:
        lines = file.readlines()
        
        for i in range(len(lines)):
            if commented_trip_id_text in lines[i]:
                lines[i] = lines[i].replace(comment_text,"")
                break
        
        search_text = trip_id_veh_text+str(no_of_cars)
         
        for i in range(len(lines)):
            if search_text in lines[i]:
                result = True
                lines[i] = lines[i].replace(trip_id_veh_text,commented_trip_id_text)
                break    
        
        if result:
            # Move the file pointer to the beginning of the file
            file.seek(0)
            file.writelines(lines)
            file.truncate()
        else:
            print("No such veh id found in the simulation file.")

    return result

if __name__ == '__main__':
    
    tech_list = ["kafka", "mqtt", "qpid", "ws"]
    
    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add a string argument
    parser.add_argument('client_technology', type=str, help='select which technology to use for the client.')

    # Parse the arguments
    args = parser.parse_args()

    # Access the parsed argument
    client_tech = args.client_technology
    
    if client_tech in tech_list:
        result = configureNoOfCars()
    else:
        print("Invalid client technology. Please select one of the following: kafka, mqtt, qpid,  or ws")
        exit(1)
    
    if result:
        cl_utl.resetMsgCount(client_tech) 
        if client_tech == "kafka":    
            kafka_cl.runKafkaClient(sumo_cmd,remote_machine_ip_addr)
        elif client_tech == "mqtt":
            mqtt_cl.runMqttClient(sumo_cmd,remote_machine_ip_addr)
         
        elif client_tech == "qpid":
            qpid_cl.runQpidClient(sumo_cmd,remote_machine_ip_addr)
        
        elif client_tech == "ws":
            ws_cl.runWsClient(sumo_cmd,remote_machine_ip_addr)
        
        sim_duration = cl_utl.calculateSimDuration(client_tech)
        
        if sim_duration is not None:
            print(f"Simulation duration: {sim_duration}")
        else:
            print("Error in calculating the simulation duration.")
        
        no_of_sent_msgs = cl_utl.getMsgCount(client_tech)
        print(f"Number of sent messages: {no_of_sent_msgs}")
         
    else:
        print("Error in configuring the number of cars in the simulation file.")
        exit(1)
    
