import os
import pandas as pd

# Define the directory name to check
reports_dir_name = "reports/phase2"
reports_path = './' + reports_dir_name

import numpy as np
import matplotlib.pyplot as plt

excel_info = {}

com_tech_title_name = 'Communication Technology'
storage_tech_title_name = 'Storage Technology' 

colors_names = ['green','blue','red', 'yellow', 'purple']


sending_periodicity = 10


def filterOutNegativeValues(df, cols_to_filter):
    mask = (df[cols_to_filter] >= 0).all(axis=1)
    return df[mask]
    

def PlotRelationShip(x, y,tech_list,operation,is_no_of_cars,storage_mode,last_file_part_name,is_list_of_list = False):


    
    # Plotting the relationship between the arrays

    if is_list_of_list:
        for i in range(len(x)):
            plt.plot(x[i], y[i], marker='s', linestyle='--', color=colors_names[i], label=str(tech_list[i]))
    else:
        plt.plot(x, y, marker='s', linestyle='--', color=colors_names[0], label=tech_list)
            
             
       # plt.annotate(f'{y[i]}', (x[i], y[i]), textcoords="offset points", xytext=(0,10), ha='center')
    
    

    x_label = ""
    y_label = ""
    title = ""
    
    if is_no_of_cars:
        x_label = 'No of Vehicles '
    else:
        x_label = 'No of Msgs '
    
    if operation == 'avg':
        y_label = 'Avg time in seconds' 
    else:
        y_label = 'Standard deviation'
    
    title = y_label + " per "+ x_label + " - "+ storage_mode +" for "+ last_file_part_name 
    
    plt.xlabel(x_label)         
    plt.ylabel(y_label)
    plt.title(title)
    plt.legend()
    plt.grid(True)

    file_name = "./reports/"+title+".png" 
    
    plt.savefig(file_name)
    
    plt.close()




def extractTechAccordingToUsage(tech_list):
    
    comm_tech_list = []
    storage_tech_list = []
    
    for tech in tech_list:
        extracted_tech = tech.split("_")
        if tech == "websocket_postgresql":
            comm_tech_list.append(extracted_tech[0]+"_P")
        elif tech == "websocket_redis": 
            comm_tech_list.append(extracted_tech[0]+"_R")
        else:
            comm_tech_list.append(extracted_tech[0])
            
        storage_tech_list.append(extracted_tech[1])
    

    return comm_tech_list,storage_tech_list


def PlotOverallFigures():
    
    tech_list = []
    
    cars_msgs_batch_mode = []
    cars_msgs_single_mode = []
    
    comm_avg_time_batch_mode = []
    comm_avg_time_single_mode = []
    
    comm_std_batch_mode = []
    comm_std_single_mode = []
    
    
    storage_avg_time_batch_mode = []
    storage_avg_time_single_mode = []
    
    storage_std_batch_mode = []
    storage_std_single_mode = []
    
    total_avg_time_batch_mode = []
    total_avg_time_single_mode = []
    
    total_std_time_batch_mode = []
    total_std_time_single_mode = []
    
    
    for tech in excel_info.keys():
        tech_list.append(tech)
        list_of_msgs = list(excel_info[tech].keys())
        
        msgs_no_int_list = [int(x) for x in list_of_msgs]
        msgs_no_int_list.sort()
        list_of_msgs = [str(x) for x in msgs_no_int_list]
        
        batch_no_of_msgs = []
        single_no_of_msgs = []
        
        comm_avg_time_per_batch = []
        comm_avg_time_per_single = []
        
        comm_std_per_batch = []
        comm_std_per_single = []
        
        storage_avg_time_per_batch = []
        storage_avg_time_per_single = []
        
        storage_std_per_batch = []
        storage_std_per_single = []
        
        total_avg_time_per_batch = []
        total_avg_time_per_single = []
        
        total_std_time_per_batch = []
        total_std_time_per_single = []
        

        for msgs_no in list_of_msgs:
            
            for storage_mode in excel_info[tech][msgs_no].keys():
                if storage_mode == "batch":
                    batch_no_of_msgs.append(int(msgs_no))
                    
                    comm_avg_time_per_batch.append(excel_info[tech][msgs_no][storage_mode]["communication info"].mean())
                    comm_std_per_batch.append(excel_info[tech][msgs_no][storage_mode]["communication info"].std())
                    
                    storage_avg_time_per_batch.append(excel_info[tech][msgs_no][storage_mode]["storage info"].mean())
                    storage_std_per_batch.append(excel_info[tech][msgs_no][storage_mode]["storage info"].std())
                    
                    total_avg_time_per_batch.append(excel_info[tech][msgs_no][storage_mode]["total info"].mean())
                    total_std_time_per_batch.append(excel_info[tech][msgs_no][storage_mode]["total info"].std())
                    
                    
                else:
                    single_no_of_msgs.append(int(msgs_no))
                    
                    comm_avg_time_per_single.append(excel_info[tech][msgs_no][storage_mode]["communication info"].mean())
                    comm_std_per_single.append(excel_info[tech][msgs_no][storage_mode]["communication info"].std())
                    
                    storage_avg_time_per_single.append(excel_info[tech][msgs_no][storage_mode]["storage info"].mean())
                    storage_std_per_single.append(excel_info[tech][msgs_no][storage_mode]["storage info"].std())
                    
                    total_avg_time_per_single.append(excel_info[tech][msgs_no][storage_mode]["total info"].mean())
                    total_std_time_per_single.append(excel_info[tech][msgs_no][storage_mode]["total info"].std())
        
        cars_msgs_batch_mode.append(batch_no_of_msgs)
        cars_msgs_single_mode.append(single_no_of_msgs)
        
        comm_avg_time_batch_mode.append(comm_avg_time_per_batch)
        comm_avg_time_single_mode.append(comm_avg_time_per_single)
        
        comm_std_batch_mode.append(comm_std_per_batch)
        comm_std_single_mode.append(comm_std_per_single)
    
        storage_avg_time_batch_mode.append(storage_avg_time_per_batch)
        storage_avg_time_single_mode.append(storage_avg_time_per_single)
    
        storage_std_batch_mode.append(storage_std_per_batch)
        storage_std_single_mode.append(storage_std_per_single)
        
        total_avg_time_batch_mode.append(total_avg_time_per_batch)
        total_avg_time_single_mode.append(total_avg_time_per_single)
        
        total_std_time_batch_mode.append(total_std_time_per_batch)
        total_std_time_single_mode.append(total_std_time_per_single)
    
        
    comm_tech_list,storage_tech_list = extractTechAccordingToUsage(tech_list)
        
 
    PlotRelationShip(cars_msgs_batch_mode, comm_avg_time_batch_mode,comm_tech_list,"avg",False,"batch","different comm technologies",True)   
    PlotRelationShip(cars_msgs_batch_mode, comm_std_batch_mode,comm_tech_list,"std",False,"batch","different comm technologies",True) 
    
    PlotRelationShip(cars_msgs_single_mode, comm_avg_time_single_mode,comm_tech_list,"avg",False,"single","different comm technologies",True)   
    PlotRelationShip(cars_msgs_single_mode, comm_std_single_mode,comm_tech_list,"std",False,"single","different comm technologies",True)   
    
    PlotRelationShip(cars_msgs_batch_mode, storage_avg_time_batch_mode,storage_tech_list,"avg",False,"batch","different storage technologies",True)   
    PlotRelationShip(cars_msgs_batch_mode, storage_std_batch_mode,storage_tech_list,"std",False,"batch","different storage technologies",True)   
    
    PlotRelationShip(cars_msgs_single_mode, storage_avg_time_single_mode,storage_tech_list,"avg",False,"single","different storage technologies",True)   
    PlotRelationShip(cars_msgs_single_mode, storage_std_single_mode,storage_tech_list,"std",False,"single","different storage technologies",True)
    
    
    PlotRelationShip(cars_msgs_batch_mode, total_avg_time_batch_mode,tech_list,"avg",False,"batch","different technologies",True)
    PlotRelationShip(cars_msgs_batch_mode, total_std_time_batch_mode,tech_list,"std",False,"batch","different technologies",True)
         
    PlotRelationShip(cars_msgs_single_mode, total_avg_time_single_mode,tech_list,"avg",False,"single","different technologies",True)   
    PlotRelationShip(cars_msgs_single_mode, total_std_time_single_mode,tech_list,"std",False,"single","different technologies",True)   
    
    
    for i in range(len(comm_tech_list)):

        
        PlotRelationShip(cars_msgs_batch_mode[i], comm_avg_time_batch_mode[i],comm_tech_list[i],"avg",False,"batch",comm_tech_list[i])
        PlotRelationShip(cars_msgs_batch_mode[i], comm_std_batch_mode[i],comm_tech_list[i],"std",False,"batch",comm_tech_list[i])
        PlotRelationShip(cars_msgs_single_mode[i], comm_avg_time_single_mode[i],comm_tech_list[i],"avg",False,"single",comm_tech_list[i])   
        PlotRelationShip(cars_msgs_single_mode[i], comm_std_single_mode[i],comm_tech_list[i],"std",False,"single",comm_tech_list[i]) 
     
    for i in range(len(storage_tech_list)):
        PlotRelationShip(cars_msgs_batch_mode[i], storage_avg_time_batch_mode[i],storage_tech_list[i],"avg",False,"batch",storage_tech_list[i])   
        PlotRelationShip(cars_msgs_batch_mode[i], storage_std_batch_mode[i],storage_tech_list[i],"std",False,"batch",storage_tech_list[i])   
        PlotRelationShip(cars_msgs_single_mode[i], storage_avg_time_single_mode[i],storage_tech_list[i],"avg",False,"single",storage_tech_list[i])   
        PlotRelationShip(cars_msgs_single_mode[i], storage_std_single_mode[i],storage_tech_list[i],"std",False,"single",storage_tech_list[i])
    


                 
                

def plotColChart(df_to_plot,operation,parent_path,no_of_cars,no_of_records,storage_mode):


    print("Creating reports in "+ parent_path)
    # Number of categories
    tech_nums = len(df_to_plot['tech'])
    
    file_path = parent_path + "/"


    # Width of each bar
    bar_width = 0.25

    #print(df_to_plot)

    # Set the positions of the bars on the x-axis
    x = np.arange(tech_nums)

    # Creating the column chart
    plt.figure(figsize=(10, 6))

    column_names_list = list(df_to_plot.keys())


    column_names_list.remove('tech')

    
    
    for i in range(len(list(df_to_plot.keys())) - 1):
        plt.bar(x + (i * bar_width), df_to_plot[column_names_list[i]], width=bar_width, color=colors_names[i], label=column_names_list[i])

    plt.xlabel('Tech')

    ylabel = ''
    if operation == 'avg':
        ylabel = 'Avg time in seconds' 
    else:
        ylabel = 'Standard deviation'
    
    title = ylabel
    title+= " for "+str(no_of_cars)+" cars sending "+str(no_of_records)+" msgs - " + storage_mode
    title+= " for different technologies" 
    

    file_path += title + '.png'
    
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(x + bar_width,df_to_plot['tech'])
    plt.legend()
    plt.grid(True)
    
    #print(f"file path:{file_path}")
    plt.savefig(file_path)
    
    plt.close()

def analyzeReports(parent_path,cars_msgs,storage_mode,reports_paths):

    data_to_plot = {}
    chart_name = 'col'


    cars_no = (int(cars_msgs))/sending_periodicity
    cars_msgs_str = cars_msgs
    
    avg_values = {}
    std_values = {}

    operations = ['avg','std']

    tech_list = []
    
    no_of_records = 0
    
    global excel_info

    
    data_to_plot['tech'] = [] 
    

    for report_path in reports_paths:
        print(f"reading file:{report_path}")
        df = pd.read_excel(report_path,engine='openpyxl') 
        no_of_records = df.shape[0] + 1 # the dropped one
        
        tech_name = None
        tech_name_found = False
        start_pos = report_path.rfind('single_')
        if start_pos != -1:
            tech_name_start_pos = start_pos + len('single_')
            tech_name_end_pos = report_path.find('obd2_data')
            tech_name = report_path[tech_name_start_pos:tech_name_end_pos - 1]
            tech_name_found = True
            
        else:
            start_pos = report_path.rfind('batch_')
            if start_pos != -1:
                tech_name_start_pos = start_pos + len('batch_')
                tech_name_end_pos = report_path.find('obd2_data')
                tech_name_found = True
        
        if tech_name_found:
            tech_name = report_path[tech_name_start_pos:tech_name_end_pos - 1]
            
            if tech_name not in excel_info.keys():
                excel_info[tech_name] = {}
            
            if cars_msgs_str not in excel_info[tech_name].keys():
                excel_info[tech_name][cars_msgs_str] = {}
            
            if storage_mode not in excel_info[tech_name][cars_msgs_str].keys():
                excel_info[tech_name][cars_msgs_str][storage_mode] = {}
            
            cols_to_filter = ['comm_latency_sec','write_latency_sec']
            filtered_df = filterOutNegativeValues(df,cols_to_filter)
            
            excel_info[tech_name][cars_msgs_str][storage_mode]["communication info"] = filtered_df['comm_latency_sec']
            excel_info[tech_name][cars_msgs_str][storage_mode]["storage info"] = filtered_df['write_latency_sec']
            excel_info[tech_name][cars_msgs_str][storage_mode]["total info"] = filtered_df['comm_latency_sec'] + filtered_df['write_latency_sec']
            
            excel_info[tech_name][cars_msgs_str][storage_mode]["no of vehicles"] = no_of_records/sending_periodicity
            
                
    for operation in operations:
        data_to_plot['tech'] = []
        data_to_plot[com_tech_title_name] = []
        data_to_plot[storage_tech_title_name] = []
        
        for tech in excel_info.keys():        
            if tech in excel_info.keys():
                if cars_msgs_str in excel_info[tech].keys():
                    if storage_mode in excel_info[tech][cars_msgs_str].keys():
                        data_to_plot['tech'].append(tech)
                        comm_data_frame = excel_info[tech][cars_msgs_str][storage_mode]["communication info"]
                        storage_data_frame = excel_info[tech][cars_msgs_str][storage_mode]["storage info"]

                
                        if operation == 'avg':
                            data_to_plot[com_tech_title_name].append(comm_data_frame.mean())
                            data_to_plot[storage_tech_title_name].append(storage_data_frame.mean())
                        else:
                            data_to_plot[com_tech_title_name].append(comm_data_frame.std())
                            data_to_plot[storage_tech_title_name].append(storage_data_frame.std())
        
        
        plotColChart(data_to_plot,operation,parent_path,cars_no,no_of_records,storage_mode)
        



def listFilesWithExtension(directory, extension):
    # Get the list of files in the directory
    files = os.listdir(directory)
    # Filter files by the specified extension
    filtered_files = [file for file in files if file.endswith(extension)]
    return filtered_files

def removeOldFigures(directory):

    old_figures = listFilesWithExtension(directory,".png")
    for old_fig in old_figures:
        os.remove(directory+"/" + old_fig)


def plotSeparateFigures():
    if os.path.isdir(reports_dir_name):
        cars_msgs_list = [d for d in os.listdir(reports_path) if os.path.isdir(os.path.join(reports_path, d))]
        
        # Convert to list of integers using a list comprehension
        cars_msgs_int_list = [int(x) for x in cars_msgs_list]
        cars_msgs_int_list.sort()
        cars_msgs_list = [str(x) for x in cars_msgs_int_list]
    
        batched_reports_paths = []
        record_reports_paths = []
        for cars_msgs in cars_msgs_list:
            storage_mode_path = reports_path+'/'+cars_msgs
            storage_modes = os.listdir(storage_mode_path)
            for storage_mode in storage_modes:

                reports_names_path = storage_mode_path+'/'+storage_mode
                reports_names = listFilesWithExtension(reports_names_path,".xlsx")
                reports_paths = []

                
                reports_paths = []
                for report_name in reports_names:
                    report_path = reports_names_path + '/' + report_name
                    reports_paths.append(report_path)

                    if storage_mode == 'batch':
                        batched_reports_paths.append(report_path)
                    else:
                        record_reports_paths.append(report_path)

                removeOldFigures(reports_names_path)        
                analyzeReports(reports_names_path,cars_msgs,storage_mode,reports_paths)

def main():
    plotSeparateFigures()
    PlotOverallFigures() 


main()