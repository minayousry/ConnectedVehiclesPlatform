import os
import pandas as pd

# Define the directory name to check
reports_dir_name = "reports"
reports_path = './' + reports_dir_name

import numpy as np
import matplotlib.pyplot as plt

excel_info = {}

com_tech_title_name = 'Communication Technology'
storage_tech_title_name = 'Storage Technology' 

colors_names = ['green','blue','red', 'yellow', 'purple']

def PlotRelationShip(x, y,tech_list,operation,is_no_of_cars,storage_mode,tech_type):

    print(tech_list)
    print(x)
    print(y)
    
    # Plotting the relationship between the arrays

    for i in range(len(x)):
        plt.plot(x[i], y[i], marker='s', linestyle='--', color=colors_names[i], label=str(tech_list[i]))
    
    

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
    
    title = y_label + " per "+ x_label + " - "+ storage_mode +" for "+ tech_type +" tech" 
    
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
        comm_tech_list.append(extracted_tech[0])
        storage_tech_list.append(extracted_tech[1])
    

    return comm_tech_list,storage_tech_list


def PlotOverallFigures():
    
    tech_list = []
    
    cars_no_batch_mode = []
    cars_no_single_mode = []
    
    comm_avg_time_batch_mode = []
    comm_avg_time_single_mode = []
    
    comm_std_batch_mode = []
    comm_std_single_mode = []
    
    
    storage_avg_time_batch_mode = []
    storage_avg_time_single_mode = []
    
    storage_std_batch_mode = []
    storage_std_single_mode = []
    
    
    for tech in excel_info.keys():
        tech_list.append(tech)
        list_of_cars = list(excel_info[tech].keys())
        
        cars_no_int_list = [int(x) for x in list_of_cars]
        cars_no_int_list.sort()
        list_of_cars = [str(x) for x in cars_no_int_list]
        
        batch_no_of_cars = []
        single_no_of_cars = []
        
        comm_avg_time_per_batch = []
        comm_avg_time_per_single = []
        
        comm_std_per_batch = []
        comm_std_per_single = []
        
        storage_avg_time_per_batch = []
        storage_avg_time_per_single = []
        
        storage_std_per_batch = []
        storage_std_per_single = []

        for cars_no in list_of_cars:
            
            for storage_mode in excel_info[tech][cars_no].keys():
                if storage_mode == "batch":
                    batch_no_of_cars.append(int(cars_no))
                    
                    comm_avg_time_per_batch.append(excel_info[tech][cars_no][storage_mode]["communication info"].mean())
                    comm_std_per_batch.append(excel_info[tech][cars_no][storage_mode]["communication info"].std())
                    
                    storage_avg_time_per_batch.append(excel_info[tech][cars_no][storage_mode]["storage info"].mean())
                    storage_std_per_batch.append(excel_info[tech][cars_no][storage_mode]["storage info"].std())
                    
                else:
                    single_no_of_cars.append(int(cars_no))
                    
                    comm_avg_time_per_single.append(excel_info[tech][cars_no][storage_mode]["communication info"].mean())
                    comm_std_per_single.append(excel_info[tech][cars_no][storage_mode]["communication info"].std())
                    
                    storage_avg_time_per_single.append(excel_info[tech][cars_no][storage_mode]["storage info"].mean())
                    storage_std_per_single.append(excel_info[tech][cars_no][storage_mode]["storage info"].std())
        
        cars_no_batch_mode.append(batch_no_of_cars)
        cars_no_single_mode.append(single_no_of_cars)
        
        comm_avg_time_batch_mode.append(comm_avg_time_per_batch)
        comm_avg_time_single_mode.append(comm_avg_time_per_single)
        
        comm_std_batch_mode.append(comm_std_per_batch)
        comm_std_single_mode.append(comm_std_per_single)
    
        storage_avg_time_batch_mode.append(storage_avg_time_per_batch)
        storage_avg_time_single_mode.append(storage_avg_time_per_single)
    
        storage_std_batch_mode.append(storage_std_per_batch)
        storage_std_single_mode.append(storage_std_per_single)
    
        
    comm_tech_list,storage_tech_list = extractTechAccordingToUsage(tech_list)
        
 
    PlotRelationShip(cars_no_batch_mode, comm_avg_time_batch_mode,comm_tech_list,"avg",True,"batch","communication")   
    PlotRelationShip(cars_no_batch_mode, comm_std_batch_mode,comm_tech_list,"std",True,"batch","communication") 
    
    PlotRelationShip(cars_no_single_mode, comm_avg_time_single_mode,comm_tech_list,"avg",True,"single","communication")   
    PlotRelationShip(cars_no_single_mode, comm_std_single_mode,comm_tech_list,"std",True,"single","communication")   
    
    PlotRelationShip(cars_no_batch_mode, storage_avg_time_batch_mode,storage_tech_list,"avg",True,"batch","storage")   
    PlotRelationShip(cars_no_batch_mode, storage_std_batch_mode,storage_tech_list,"std",True,"batch","storage")   
    
    PlotRelationShip(cars_no_single_mode, storage_avg_time_single_mode,storage_tech_list,"avg",True,"single","storage")   
    PlotRelationShip(cars_no_single_mode, storage_std_single_mode,storage_tech_list,"std",True,"single","storage") 
             
 
                 
                

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

def analyzeReports(parent_path,cars_no,storage_mode,reports_paths):

    data_to_plot = {}
    chart_name = 'col'

    cars_no_str = str(cars_no)
    
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
        no_of_records = df.shape[0]
        
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
            
            if cars_no_str not in excel_info[tech_name].keys():
                excel_info[tech_name][cars_no_str] = {}
            
            if storage_mode not in excel_info[tech_name][cars_no_str].keys():
                excel_info[tech_name][cars_no_str][storage_mode] = {}
            
            excel_info[tech_name][cars_no_str][storage_mode]["communication info"] = df['comm_latency_sec']
            

            excel_info[tech_name][cars_no_str][storage_mode]["storage info"] = df['write_latency_sec']
            excel_info[tech_name][cars_no_str][storage_mode]["no of msgs"] = no_of_records
            
                
    for operation in operations:
        data_to_plot['tech'] = []
        data_to_plot[com_tech_title_name] = []
        data_to_plot[storage_tech_title_name] = []
        
        for tech in excel_info.keys():        
            if tech in excel_info.keys():
                if cars_no_str in excel_info[tech].keys():
                    if storage_mode in excel_info[tech][cars_no_str].keys():
                        data_to_plot['tech'].append(tech)
                        comm_data_frame = excel_info[tech][cars_no_str][storage_mode]["communication info"]
                        storage_data_frame = excel_info[tech][cars_no_str][storage_mode]["storage info"]

                
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
        cars_no_list = [d for d in os.listdir(reports_path) if os.path.isdir(os.path.join(reports_path, d))]
        
        # Convert to list of integers using a list comprehension
        cars_no_int_list = [int(x) for x in cars_no_list]
        cars_no_int_list.sort()
        cars_no_list = [str(x) for x in cars_no_int_list]
    
        batched_reports_paths = []
        record_reports_paths = []
        for cars_no in cars_no_list:
            storage_mode_path = reports_path+'/'+cars_no
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
                analyzeReports(reports_names_path,cars_no,storage_mode,reports_paths)

def main():
    plotSeparateFigures()
    PlotOverallFigures() 


main()