import os
import pandas as pd

# Define the directory name to check
reports_dir_name = "reports"
reports_path = './' + reports_dir_name

import numpy as np
import matplotlib.pyplot as plt


com_tech_title_name = 'Communication Technology'
storage_tech_title_name = 'Storage Technology' 

# Sample multidimensional arrays
x = np.array([[1, 2, 3, 4, 5],
              [1, 2, 3, 4, 5]])
y = np.array([[2, 4, 6, 8, 10],
              [3, 6, 9, 12, 15]])

# Plotting the relationship between the arrays
plt.plot(x[0], y[0], marker='o', linestyle='-', color='b', label='Array 1')
plt.plot(x[1], y[1], marker='s', linestyle='--', color='r', label='Array 2')

# Adding labels and title
plt.xlabel('X Axis')
plt.ylabel('Y Axis')
plt.title('Relationship between two multidimensional arrays')

# Example list of inputs for x
x_values = [10, 12, 15, 20, 25]

# Calculate the standard deviation
std_dev = np.std(x_values)

print("Standard Deviation:", std_dev)
# Display the legend
plt.legend()

# Display the grid
plt.grid(True)

# Save the plot as an image file
plt.savefig('plot.png')

# Show the plot
plt.show()



def plotColChart(df_to_plot,operation,parent_path,no_of_cars,no_of_records,storage_mode):


    print("Creating reports in "+ parent_path)
    # Number of categories
    tech_nums = len(df_to_plot['tech'])
    
    file_path = parent_path + "/"

    colors_names = ['green','blue','red', 'yellow', 'purple']


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
    
    #print(f"file path:{file_path}")
    plt.savefig(file_path)

def analyzeReports(parent_path,cars_no,storage_mode,reports_paths):

    data_to_plot = {}
    chart_name = 'col'

    
    avg_values = {}
    std_values = {}

    operations = ['avg','std']

    tech_list = []
    
    no_of_records = 0
    
    excel_info = {}

    
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
            excel_info[tech_name] = {}
            excel_info[tech_name]["communication info"] = df['comm_latency_sec']
            excel_info[tech_name]["storage info"] = df['write_latency_sec']
                
    for operation in operations:
        data_to_plot['tech'] = []
        data_to_plot[com_tech_title_name] = []
        data_to_plot[storage_tech_title_name] = []
        
        for tech in excel_info.keys():
            data_to_plot['tech'].append(tech)
            
            
            comm_data_frame = excel_info[tech]["communication info"]
            storage_data_frame = excel_info[tech]["storage info"]
            
                
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


def main():
    if os.path.isdir(reports_dir_name):
        cars_no_list = os.listdir(reports_path)
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
                        
                analyzeReports(reports_names_path,cars_no,storage_mode,reports_paths) 
                #print(reports_paths)

main()