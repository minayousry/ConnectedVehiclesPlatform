
import sys
import os
import pandas as pd
import requests


def createExcelFile(obd2_data_frame,generation_path):

    try:
        if type(obd2_data_frame['tx_time'].iloc[0]) == str:
            print("Converting tx time to datetime")
            obd2_data_frame['tx_time'] = pd.to_datetime(obd2_data_frame['tx_time'], format='%Y-%m-%d %H:%M:%S.%f')
        
        if type(obd2_data_frame['storage_time'].iloc[0]) == str:
            print("Converting storage time to datetime")
            obd2_data_frame['storage_time'] = pd.to_datetime(obd2_data_frame['storage_time'], format='%Y-%m-%d %H:%M:%S.%f')
    
        print("Calculating time difference")
        time_diff = obd2_data_frame['storage_time'] - obd2_data_frame['tx_time']
        
        # Convert time difference to seconds (assuming all values are valid)
        obd2_data_frame['time_diff_seconds'] = time_diff.dt.total_seconds().abs()
        
        # Generate Excel report
        obd2_data_frame.to_excel(generation_path+"obd2_data_report.xlsx", index=False)
        print("Excel file has been created.")
    
    except Exception as e:
        print(f"Failed to create excel file: {e}")
        
def set_file_mode(file_path, new_mode):
    # Check if the file exists
    
    if not os.path.exists(file_path):
        print(f"File '{file_path}' does not exist.")
        return
    
    # Close the file if it's already open
    with open(file_path, 'r') as file:
        pass  # Do nothing, just close the file

    # Reopen the file with the new mode
    with open(file_path, new_mode) as file:
        return file.read()
    
def getExternalIp():
    ip_address = None
    try:
        response = requests.get('https://httpbin.org/ip')
        if response.status_code == 200:
            data = response.json()
            return data['origin']
        else:
            print("Failed to retrieve IP:", response.status_code)
            return None
    except Exception as e:
        print("An error occurred:", e)
        return None
