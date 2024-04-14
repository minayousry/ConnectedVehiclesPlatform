
import sys
import os
import pandas as pd


def createExcelFile(obd2_data_frame):

    try:
        if(type(obd2_data_frame['tx_time'].iloc[0]) == str):
            obd2_data_frame['tx_time'] = pd.to_datetime(obd2_data_frame['tx_time'], format='%Y-%m-%d %H:%M:%S')
        
        if(type(obd2_data_frame['storage_time'].iloc[0]) == str):
            obd2_data_frame['storage_time'] = pd.to_datetime(obd2_data_frame['storage_time'], format='%Y-%m-%d %H:%M:%S')
    
        time_diff = obd2_data_frame['storage_time'] - obd2_data_frame['tx_time']
        
        # Convert time difference to seconds (assuming all values are valid)
        obd2_data_frame['time_diff_seconds'] = time_diff.dt.total_seconds()
            
        # Generate Excel report
        obd2_data_frame.to_excel("obd2_data_report.xlsx", index=False)
        print("Excel file has been created.")
    
    except Exception as e:
        print(f"Failed to create excel file: {e}")